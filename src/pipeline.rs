use tokio::sync::mpsc::UnboundedSender;

use crate::feeds::Persona;
use crate::fetcher::fetch_all;
use crate::llm::{daily_brief, ollama_client, score_articles, summarize_article};
use crate::progress::{BriefStats, ProgressEvent};

pub struct PipelineConfig {
    pub model: String,
    pub hours: i64,
    pub top_n: usize,
    pub persona: Persona,
}

pub async fn run_pipeline(cfg: PipelineConfig, tx: UnboundedSender<ProgressEvent>) {
    let sources = &cfg.persona.feeds;
    let n_feeds = sources.len();

    // === FETCH ===
    let articles = fetch_all(sources, cfg.hours, &tx).await;
    let total = articles.len();
    let _ = tx.send(ProgressEvent::Stage {
        stage: "FETCH".into(),
        message: format!("Got {} articles after dedup. Preparing LLM…", total),
        percent: 26,
    });

    if articles.is_empty() {
        let _ = tx.send(ProgressEvent::Done {
            headline: "No Articles Found".to_string(),
            brief: "No articles found in the time window. Try expanding the hours filter."
                .to_string(),
            articles: vec![],
            stats: BriefStats {
                feeds_fetched: n_feeds,
                total_articles: 0,
                articles_kept: 0,
            },
        });
        return;
    }

    let client = ollama_client();
    let mut to_score: Vec<_> = articles.into_iter().take(80).collect();

    // === SCORE ===
    if let Err(e) = score_articles(
        &client,
        &cfg.model,
        &cfg.persona.name,
        &cfg.persona.description,
        &mut to_score,
        &tx,
    )
    .await
    {
        let _ = tx.send(ProgressEvent::Error(format!(
            "LLM scoring failed: {}. Is Ollama running with model '{}'?",
            e, cfg.model
        )));
        return;
    }

    to_score.sort_by(|a, b| {
        b.relevance
            .partial_cmp(&a.relevance)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let top: Vec<_> = to_score.into_iter().take(cfg.top_n).collect();
    let n_candidates = top.len();

    // === SUMMARIZE ===
    let mut summarized_top = Vec::new();
    for (i, mut article) in top.into_iter().enumerate() {
        let pct = 52 + ((i * 38) / n_candidates.max(1)) as u8;
        let title_short: String = article.title.chars().take(70).collect();
        let _ = tx.send(ProgressEvent::Stage {
            stage: "SUMMARIZE".into(),
            message: format!("[{}/{}] {}", i + 1, n_candidates, title_short),
            percent: pct,
        });
        match summarize_article(&client, &cfg.model, &cfg.persona.name, &article).await {
            Ok(s) => {
                article.ai_summary = Some(s);
                summarized_top.push(article);
            }
            Err(_) => {
                // LLM cannot produce a summary -> omit the article
            }
        }
    }

    let top = summarized_top;
    let n = top.len();

    if top.is_empty() {
        let _ = tx.send(ProgressEvent::Done {
            headline: "No Summaries Generated".to_string(),
            brief: "Could not generate valid summaries for any of the fetched articles.".to_string(),
            articles: vec![],
            stats: BriefStats {
                feeds_fetched: n_feeds,
                total_articles: total,
                articles_kept: 0,
            },
        });
        return;
    }

    // === BRIEF ===
    let _ = tx.send(ProgressEvent::Stage {
        stage: "BRIEF".into(),
        message: "Synthesizing headline and executive brief…".into(),
        percent: 94,
    });
    let brief_output = daily_brief(&client, &cfg.model, &cfg.persona.name, &top)
        .await
        .unwrap_or_else(|e| crate::llm::BriefOutput {
            headline: "Daily Intelligence Briefing".to_string(),
            executive_brief: format!("(Brief generation failed: {}.)", e),
        });

    let _ = tx.send(ProgressEvent::Stage {
        stage: "DONE".into(),
        message: "Complete.".into(),
        percent: 100,
    });

    let _ = tx.send(ProgressEvent::Done {
        headline: brief_output.headline,
        brief: brief_output.executive_brief,
        articles: top,
        stats: BriefStats {
            feeds_fetched: n_feeds,
            total_articles: total,
            articles_kept: n,
        },
    });
}
