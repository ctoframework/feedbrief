use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;

use crate::fetcher::Article;
use crate::progress::ProgressEvent;

const OLLAMA_URL: &str = "http://localhost:11434/api/generate";

#[derive(Serialize)]
struct OllamaRequest<'a> {
    model: &'a str,
    prompt: String,
    stream: bool,
    format: Option<&'a str>,
    options: OllamaOptions,
}

#[derive(Serialize)]
struct OllamaOptions {
    temperature: f32,
    num_predict: i32,
}

#[derive(Deserialize)]
struct OllamaResponse {
    response: String,
}

async fn ollama_call(
    client: &reqwest::Client,
    model: &str,
    prompt: String,
    json_mode: bool,
    max_tokens: i32,
) -> Result<String> {
    let req = OllamaRequest {
        model,
        prompt,
        stream: false,
        format: if json_mode { Some("json") } else { None },
        options: OllamaOptions {
            temperature: 0.2,
            num_predict: max_tokens,
        },
    };
    let resp = client
        .post(OLLAMA_URL)
        .timeout(Duration::from_secs(240))
        .json(&req)
        .send()
        .await
        .context("Ollama not reachable — is `ollama serve` running?")?;

    let status = resp.status();
    let text = resp
        .text()
        .await
        .context("Failed to read Ollama response body")?;

    if !status.is_success() {
        anyhow::bail!("Ollama returned error {}: {}", status, text);
    }

    let body: OllamaResponse = serde_json::from_str(&text)
        .with_context(|| format!("Failed to parse Ollama JSON response: {}", text))?;
    Ok(body.response)
}

#[derive(Deserialize)]
struct ScoringResult {
    relevance: f32,
    topic: String,
}

pub async fn score_articles(
    client: &reqwest::Client,
    model: &str,
    persona_name: &str,
    persona_description: &str,
    articles: &mut [Article],
    tx: &UnboundedSender<ProgressEvent>,
) -> Result<()> {
    let total = articles.len();
    let n_batches = (total + 4) / 5;
    let mut batch_idx = 0usize;

    for chunk in articles.chunks_mut(5) {
        batch_idx += 1;
        let percent = 28 + (batch_idx * 22 / n_batches.max(1)) as u8;
        let _ = tx.send(ProgressEvent::Stage {
            stage: "SCORE".into(),
            message: format!(
                "Scoring batch {}/{} ({} articles)…",
                batch_idx,
                n_batches,
                chunk.len()
            ),
            percent,
        });

        let items: Vec<String> = chunk
            .iter()
            .enumerate()
            .map(|(i, a)| {
                let snippet: String = a.summary.chars().take(200).collect();
                format!("{}. [{}] {}\n   {}", i + 1, a.source, a.title, snippet)
            })
            .collect();

        let prompt = format!(
            r#"You are an intelligence analyst for {}. Score each item below by how much it matters for someone tracking: {}

For each item, return a relevance score 0.0–10.0 (10 = must-read, 0 = noise) and a short topic tag (2–4 words, lowercase). Invent new tags if a story doesn't fit existing ones — emerging themes matter.

Items:
{}

Return ONLY a JSON object of this exact shape:
{{"scores": [{{"relevance": 7.5, "topic": "llm training"}}, ...]}}
The array must have exactly {} entries in the same order as the items above."#,
            persona_name,
            persona_description,
            items.join("\n\n"),
            chunk.len()
        );

        let response = ollama_call(client, model, prompt, true, 400).await?;

        #[derive(Deserialize)]
        struct Wrapper {
            scores: Vec<ScoringResult>,
        }

        match serde_json::from_str::<Wrapper>(&response) {
            Ok(w) if w.scores.len() == chunk.len() => {
                for (article, score) in chunk.iter_mut().zip(w.scores.iter()) {
                    article.relevance = Some(score.relevance);
                    article.topic_tag = Some(score.topic.clone());
                }
            }
            _ => {
                for a in chunk.iter_mut() {
                    a.relevance = Some(5.0);
                    a.topic_tag = Some("uncategorized".to_string());
                }
            }
        }
    }
    Ok(())
}

pub fn is_valid_summary(summary: &str) -> bool {
    let trimmed = summary.trim();
    if trimmed.is_empty() {
        return false;
    }
    let lower = trimmed.to_lowercase();

    let refusal_phrases = [
        "don't see any content",
        "dont see any content",
        "no content provided",
        "no text provided",
        "no content to summarize",
        "no text to summarize",
        "no content available",
        "no text available",
        "please provide the text",
        "please provide text",
        "please provide content",
        "please provide the article",
        "cannot summarize",
        "can't summarize",
        "unable to summarize",
        "cannot provide a summary",
        "can't provide a summary",
        "unable to provide a summary",
        "there is no content",
        "there is no text",
        "i don't see any text",
        "i dont see any text",
        "not enough content",
        "insufficient content",
        "no information provided",
        "no content was provided",
        "no text was provided",
    ];

    for phrase in refusal_phrases {
        if lower.contains(phrase) {
            return false;
        }
    }

    let is_refusal_prefix = lower.starts_with("unfortunately")
        || lower.starts_with("i'm sorry")
        || lower.starts_with("i am sorry")
        || lower.starts_with("i apologize")
        || lower.starts_with("as an ai");

    if is_refusal_prefix {
        let refusal_action = lower.contains("provide")
            || lower.contains("don't see")
            || lower.contains("dont see")
            || lower.contains("no content")
            || lower.contains("no text")
            || lower.contains("cannot")
            || lower.contains("can't")
            || lower.contains("unable");
        if refusal_action {
            return false;
        }
    }

    true
}

pub async fn summarize_article(
    client: &reqwest::Client,
    model: &str,
    persona_name: &str,
    article: &Article,
) -> Result<String> {
    let body: String = article.summary.chars().take(1500).collect();
    if body.trim().is_empty() {
        anyhow::bail!("Article content is empty; cannot summarize");
    }

    let prompt = format!(
        r#"Summarize the following news item in EXACTLY 2 sentences for {}. Be concrete: name the actors, the number, the technique, the impact. No fluff, no "in this article", no editorializing.

Title: {}
Source: {}
Content: {}

Two-sentence summary:"#,
        persona_name, article.title, article.source, body
    );
    let response = ollama_call(client, model, prompt, false, 200).await?;
    let summary = response.trim().to_string();

    if !is_valid_summary(&summary) {
        anyhow::bail!("LLM failed to produce a valid summary: {}", summary);
    }

    Ok(summary)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BriefOutput {
    pub headline: String,
    pub executive_brief: String,
}

fn parse_brief_output(text: &str) -> Result<BriefOutput> {
    let cleaned = text.trim();
    let json_str = if cleaned.starts_with("```") {
        cleaned
            .strip_prefix("```json")
            .or_else(|| cleaned.strip_prefix("```"))
            .and_then(|s| s.strip_suffix("```"))
            .unwrap_or(cleaned)
            .trim()
    } else {
        cleaned
    };

    let val: serde_json::Value = serde_json::from_str(json_str)?;
    let headline = val
        .get("headline")
        .and_then(|h| h.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let executive_brief = val
        .get("executive_brief")
        .or_else(|| val.get("executiveBrief"))
        .or_else(|| val.get("brief"))
        .and_then(|b| b.as_str())
        .unwrap_or("")
        .trim()
        .to_string();

    if headline.is_empty() || executive_brief.is_empty() {
        anyhow::bail!("Missing headline or executive_brief in JSON output");
    }

    Ok(BriefOutput {
        headline,
        executive_brief,
    })
}

fn fallback_brief_parse(text: &str) -> BriefOutput {
    let lines: Vec<&str> = text
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect();
    if lines.is_empty() {
        return BriefOutput {
            headline: "Daily Intelligence Briefing".to_string(),
            executive_brief: text.trim().to_string(),
        };
    }

    let mut headline = String::new();
    let mut brief_lines = Vec::new();

    for (i, line) in lines.iter().enumerate() {
        if i == 0 {
            headline = line
                .trim_start_matches('#')
                .trim_start_matches("Headline:")
                .trim_start_matches("HEADLINE:")
                .trim_matches('"')
                .trim()
                .to_string();
        } else {
            let cleaned = line
                .trim_start_matches("Executive Brief:")
                .trim_start_matches("EXECUTIVE BRIEF:")
                .trim_start_matches("Brief:")
                .trim();
            if !cleaned.is_empty() {
                brief_lines.push(cleaned);
            }
        }
    }

    if headline.is_empty() {
        headline = "Daily Intelligence Briefing".to_string();
    }
    let executive_brief = if brief_lines.is_empty() {
        text.trim().to_string()
    } else {
        brief_lines.join("\n\n")
    };

    BriefOutput {
        headline,
        executive_brief,
    }
}

pub async fn daily_brief(
    client: &reqwest::Client,
    model: &str,
    persona_name: &str,
    top_articles: &[Article],
) -> Result<BriefOutput> {
    let bullets: Vec<String> = top_articles
        .iter()
        .take(15)
        .map(|a| format!("- [{}] {}", a.topic_tag.as_deref().unwrap_or("?"), a.title))
        .collect();

    let prompt = format!(
        r#"You are briefing {}. Below are today's top headlines, already filtered for relevance.

Generate a daily briefing with two parts:
1. "headline": A punchy 1-line title (under 10 words, no quotes) capturing the main theme/story of the day.
2. "executive_brief": A single-paragraph (4–6 sentences) executive briefing that synthesizes the THEMES of the day — what's the through-line? what's accelerating? what should they pay attention to this week? Be specific, name companies and technologies. No greetings or sign-offs.

Today's top items:
{}

Return ONLY a JSON object of this exact format:
{{"headline": "Headline Here", "executive_brief": "Executive brief text here..."}}"#,
        persona_name,
        bullets.join("\n")
    );

    let response = ollama_call(client, model, prompt, true, 450).await?;

    if let Ok(output) = parse_brief_output(&response) {
        return Ok(output);
    }

    Ok(fallback_brief_parse(&response))
}

pub fn ollama_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(240))
        .build()
        .expect("ollama client")
}

pub async fn check_ollama(model: &str) -> bool {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let resp = match client.get("http://localhost:11434/api/tags").send().await {
        Ok(r) => r,
        Err(_) => return false,
    };
    let body: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(_) => return false,
    };
    body.get("models")
        .and_then(|m| m.as_array())
        .map(|arr| {
            arr.iter().any(|m| {
                m.get("name")
                    .and_then(|n| n.as_str())
                    .map(|s| s.starts_with(model))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_summary_valid() {
        let valid_summary = "Nvidia announced the H200 GPU with 141GB of HBM3e memory. The new chip provides 1.4x higher memory bandwidth for LLM training.";
        assert!(is_valid_summary(valid_summary));
    }

    #[test]
    fn test_is_valid_summary_refusal_patterns() {
        let refusals = [
            "Unfortunately, I don't see any content provided for the news item. Please provide the text, and I'll be happy to summarize it in exactly 2 sentences for a Chief Technology Officer.",
            "I don't see any content provided for the news item.",
            "No content provided for this item.",
            "Please provide the text of the article.",
            "I cannot summarize this article because no text was provided.",
            "Unfortunately, there is no text available.",
            "   ",
            "",
        ];

        for refusal in refusals {
            assert!(
                !is_valid_summary(refusal),
                "Expected refusal to be invalid: {}",
                refusal
            );
        }
    }
}
