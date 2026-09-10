use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use feed_rs::parser;
use futures::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::Duration as StdDuration;
use tokio::sync::mpsc::UnboundedSender;

use crate::feeds::FeedSource;
use crate::progress::ProgressEvent;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Article {
    pub id: String,
    pub title: String,
    pub url: String,
    pub source: String,
    pub category: String,
    pub published: DateTime<Utc>,
    pub summary: String,
    pub relevance: Option<f32>,
    pub topic_tag: Option<String>,
    pub ai_summary: Option<String>,
    #[serde(default)]
    pub is_favourite: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct FeedFetchStatus {
    pub feed_url: String,
    pub last_item_published: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

fn hash_id(url: &str) -> String {
    let mut h = Sha256::new();
    h.update(url.as_bytes());
    hex::encode(&h.finalize()[..8])
}

fn clean_html(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut in_tag = false;
    for c in input.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(c),
            _ => {}
        }
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// UTF-8-safe truncation by character count (never panics on multi-byte boundaries).
fn truncate_chars(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max_chars).collect();
    out.push('…');
    out
}

async fn fetch_one(
    client: &reqwest::Client,
    source: &FeedSource,
    max_age: Duration,
) -> Result<(Vec<Article>, Option<DateTime<Utc>>)> {
    let resp = client
        .get(&source.url)
        .timeout(StdDuration::from_secs(15))
        .header("User-Agent", "Feedbrief/0.2 (personal news aggregator)")
        .send()
        .await?;

    let bytes = resp.bytes().await?;
    let feed = parser::parse(&bytes[..])?;
    let cutoff = Utc::now() - max_age;
    let mut articles = Vec::new();
    let mut last_item_published = None;

    for entry in feed.entries {
        let url = entry
            .links
            .first()
            .map(|l| l.href.clone())
            .unwrap_or_default();
        if url.is_empty() {
            continue;
        }
        let title = entry
            .title
            .map(|t| t.content)
            .unwrap_or_else(|| "Untitled".to_string());
        let published = entry.published.or(entry.updated).unwrap_or_else(Utc::now);
        last_item_published = Some(
            last_item_published.map_or(published, |latest: DateTime<Utc>| latest.max(published)),
        );
        if published < cutoff {
            continue;
        }

        let raw_summary = entry
            .summary
            .map(|s| s.content)
            .or_else(|| entry.content.and_then(|c| c.body))
            .unwrap_or_default();
        let summary = clean_html(&raw_summary);
        let summary = truncate_chars(&summary, 600);

        articles.push(Article {
            id: hash_id(&url),
            title: clean_html(&title),
            url,
            source: source.name.to_string(),
            category: source.category.to_string(),
            published,
            summary,
            relevance: None,
            topic_tag: None,
            ai_summary: None,
            is_favourite: None,
        });
    }
    Ok((articles, last_item_published))
}

/// Fetches all feeds in parallel, emitting progress per-feed as it completes.
pub async fn fetch_all(
    sources: &[FeedSource],
    hours: i64,
    tx: &UnboundedSender<ProgressEvent>,
) -> (Vec<Article>, Vec<FeedFetchStatus>) {
    let client = reqwest::Client::builder()
        .timeout(StdDuration::from_secs(20))
        .build()
        .expect("client");
    let max_age = Duration::hours(hours);
    let total = sources.len();

    let _ = tx.send(ProgressEvent::Stage {
        stage: "FETCH".into(),
        message: format!("Pulling from {} feeds in parallel…", total),
        percent: 2,
    });

    let mut futs: FuturesUnordered<_> = sources
        .iter()
        .map(|s| {
            let client = client.clone();
            let source = s.clone();
            async move {
                let result = fetch_one(&client, &source, max_age).await;
                (source, result)
            }
        })
        .collect();

    let mut all = Vec::new();
    let mut statuses = Vec::new();
    let mut completed = 0usize;

    while let Some((source, result)) = futs.next().await {
        completed += 1;
        let percent = 2 + (completed * 23 / total) as u8;
        match result {
            Ok((mut articles, last_item_published)) => {
                let count = articles.len();
                let _ = tx.send(ProgressEvent::Stage {
                    stage: "FETCH".into(),
                    message: format!("[{}/{}] {} — {} new", completed, total, source.name, count),
                    percent,
                });
                all.append(&mut articles);
                let status = FeedFetchStatus {
                    feed_url: source.url.clone(),
                    last_item_published,
                    error: None,
                };
                let _ = tx.send(ProgressEvent::FeedStatus(status.clone()));
                statuses.push(status);
            }
            Err(e) => {
                let error = e.to_string();
                let _ = tx.send(ProgressEvent::Stage {
                    stage: "FETCH".into(),
                    message: format!(
                        "[{}/{}] {} — failed ({})",
                        completed, total, source.name, error
                    ),
                    percent,
                });
                let status = FeedFetchStatus {
                    feed_url: source.url.clone(),
                    last_item_published: None,
                    error: Some(error),
                };
                let _ = tx.send(ProgressEvent::FeedStatus(status.clone()));
                statuses.push(status);
            }
        }
    }

    all.sort_by(|a, b| a.url.cmp(&b.url));
    all.dedup_by(|a, b| a.url == b.url);
    all.sort_by(|a, b| b.published.cmp(&a.published));
    (all, statuses)
}
