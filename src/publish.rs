use crate::fetcher::Article;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishDigestPayload {
    pub date: String,
    pub headline: String,
    pub executive_brief: String,
    pub tags: Vec<String>,
    pub content: String,
    pub sources: Vec<PublishSource>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PublishSource {
    pub title: String,
    pub url: String,
    pub description: String,
}

pub fn build_publish_payload(
    date: chrono::NaiveDate,
    headline: &str,
    brief_text: &str,
    articles: &[Article],
) -> PublishDigestPayload {
    let date_str = format!("{}T10:00:00.000Z", date.format("%Y-%m-%d"));
    let mut tags: Vec<String> = articles
        .iter()
        .filter_map(|a| a.topic_tag.clone())
        .map(|t| t.to_lowercase())
        .collect();
    tags.sort();
    tags.dedup();
    if tags.is_empty() {
        tags.push("news".to_string());
    }

    let mut content = String::new();
    content.push_str("### Executive Briefing\n\n");
    content.push_str(brief_text);
    content.push_str("\n\n### Key Stories\n\n");
    for a in articles {
        content.push_str(&format!("#### [{}]({})\n", a.title, a.url));
        let summary = a.ai_summary.as_deref().unwrap_or(&a.summary);
        content.push_str(summary);
        content.push_str("\n\n");
    }

    let sources = articles
        .iter()
        .map(|a| PublishSource {
            title: a.title.clone(),
            url: a.url.clone(),
            description: a.ai_summary.clone().unwrap_or_else(|| a.summary.clone()),
        })
        .collect();

    PublishDigestPayload {
        date: date_str,
        headline: headline.to_string(),
        executive_brief: brief_text.to_string(),
        tags,
        content,
        sources,
    }
}

pub async fn do_publish_http(
    endpoint: &str,
    token: &str,
    payload: &PublishDigestPayload,
) -> Result<String, String> {
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
    {
        Ok(c) => c,
        Err(e) => return Err(format!("HTTP client error: {}", e)),
    };

    let mut req = client
        .post(endpoint)
        .header("Content-Type", "application/json");

    let trimmed_token = token.trim();
    if !trimmed_token.is_empty() {
        let auth_val = if trimmed_token.to_lowercase().starts_with("bearer ") {
            trimmed_token.to_string()
        } else {
            format!("Bearer {}", trimmed_token)
        };
        req = req.header("Authorization", auth_val);
    }

    match req.json(payload).send().await {
        Ok(resp) => {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            if status.is_success() {
                Ok(format!("Digest published successfully! (HTTP {})", status))
            } else {
                Err(format!("Publish failed with HTTP {}: {}", status, text))
            }
        }
        Err(e) => Err(format!("Network request failed: {}", e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_publish_digest_payload_serialization() {
        let payload = PublishDigestPayload {
            date: "2026-08-07T10:00:00.000Z".to_string(),
            headline: "Quantum Computing Milestones & Cloud AI Scaling Standards".to_string(),
            executive_brief: "Key industry advancements in GPU cluster interconnects and multi-agent governance frameworks.".to_string(),
            tags: vec!["ai".to_string(), "architecture".to_string(), "infrastructure".to_string()],
            content: "### Major Breakthroughs\n\nDetailed breakdown of new LLM serving optimisations...".to_string(),
            sources: vec![
                PublishSource {
                    title: "ArXiv AI Infrastructure Paper".to_string(),
                    url: "https://arxiv.org".to_string(),
                    description: "Open access research on cluster scaling.".to_string(),
                }
            ],
        };

        let json = serde_json::to_string_pretty(&payload).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(val["date"], "2026-08-07T10:00:00.000Z");
        assert_eq!(
            val["headline"],
            "Quantum Computing Milestones & Cloud AI Scaling Standards"
        );
        assert_eq!(
            val["executiveBrief"],
            "Key industry advancements in GPU cluster interconnects and multi-agent governance frameworks."
        );
        assert_eq!(val["tags"][0], "ai");
        assert_eq!(val["sources"][0]["title"], "ArXiv AI Infrastructure Paper");
    }
}

