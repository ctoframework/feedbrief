use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;

use crate::fetcher::Article;
use crate::progress::ProgressEvent;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ProviderType {
    Ollama,
    OpenRouter,
}

impl std::fmt::Display for ProviderType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProviderType::Ollama => write!(f, "Ollama"),
            ProviderType::OpenRouter => write!(f, "OpenRouter"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LlmConfig {
    pub provider: ProviderType,
    pub ollama_url: String,
    pub ollama_model: String,
    pub openrouter_api_key: String,
    pub openrouter_model: String,
    pub openrouter_url: String,
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            provider: ProviderType::Ollama,
            ollama_url: "http://localhost:11434".to_string(),
            ollama_model: "llama3.1:8b".to_string(),
            openrouter_api_key: String::new(),
            openrouter_model: "openai/gpt-4o-mini".to_string(),
            openrouter_url: "https://openrouter.ai/api/v1".to_string(),
        }
    }
}

impl LlmConfig {
    pub fn active_model(&self) -> &str {
        match self.provider {
            ProviderType::Ollama => &self.ollama_model,
            ProviderType::OpenRouter => &self.openrouter_model,
        }
    }

    pub fn active_model_display(&self) -> String {
        match self.provider {
            ProviderType::Ollama => self.ollama_model.clone(),
            ProviderType::OpenRouter => format!("openrouter:{}", self.openrouter_model),
        }
    }

    pub fn provider_label(&self) -> &'static str {
        match self.provider {
            ProviderType::Ollama => "OLLAMA",
            ProviderType::OpenRouter => "OPENROUTER",
        }
    }
}

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
    base_url: &str,
    model: &str,
    prompt: String,
    json_mode: bool,
    max_tokens: i32,
) -> Result<String> {
    let endpoint = format!("{}/api/generate", base_url.trim_end_matches('/'));
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
        .post(&endpoint)
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

#[derive(Serialize)]
struct OpenRouterMessage<'a> {
    role: &'a str,
    content: String,
}

#[derive(Serialize)]
struct OpenRouterResponseFormat<'a> {
    #[serde(rename = "type")]
    format_type: &'a str,
}

#[derive(Serialize)]
struct OpenRouterRequest<'a> {
    model: &'a str,
    messages: Vec<OpenRouterMessage<'a>>,
    temperature: f32,
    max_tokens: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_format: Option<OpenRouterResponseFormat<'a>>,
}

#[derive(Deserialize)]
struct OpenRouterChoice {
    message: OpenRouterChatMessage,
}

#[derive(Deserialize)]
struct OpenRouterChatMessage {
    content: Option<String>,
}

#[derive(Deserialize)]
struct OpenRouterResponse {
    choices: Option<Vec<OpenRouterChoice>>,
    error: Option<OpenRouterErrorDetail>,
}

#[derive(Deserialize)]
struct OpenRouterErrorDetail {
    message: Option<String>,
}

async fn openrouter_call(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    model: &str,
    prompt: String,
    json_mode: bool,
    max_tokens: i32,
) -> Result<String> {
    if api_key.trim().is_empty() {
        anyhow::bail!("OpenRouter API key is missing. Please configure it in LLM Settings.");
    }

    let endpoint = format!("{}/chat/completions", base_url.trim_end_matches('/'));

    let req = OpenRouterRequest {
        model,
        messages: vec![OpenRouterMessage {
            role: "user",
            content: prompt,
        }],
        temperature: 0.2,
        max_tokens,
        response_format: if json_mode {
            Some(OpenRouterResponseFormat {
                format_type: "json_object",
            })
        } else {
            None
        },
    };

    let resp = client
        .post(&endpoint)
        .header("Authorization", format!("Bearer {}", api_key.trim()))
        .header("HTTP-Referer", "https://ctoframework.com")
        .header("X-Title", "Feedbrief")
        .timeout(Duration::from_secs(240))
        .json(&req)
        .send()
        .await
        .context("Failed to send request to OpenRouter API")?;

    let status = resp.status();
    let text = resp
        .text()
        .await
        .context("Failed to read OpenRouter response body")?;

    if !status.is_success() {
        if json_mode && status.as_u16() == 400 && text.contains("response_format") {
            let req_no_format = OpenRouterRequest {
                model,
                messages: vec![OpenRouterMessage {
                    role: "user",
                    content: req.messages[0].content.clone(),
                }],
                temperature: 0.2,
                max_tokens,
                response_format: None,
            };
            let resp_retry = client
                .post(&endpoint)
                .header("Authorization", format!("Bearer {}", api_key.trim()))
                .header("HTTP-Referer", "https://ctoframework.com")
                .header("X-Title", "Feedbrief")
                .timeout(Duration::from_secs(240))
                .json(&req_no_format)
                .send()
                .await
                .context("Failed to send retry request to OpenRouter API")?;

            let status_retry = resp_retry.status();
            let text_retry = resp_retry
                .text()
                .await
                .context("Failed to read OpenRouter retry response body")?;

            if !status_retry.is_success() {
                anyhow::bail!("OpenRouter returned error {}: {}", status_retry, text_retry);
            }
            let body: OpenRouterResponse = serde_json::from_str(&text_retry)
                .with_context(|| format!("Failed to parse OpenRouter JSON: {}", text_retry))?;

            if let Some(err) = body.error {
                anyhow::bail!("OpenRouter API error: {}", err.message.unwrap_or_default());
            }

            if let Some(choices) = body.choices {
                if let Some(first) = choices.into_iter().next() {
                    if let Some(content) = first.message.content {
                        return Ok(content);
                    }
                }
            }
            anyhow::bail!("OpenRouter response contained no choices/content");
        }

        anyhow::bail!("OpenRouter returned error {}: {}", status, text);
    }

    let body: OpenRouterResponse = serde_json::from_str(&text)
        .with_context(|| format!("Failed to parse OpenRouter JSON: {}", text))?;

    if let Some(err) = body.error {
        anyhow::bail!("OpenRouter API error: {}", err.message.unwrap_or_default());
    }

    if let Some(choices) = body.choices {
        if let Some(first) = choices.into_iter().next() {
            if let Some(content) = first.message.content {
                return Ok(content);
            }
        }
    }

    anyhow::bail!("OpenRouter response contained no choices/content")
}

async fn llm_call(
    client: &reqwest::Client,
    config: &LlmConfig,
    prompt: String,
    json_mode: bool,
    max_tokens: i32,
) -> Result<String> {
    match config.provider {
        ProviderType::Ollama => {
            ollama_call(
                client,
                &config.ollama_url,
                &config.ollama_model,
                prompt,
                json_mode,
                max_tokens,
            )
            .await
        }
        ProviderType::OpenRouter => {
            openrouter_call(
                client,
                &config.openrouter_url,
                &config.openrouter_api_key,
                &config.openrouter_model,
                prompt,
                json_mode,
                max_tokens,
            )
            .await
        }
    }
}

#[derive(Deserialize)]
struct ScoringResult {
    relevance: f32,
    topic: String,
}

pub async fn score_articles(
    client: &reqwest::Client,
    config: &LlmConfig,
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

        let response = llm_call(client, config, prompt, true, 400).await?;

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

pub fn strip_summary_preamble(summary: &str) -> String {
    let mut text = summary.trim();

    if let Some((first_line, rest)) = text.split_once('\n') {
        let first_trimmed = first_line.trim();
        let lower = first_trimmed.to_lowercase();
        if (lower.starts_with("here is")
            || lower.starts_with("here's")
            || lower.starts_with("below is")
            || lower.starts_with("sure,")
            || lower.starts_with("sure!")
            || lower.starts_with("summary:")
            || lower.starts_with("two-sentence summary:")
            || lower.starts_with("2-sentence summary:")
            || lower.ends_with(':'))
            && (lower.contains("summary")
                || lower.contains("here is")
                || lower.contains("here's")
                || lower.ends_with(':'))
        {
            text = rest.trim();
        }
    }

    let lower = text.to_lowercase();
    let inline_preamble_prefixes = [
        "here is a 2-sentence summary of the news item for a chief technology officer:",
        "here is a 2-sentence summary of the news item:",
        "here is a 2-sentence summary of the news:",
        "here is a 2-sentence summary:",
        "here is a two-sentence summary:",
        "here is a summary of the article:",
        "here is a summary of the news:",
        "here is a summary:",
        "here is the 2-sentence summary:",
        "here is the summary:",
        "here's a 2-sentence summary of the news item:",
        "here's a 2-sentence summary of the news:",
        "here's a 2-sentence summary:",
        "here's a two-sentence summary:",
        "here's a summary of the news:",
        "here's a summary:",
        "here's the summary:",
        "two-sentence summary:",
        "2-sentence summary:",
        "summary:",
    ];

    for prefix in inline_preamble_prefixes {
        if lower.starts_with(prefix) {
            let after = text[prefix.len()..].trim();
            if !after.is_empty() {
                text = after;
                break;
            }
        }
    }

    if let Some(idx) = text.find(':') {
        let prefix = text[..idx].trim();
        let prefix_lower = prefix.to_lowercase();
        if (prefix_lower.starts_with("here is")
            || prefix_lower.starts_with("here's")
            || prefix_lower.starts_with("below is")
            || prefix_lower.starts_with("sure")
            || prefix_lower == "summary"
            || prefix_lower == "two-sentence summary"
            || prefix_lower == "2-sentence summary")
            && (prefix_lower.contains("summary")
                || prefix_lower.contains("here is")
                || prefix_lower.contains("here's"))
        {
            let after = text[idx + 1..].trim();
            if !after.is_empty() {
                text = after;
            }
        }
    }

    text.trim_start_matches('"')
        .trim_start_matches('\'')
        .trim()
        .to_string()
}

pub async fn summarize_article(
    client: &reqwest::Client,
    config: &LlmConfig,
    persona_name: &str,
    article: &Article,
) -> Result<String> {
    let body: String = article.summary.chars().take(1500).collect();
    if body.trim().is_empty() {
        anyhow::bail!("Article content is empty; cannot summarize");
    }

    let prompt = format!(
        r#"Summarize the following news item in EXACTLY 2 sentences for {}. Be concrete: name the actors, the number, the technique, the impact. No fluff, no "in this article", no editorializing.
Output ONLY the 2-sentence summary itself. Do NOT include any preamble, introductory text, or labels such as "Here is a 2-sentence summary" or "Summary:". Start directly with the summary text.

Title: {}
Source: {}
Content: {}

Summary:"#,
        persona_name, article.title, article.source, body
    );
    let response = llm_call(client, config, prompt, false, 200).await?;
    let summary = strip_summary_preamble(&response);

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
    config: &LlmConfig,
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

    let response = llm_call(client, config, prompt, true, 450).await?;

    if let Ok(output) = parse_brief_output(&response) {
        return Ok(output);
    }

    Ok(fallback_brief_parse(&response))
}

pub fn llm_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(240))
        .build()
        .expect("llm client")
}

#[allow(dead_code)]
pub fn ollama_client() -> reqwest::Client {
    llm_client()
}

pub async fn check_llm_provider(config: &LlmConfig) -> bool {
    match config.provider {
        ProviderType::Ollama => check_ollama(&config.ollama_url, &config.ollama_model).await,
        ProviderType::OpenRouter => check_openrouter(&config.openrouter_url, &config.openrouter_api_key).await,
    }
}

pub async fn check_ollama(base_url: &str, model: &str) -> bool {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };
    let endpoint = format!("{}/api/tags", base_url.trim_end_matches('/'));
    let resp = match client.get(&endpoint).send().await {
        Ok(r) => r,
        Err(_) => return false,
    };
    if !resp.status().is_success() {
        return false;
    }
    let body: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(_) => return false,
    };
    if model.is_empty() {
        return true;
    }
    body.get("models")
        .and_then(|m| m.as_array())
        .map(|arr| {
            arr.iter().any(|m| {
                m.get("name")
                    .and_then(|n| n.as_str())
                    .map(|s| s.starts_with(model) || model.starts_with(s))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

pub async fn check_openrouter(base_url: &str, api_key: &str) -> bool {
    if api_key.trim().is_empty() {
        return false;
    }
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };
    let endpoint = format!("{}/auth/key", base_url.trim_end_matches('/'));
    let resp = match client
        .get(&endpoint)
        .header("Authorization", format!("Bearer {}", api_key.trim()))
        .send()
        .await
    {
        Ok(r) => r,
        Err(_) => return false,
    };
    resp.status().is_success()
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

    #[test]
    fn test_strip_summary_preamble() {
        let cases = [
            (
                "Here is a 2-sentence summary of the news item for a Chief Technology Officer:\n\nNvidia launched Blackwell GPUs. They feature 208 billion transistors.",
                "Nvidia launched Blackwell GPUs. They feature 208 billion transistors.",
            ),
            (
                "Here is a 2-sentence summary of the news: AMD announced MI300X. Memory bandwidth reached 5.3 TB/s.",
                "AMD announced MI300X. Memory bandwidth reached 5.3 TB/s.",
            ),
            (
                "Summary: Meta released Llama 3. The 70B model was trained on 15T tokens.",
                "Meta released Llama 3. The 70B model was trained on 15T tokens.",
            ),
            (
                "Direct summary sentence one. Direct summary sentence two.",
                "Direct summary sentence one. Direct summary sentence two.",
            ),
        ];

        for (input, expected) in cases {
            assert_eq!(strip_summary_preamble(input), expected);
        }
    }

    #[test]
    fn test_llm_config_defaults() {
        let cfg = LlmConfig::default();
        assert_eq!(cfg.provider, ProviderType::Ollama);
        assert_eq!(cfg.active_model(), "llama3.1:8b");
        assert_eq!(cfg.active_model_display(), "llama3.1:8b");
        assert_eq!(cfg.provider_label(), "OLLAMA");
    }

    #[test]
    fn test_llm_config_openrouter() {
        let mut cfg = LlmConfig::default();
        cfg.provider = ProviderType::OpenRouter;
        cfg.openrouter_model = "openai/gpt-4o-mini".to_string();
        cfg.openrouter_api_key = "sk-or-v1-testkey".to_string();

        assert_eq!(cfg.active_model(), "openai/gpt-4o-mini");
        assert_eq!(cfg.active_model_display(), "openrouter:openai/gpt-4o-mini");
        assert_eq!(cfg.provider_label(), "OPENROUTER");

        let json = serde_json::to_string(&cfg).expect("serialize");
        let deserialized: LlmConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(cfg, deserialized);
    }
}


