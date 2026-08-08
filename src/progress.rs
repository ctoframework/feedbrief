use crate::fetcher::Article;

#[derive(Debug, Clone)]
pub enum ProgressEvent {
    Stage {
        stage: String,
        message: String,
        percent: u8,
    },
    Done {
        headline: String,
        brief: String,
        articles: Vec<Article>,
        stats: BriefStats,
    },
    Error(String),
}

#[derive(Debug, Clone, Default)]
pub struct BriefStats {
    pub feeds_fetched: usize,
    pub total_articles: usize,
    pub articles_kept: usize,
}
