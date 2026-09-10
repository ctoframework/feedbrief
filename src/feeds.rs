use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FeedSource {
    pub name: String,
    pub url: String,
    pub category: String,
    #[serde(default = "default_active")]
    pub active: bool,
}

fn default_active() -> bool {
    true
}

impl FeedSource {
    fn new(name: &str, url: &str, category: &str) -> Self {
        Self {
            name: name.into(),
            url: url.into(),
            category: category.into(),
            active: true,
        }
    }
}

fn default_publish_endpoint() -> String {
    "http://localhost:3000/api/news-digest".to_string()
}

fn default_publish_token() -> String {
    "YOUR_SECRET_KEY".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Persona {
    pub id: Option<i64>,
    pub name: String,
    pub description: String,
    pub feeds: Vec<FeedSource>,
    #[serde(default = "default_publish_endpoint")]
    pub publish_endpoint: String,
    #[serde(default = "default_publish_token")]
    pub publish_token: String,
}

impl Default for Persona {
    fn default() -> Self {
        Self {
            id: Some(1),
            name: "Default".to_string(),
            description: "AI/ML breakthroughs, startups & funding, computer-science research, hardware/chips, robotics, cybersecurity, and emerging tech themes.".to_string(),
            feeds: default_feeds(),
            publish_endpoint: default_publish_endpoint(),
            publish_token: default_publish_token(),
        }
    }
}

pub fn default_feeds() -> Vec<FeedSource> {
    vec![
        FeedSource::new(
            "Hugging Face Blog",
            "https://huggingface.co/blog/feed.xml",
            "AI/ML",
        ),
        FeedSource::new(
            "Google DeepMind",
            "https://deepmind.google/blog/rss.xml",
            "AI/ML",
        ),
        FeedSource::new("OpenAI Blog", "https://openai.com/blog/rss.xml", "AI/ML"),
        FeedSource::new(
            "Anthropic News",
            "https://www.anthropic.com/news/rss.xml",
            "AI/ML",
        ),
        FeedSource::new(
            "BAIR Blog",
            "https://bair.berkeley.edu/blog/feed.xml",
            "AI/ML",
        ),
        FeedSource::new("Import AI", "https://importai.substack.com/feed", "AI/ML"),
        FeedSource::new(
            "arXiv cs.AI",
            "http://export.arxiv.org/rss/cs.AI",
            "Research",
        ),
        FeedSource::new(
            "arXiv cs.LG",
            "http://export.arxiv.org/rss/cs.LG",
            "Research",
        ),
        FeedSource::new(
            "arXiv cs.CL",
            "http://export.arxiv.org/rss/cs.CL",
            "Research",
        ),
        FeedSource::new(
            "arXiv cs.CR",
            "http://export.arxiv.org/rss/cs.CR",
            "Research",
        ),
        FeedSource::new(
            "arXiv cs.RO",
            "http://export.arxiv.org/rss/cs.RO",
            "Research",
        ),
        FeedSource::new("TechCrunch", "https://techcrunch.com/feed/", "Startups"),
        FeedSource::new(
            "Hacker News Front",
            "https://hnrss.org/frontpage",
            "Startups",
        ),
        FeedSource::new(
            "Y Combinator Blog",
            "https://www.ycombinator.com/blog/rss",
            "Startups",
        ),
        FeedSource::new(
            "Crunchbase News",
            "https://news.crunchbase.com/feed/",
            "Startups",
        ),
        FeedSource::new(
            "SemiAnalysis",
            "https://www.semianalysis.com/feed",
            "Hardware",
        ),
        FeedSource::new(
            "IEEE Spectrum",
            "https://spectrum.ieee.org/feeds/feed.rss",
            "Hardware",
        ),
        FeedSource::new("AnandTech", "https://www.anandtech.com/rss/", "Hardware"),
        FeedSource::new(
            "Krebs on Security",
            "https://krebsonsecurity.com/feed/",
            "Security",
        ),
        FeedSource::new(
            "The Hacker News",
            "https://feeds.feedburner.com/TheHackersNews",
            "Security",
        ),
        FeedSource::new(
            "Schneier on Security",
            "https://www.schneier.com/feed/atom/",
            "Security",
        ),
        FeedSource::new(
            "Ars Technica",
            "https://feeds.arstechnica.com/arstechnica/index",
            "Tech",
        ),
        FeedSource::new(
            "The Verge",
            "https://www.theverge.com/rss/index.xml",
            "Tech",
        ),
        FeedSource::new(
            "MIT Tech Review",
            "https://www.technologyreview.com/feed/",
            "Tech",
        ),
        FeedSource::new("Wired", "https://www.wired.com/feed/rss", "Tech"),
        FeedSource::new(
            "Quanta Magazine",
            "https://www.quantamagazine.org/feed/",
            "Science",
        ),
        FeedSource::new(
            "Nature News",
            "https://www.nature.com/nature.rss",
            "Science",
        ),
        FeedSource::new(
            "Science News",
            "https://www.sciencenews.org/feed",
            "Science",
        ),
    ]
}
