use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FeedSource {
    pub name: String,
    pub url: String,
    pub category: String,
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
        FeedSource {
            name: "Hugging Face Blog".into(),
            url: "https://huggingface.co/blog/feed.xml".into(),
            category: "AI/ML".into(),
        },
        FeedSource {
            name: "Google DeepMind".into(),
            url: "https://deepmind.google/blog/rss.xml".into(),
            category: "AI/ML".into(),
        },
        FeedSource {
            name: "OpenAI Blog".into(),
            url: "https://openai.com/blog/rss.xml".into(),
            category: "AI/ML".into(),
        },
        FeedSource {
            name: "Anthropic News".into(),
            url: "https://www.anthropic.com/news/rss.xml".into(),
            category: "AI/ML".into(),
        },
        FeedSource {
            name: "BAIR Blog".into(),
            url: "https://bair.berkeley.edu/blog/feed.xml".into(),
            category: "AI/ML".into(),
        },
        FeedSource {
            name: "Import AI".into(),
            url: "https://importai.substack.com/feed".into(),
            category: "AI/ML".into(),
        },
        FeedSource {
            name: "arXiv cs.AI".into(),
            url: "http://export.arxiv.org/rss/cs.AI".into(),
            category: "Research".into(),
        },
        FeedSource {
            name: "arXiv cs.LG".into(),
            url: "http://export.arxiv.org/rss/cs.LG".into(),
            category: "Research".into(),
        },
        FeedSource {
            name: "arXiv cs.CL".into(),
            url: "http://export.arxiv.org/rss/cs.CL".into(),
            category: "Research".into(),
        },
        FeedSource {
            name: "arXiv cs.CR".into(),
            url: "http://export.arxiv.org/rss/cs.CR".into(),
            category: "Research".into(),
        },
        FeedSource {
            name: "arXiv cs.RO".into(),
            url: "http://export.arxiv.org/rss/cs.RO".into(),
            category: "Research".into(),
        },
        FeedSource {
            name: "TechCrunch".into(),
            url: "https://techcrunch.com/feed/".into(),
            category: "Startups".into(),
        },
        FeedSource {
            name: "Hacker News Front".into(),
            url: "https://hnrss.org/frontpage".into(),
            category: "Startups".into(),
        },
        FeedSource {
            name: "Y Combinator Blog".into(),
            url: "https://www.ycombinator.com/blog/rss".into(),
            category: "Startups".into(),
        },
        FeedSource {
            name: "Crunchbase News".into(),
            url: "https://news.crunchbase.com/feed/".into(),
            category: "Startups".into(),
        },
        FeedSource {
            name: "SemiAnalysis".into(),
            url: "https://www.semianalysis.com/feed".into(),
            category: "Hardware".into(),
        },
        FeedSource {
            name: "IEEE Spectrum".into(),
            url: "https://spectrum.ieee.org/feeds/feed.rss".into(),
            category: "Hardware".into(),
        },
        FeedSource {
            name: "AnandTech".into(),
            url: "https://www.anandtech.com/rss/".into(),
            category: "Hardware".into(),
        },
        FeedSource {
            name: "Krebs on Security".into(),
            url: "https://krebsonsecurity.com/feed/".into(),
            category: "Security".into(),
        },
        FeedSource {
            name: "The Hacker News".into(),
            url: "https://feeds.feedburner.com/TheHackersNews".into(),
            category: "Security".into(),
        },
        FeedSource {
            name: "Schneier on Security".into(),
            url: "https://www.schneier.com/feed/atom/".into(),
            category: "Security".into(),
        },
        FeedSource {
            name: "Ars Technica".into(),
            url: "https://feeds.arstechnica.com/arstechnica/index".into(),
            category: "Tech".into(),
        },
        FeedSource {
            name: "The Verge".into(),
            url: "https://www.theverge.com/rss/index.xml".into(),
            category: "Tech".into(),
        },
        FeedSource {
            name: "MIT Tech Review".into(),
            url: "https://www.technologyreview.com/feed/".into(),
            category: "Tech".into(),
        },
        FeedSource {
            name: "Wired".into(),
            url: "https://www.wired.com/feed/rss".into(),
            category: "Tech".into(),
        },
        FeedSource {
            name: "Quanta Magazine".into(),
            url: "https://www.quantamagazine.org/feed/".into(),
            category: "Science".into(),
        },
        FeedSource {
            name: "Nature News".into(),
            url: "https://www.nature.com/nature.rss".into(),
            category: "Science".into(),
        },
        FeedSource {
            name: "Science News".into(),
            url: "https://www.sciencenews.org/feed".into(),
            category: "Science".into(),
        },
    ]
}
