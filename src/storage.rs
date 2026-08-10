use anyhow::{Context, Result};
use chrono::NaiveDate;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::feeds::Persona;
use crate::fetcher::Article;
use crate::llm::LlmConfig;
use crate::progress::BriefStats;

const PERSONA_CONFIG_VERSION: u32 = 1;

pub struct Storage {
    conn: Connection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersonaConfigFile {
    version: u32,
    personas: Vec<Persona>,
}

#[derive(Debug, Clone)]
pub struct StoredBrief {
    pub date: NaiveDate,
    pub headline: String,
    pub brief: String,
    pub articles: Vec<Article>,
    pub stats: BriefStats,
    pub model: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub persona_id: i64,
}

pub fn data_dir() -> PathBuf {
    directories::ProjectDirs::from("com", "feedbrief", "Feedbrief")
        .map(|p| p.data_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."))
}

impl Storage {
    pub fn open() -> Result<Self> {
        let dir = data_dir();
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join("briefs.db");
        let mut conn = Connection::open(path)?;

        // Migration: check if 'briefs' table has 'persona_id' column
        let has_persona_id: bool = conn
            .query_row(
                "SELECT count(*) FROM pragma_table_info('briefs') WHERE name='persona_id'",
                [],
                |r| Ok(r.get::<_, i64>(0)? > 0),
            )
            .unwrap_or(false);

        let table_exists: bool = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='briefs'",
                [],
                |r| Ok(r.get::<_, i64>(0)? > 0),
            )
            .unwrap_or(false);

        if table_exists && !has_persona_id {
            // Need to migrate: rename old table, create new one, copy data
            let tx = conn.transaction()?;
            tx.execute("ALTER TABLE briefs RENAME TO briefs_old", [])?;
            tx.execute_batch(r#"
                CREATE TABLE briefs (
                    date         TEXT NOT NULL,
                    persona_id   INTEGER NOT NULL DEFAULT 1,
                    headline     TEXT NOT NULL DEFAULT '',
                    brief_text   TEXT NOT NULL,
                    articles_json TEXT NOT NULL,
                    feeds_fetched INTEGER NOT NULL,
                    total_articles INTEGER NOT NULL,
                    articles_kept INTEGER NOT NULL,
                    model        TEXT NOT NULL,
                    created_at   TEXT NOT NULL,
                    PRIMARY KEY (date, persona_id)
                );
                INSERT INTO briefs (date, persona_id, headline, brief_text, articles_json, feeds_fetched, total_articles, articles_kept, model, created_at)
                SELECT date, 1, '', brief_text, articles_json, feeds_fetched, total_articles, articles_kept, model, created_at FROM briefs_old;
                DROP TABLE briefs_old;
            "#)?;
            tx.commit()?;
        }

        let has_headline: bool = conn
            .query_row(
                "SELECT count(*) FROM pragma_table_info('briefs') WHERE name='headline'",
                [],
                |r| Ok(r.get::<_, i64>(0)? > 0),
            )
            .unwrap_or(false);

        if table_exists && has_persona_id && !has_headline {
            conn.execute(
                "ALTER TABLE briefs ADD COLUMN headline TEXT NOT NULL DEFAULT ''",
                [],
            )?;
        }

        let has_publish_endpoint: bool = conn
            .query_row(
                "SELECT count(*) FROM pragma_table_info('personas') WHERE name='publish_endpoint'",
                [],
                |r| Ok(r.get::<_, i64>(0)? > 0),
            )
            .unwrap_or(false);

        let personas_table_exists: bool = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='personas'",
                [],
                |r| Ok(r.get::<_, i64>(0)? > 0),
            )
            .unwrap_or(false);

        if personas_table_exists && !has_publish_endpoint {
            let _ = conn.execute(
                "ALTER TABLE personas ADD COLUMN publish_endpoint TEXT NOT NULL DEFAULT 'http://localhost:3000/api/news-digest'",
                [],
            );
            let _ = conn.execute(
                "ALTER TABLE personas ADD COLUMN publish_token TEXT NOT NULL DEFAULT 'YOUR_SECRET_KEY'",
                [],
            );
        }

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS personas (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                name             TEXT NOT NULL UNIQUE,
                description      TEXT NOT NULL,
                feeds_json       TEXT NOT NULL,
                publish_endpoint TEXT NOT NULL DEFAULT 'http://localhost:3000/api/news-digest',
                publish_token    TEXT NOT NULL DEFAULT 'YOUR_SECRET_KEY'
            );

            CREATE TABLE IF NOT EXISTS briefs (
                date         TEXT NOT NULL,
                persona_id   INTEGER NOT NULL DEFAULT 1,
                headline     TEXT NOT NULL DEFAULT '',
                brief_text   TEXT NOT NULL,
                articles_json TEXT NOT NULL,
                feeds_fetched INTEGER NOT NULL,
                total_articles INTEGER NOT NULL,
                articles_kept INTEGER NOT NULL,
                model        TEXT NOT NULL,
                created_at   TEXT NOT NULL,
                PRIMARY KEY (date, persona_id),
                FOREIGN KEY (persona_id) REFERENCES personas(id)
            );

            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
        "#,
        )?;

        // Ensure default persona exists
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM personas", [], |r| r.get(0))?;
        if count == 0 {
            let default_persona = Persona::default();
            let feeds_json = serde_json::to_string(&default_persona.feeds)?;
            conn.execute(
                "INSERT INTO personas (id, name, description, feeds_json, publish_endpoint, publish_token) VALUES (?, ?, ?, ?, ?, ?)",
                params![
                    1,
                    default_persona.name,
                    default_persona.description,
                    feeds_json,
                    default_persona.publish_endpoint,
                    default_persona.publish_token,
                ],
            )?;
        }

        Ok(Self { conn })
    }

    pub fn list_personas(&self) -> Result<Vec<Persona>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, name, description, feeds_json, publish_endpoint, publish_token FROM personas ORDER BY id ASC")?;
        let personas = stmt
            .query_map([], |row| {
                let feeds_json: String = row.get(3)?;
                let publish_endpoint: String = row.get(4).unwrap_or_else(|_| "http://localhost:3000/api/news-digest".to_string());
                let publish_token: String = row.get(5).unwrap_or_else(|_| "YOUR_SECRET_KEY".to_string());
                Ok(Persona {
                    id: Some(row.get(0)?),
                    name: row.get(1)?,
                    description: row.get(2)?,
                    feeds: serde_json::from_str(&feeds_json).unwrap_or_default(),
                    publish_endpoint,
                    publish_token,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(personas)
    }

    pub fn personas_config_path() -> PathBuf {
        data_dir().join("personas.json")
    }

    pub fn export_personas_json(&self) -> Result<String> {
        let personas = self.list_personas()?;
        let archive = PersonaConfigFile {
            version: PERSONA_CONFIG_VERSION,
            personas,
        };
        Ok(serde_json::to_string_pretty(&archive)?)
    }

    pub fn export_personas_to_path<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let json = self.export_personas_json()?;
        std::fs::write(path, json)?;
        Ok(())
    }

    pub fn import_personas_json(&mut self, json: &str) -> Result<usize> {
        let mut archive: PersonaConfigFile = serde_json::from_str(json)?;
        if archive.version != PERSONA_CONFIG_VERSION {
            anyhow::bail!(
                "Unsupported persona config version {} (expected {})",
                archive.version,
                PERSONA_CONFIG_VERSION
            );
        }
        if archive.personas.is_empty() {
            anyhow::bail!("Persona config does not contain any personas");
        }

        if archive.personas.iter().all(|persona| persona.id != Some(1)) {
            archive.personas[0].id = Some(1);
        }

        let imported_count = archive.personas.len();
        let mut ids = HashSet::new();
        let mut names = HashSet::new();
        for persona in &archive.personas {
            let id = persona.id.unwrap_or(-1);
            if !ids.insert(id) {
                anyhow::bail!("Persona config contains duplicate ids");
            }
            if !names.insert(persona.name.clone()) {
                anyhow::bail!("Persona config contains duplicate persona names");
            }
        }

        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM personas", [])?;

        for persona in archive.personas {
            let feeds_json = serde_json::to_string(&persona.feeds)?;
            if let Some(id) = persona.id {
                tx.execute(
                    "INSERT INTO personas (id, name, description, feeds_json, publish_endpoint, publish_token) VALUES (?, ?, ?, ?, ?, ?)",
                    params![
                        id,
                        persona.name,
                        persona.description,
                        feeds_json,
                        persona.publish_endpoint,
                        persona.publish_token,
                    ],
                )?;
            } else {
                tx.execute(
                    "INSERT INTO personas (name, description, feeds_json, publish_endpoint, publish_token) VALUES (?, ?, ?, ?, ?)",
                    params![
                        persona.name,
                        persona.description,
                        feeds_json,
                        persona.publish_endpoint,
                        persona.publish_token,
                    ],
                )?;
            }
        }

        tx.commit()?;
        Ok(imported_count)
    }

    pub fn import_personas_from_path<P: AsRef<Path>>(&mut self, path: P) -> Result<usize> {
        let json = std::fs::read_to_string(path)?;
        self.import_personas_json(&json)
    }

    pub fn save_persona(&self, persona: &Persona) -> Result<i64> {
        let feeds_json = serde_json::to_string(&persona.feeds)?;
        if let Some(id) = persona.id {
            self.conn.execute(
                "UPDATE personas SET name = ?, description = ?, feeds_json = ?, publish_endpoint = ?, publish_token = ? WHERE id = ?",
                params![
                    persona.name,
                    persona.description,
                    feeds_json,
                    persona.publish_endpoint,
                    persona.publish_token,
                    id
                ],
            )?;
            Ok(id)
        } else {
            self.conn.execute(
                "INSERT INTO personas (name, description, feeds_json, publish_endpoint, publish_token) VALUES (?, ?, ?, ?, ?)",
                params![
                    persona.name,
                    persona.description,
                    feeds_json,
                    persona.publish_endpoint,
                    persona.publish_token
                ],
            )?;
            Ok(self.conn.last_insert_rowid())
        }
    }

    pub fn delete_persona(&self, id: i64) -> Result<()> {
        if id == 1 {
            anyhow::bail!("Cannot delete default persona");
        }
        self.conn
            .execute("DELETE FROM briefs WHERE persona_id = ?", params![id])?;
        self.conn
            .execute("DELETE FROM personas WHERE id = ?", params![id])?;
        Ok(())
    }

    /// Save (or overwrite) today's brief.
    pub fn save(
        &self,
        date: NaiveDate,
        persona_id: i64,
        headline: &str,
        brief: &str,
        articles: &[Article],
        stats: &BriefStats,
        model: &str,
    ) -> Result<()> {
        let articles_json = serde_json::to_string(articles)?;
        let now = chrono::Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT OR REPLACE INTO briefs (date, persona_id, headline, brief_text, articles_json, feeds_fetched, total_articles, articles_kept, model, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                date.format("%Y-%m-%d").to_string(),
                persona_id,
                headline,
                brief,
                articles_json,
                stats.feeds_fetched as i64,
                stats.total_articles as i64,
                stats.articles_kept as i64,
                model,
                now,
            ],
        )?;
        Ok(())
    }

    pub fn load(&self, date: NaiveDate, persona_id: i64) -> Result<Option<StoredBrief>> {
        let mut stmt = self.conn.prepare(
            "SELECT date, headline, brief_text, articles_json, feeds_fetched, total_articles, articles_kept, model, created_at, persona_id
             FROM briefs WHERE date = ? AND persona_id = ?",
        )?;
        let mut rows = stmt.query(params![date.format("%Y-%m-%d").to_string(), persona_id])?;
        if let Some(row) = rows.next()? {
            let date_str: String = row.get(0)?;
            let headline: String = row.get(1)?;
            let brief: String = row.get(2)?;
            let articles_json: String = row.get(3)?;
            let stats = BriefStats {
                feeds_fetched: row.get::<_, i64>(4)? as usize,
                total_articles: row.get::<_, i64>(5)? as usize,
                articles_kept: row.get::<_, i64>(6)? as usize,
            };
            let model: String = row.get(7)?;
            let created_at_str: String = row.get(8)?;
            let persona_id: i64 = row.get(9)?;
            let articles: Vec<Article> = serde_json::from_str(&articles_json)?;
            Ok(Some(StoredBrief {
                date: NaiveDate::parse_from_str(&date_str, "%Y-%m-%d")?,
                headline,
                brief,
                articles,
                stats,
                model,
                created_at: chrono::DateTime::parse_from_rfc3339(&created_at_str)?
                    .with_timezone(&chrono::Utc),
                persona_id,
            }))
        } else {
            Ok(None)
        }
    }

    /// All dates that have a brief for a given persona, sorted ascending.
    pub fn all_dates(&self, persona_id: i64) -> Result<Vec<NaiveDate>> {
        let mut stmt = self
            .conn
            .prepare("SELECT date FROM briefs WHERE persona_id = ? ORDER BY date ASC")?;
        let dates: Vec<NaiveDate> = stmt
            .query_map(params![persona_id], |row| {
                let s: String = row.get(0)?;
                Ok(NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                    .unwrap_or_else(|_| NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(dates)
    }

    pub fn previous_date(&self, current: NaiveDate, persona_id: i64) -> Result<Option<NaiveDate>> {
        let result = self.conn.query_row(
            "SELECT date FROM briefs WHERE date < ? AND persona_id = ? ORDER BY date DESC LIMIT 1",
            params![current.format("%Y-%m-%d").to_string(), persona_id],
            |row| {
                let s: String = row.get(0)?;
                Ok(NaiveDate::parse_from_str(&s, "%Y-%m-%d").unwrap())
            },
        ).optional().context("query previous_date")?;
        Ok(result)
    }

    pub fn next_date(&self, current: NaiveDate, persona_id: i64) -> Result<Option<NaiveDate>> {
        let result = self.conn.query_row(
            "SELECT date FROM briefs WHERE date > ? AND persona_id = ? ORDER BY date ASC LIMIT 1",
            params![current.format("%Y-%m-%d").to_string(), persona_id],
            |row| {
                let s: String = row.get(0)?;
                Ok(NaiveDate::parse_from_str(&s, "%Y-%m-%d").unwrap())
            },
        ).optional().context("query next_date")?;
        Ok(result)
    }

    pub fn load_llm_config(&self) -> Result<LlmConfig> {
        let result: Option<String> = self
            .conn
            .query_row(
                "SELECT value FROM settings WHERE key = 'llm_config'",
                [],
                |r| r.get(0),
            )
            .optional()?;

        if let Some(json) = result {
            if let Ok(config) = serde_json::from_str::<LlmConfig>(&json) {
                return Ok(config);
            }
        }
        Ok(LlmConfig::default())
    }

    pub fn save_llm_config(&self, config: &LlmConfig) -> Result<()> {
        let json = serde_json::to_string(config)?;
        self.conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('llm_config', ?)",
            params![json],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn in_memory_storage() -> Storage {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE personas (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                name             TEXT NOT NULL UNIQUE,
                description      TEXT NOT NULL,
                feeds_json       TEXT NOT NULL,
                publish_endpoint TEXT NOT NULL DEFAULT 'http://localhost:3000/api/news-digest',
                publish_token    TEXT NOT NULL DEFAULT 'YOUR_SECRET_KEY'
            );
            "#,
        ).unwrap();
        let default_persona = Persona::default();
        let feeds_json = serde_json::to_string(&default_persona.feeds).unwrap();
        conn.execute(
            "INSERT INTO personas (id, name, description, feeds_json, publish_endpoint, publish_token) VALUES (?, ?, ?, ?, ?, ?)",
            params![
                1,
                default_persona.name,
                default_persona.description,
                feeds_json,
                default_persona.publish_endpoint,
                default_persona.publish_token,
            ],
        ).unwrap();
        Storage { conn }
    }

    #[test]
    fn test_per_persona_publish_configs_save_and_list() {
        let storage = in_memory_storage();

        let mut sec_persona = Persona {
            id: None,
            name: "Security Researcher".to_string(),
            description: "Cybersecurity vulnerabilities and exploits".to_string(),
            feeds: vec![],
            publish_endpoint: "https://security-site.org/api/publish".to_string(),
            publish_token: "sec_secret_token_123".to_string(),
        };

        let sec_id = storage.save_persona(&sec_persona).unwrap();
        sec_persona.id = Some(sec_id);

        let mut cto_persona = Persona {
            id: None,
            name: "CTO Persona".to_string(),
            description: "Executive tech strategy and architecture".to_string(),
            feeds: vec![],
            publish_endpoint: "https://cto-brief.com/v1/digest".to_string(),
            publish_token: "cto_bearer_987".to_string(),
        };

        let cto_id = storage.save_persona(&cto_persona).unwrap();
        cto_persona.id = Some(cto_id);

        let personas = storage.list_personas().unwrap();
        assert_eq!(personas.len(), 3);

        let found_sec = personas.iter().find(|p| p.name == "Security Researcher").unwrap();
        assert_eq!(found_sec.publish_endpoint, "https://security-site.org/api/publish");
        assert_eq!(found_sec.publish_token, "sec_secret_token_123");

        let found_cto = personas.iter().find(|p| p.name == "CTO Persona").unwrap();
        assert_eq!(found_cto.publish_endpoint, "https://cto-brief.com/v1/digest");
        assert_eq!(found_cto.publish_token, "cto_bearer_987");
    }

    #[test]
    fn test_persona_export_import_publish_configs() {
        let mut storage = in_memory_storage();

        let sec_persona = Persona {
            id: None,
            name: "Security Researcher".to_string(),
            description: "Vulnerabilities and malware analysis".to_string(),
            feeds: vec![],
            publish_endpoint: "https://sec.example.com/hooks/publish".to_string(),
            publish_token: "tok_sec_key".to_string(),
        };
        storage.save_persona(&sec_persona).unwrap();

        let json = storage.export_personas_json().unwrap();
        assert!(json.contains("https://sec.example.com/hooks/publish"));
        assert!(json.contains("tok_sec_key"));

        let imported_count = storage.import_personas_json(&json).unwrap();
        assert_eq!(imported_count, 2);

        let personas = storage.list_personas().unwrap();
        let sec_imported = personas.iter().find(|p| p.name == "Security Researcher").unwrap();
        assert_eq!(sec_imported.publish_endpoint, "https://sec.example.com/hooks/publish");
        assert_eq!(sec_imported.publish_token, "tok_sec_key");
    }
}

