use anyhow::{Context, Result};
use chrono::NaiveDate;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::feeds::Persona;
use crate::fetcher::Article;
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

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS personas (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT NOT NULL UNIQUE,
                description TEXT NOT NULL,
                feeds_json  TEXT NOT NULL
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
        "#,
        )?;

        // Ensure default persona exists
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM personas", [], |r| r.get(0))?;
        if count == 0 {
            let default_persona = Persona::default();
            let feeds_json = serde_json::to_string(&default_persona.feeds)?;
            conn.execute(
                "INSERT INTO personas (id, name, description, feeds_json) VALUES (?, ?, ?, ?)",
                params![
                    1,
                    default_persona.name,
                    default_persona.description,
                    feeds_json
                ],
            )?;
        }

        Ok(Self { conn })
    }

    pub fn list_personas(&self) -> Result<Vec<Persona>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, name, description, feeds_json FROM personas ORDER BY id ASC")?;
        let personas = stmt
            .query_map([], |row| {
                let feeds_json: String = row.get(3)?;
                Ok(Persona {
                    id: Some(row.get(0)?),
                    name: row.get(1)?,
                    description: row.get(2)?,
                    feeds: serde_json::from_str(&feeds_json).unwrap_or_default(),
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
                    "INSERT INTO personas (id, name, description, feeds_json) VALUES (?, ?, ?, ?)",
                    params![id, persona.name, persona.description, feeds_json],
                )?;
            } else {
                tx.execute(
                    "INSERT INTO personas (name, description, feeds_json) VALUES (?, ?, ?)",
                    params![persona.name, persona.description, feeds_json],
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
                "UPDATE personas SET name = ?, description = ?, feeds_json = ? WHERE id = ?",
                params![persona.name, persona.description, feeds_json, id],
            )?;
            Ok(id)
        } else {
            self.conn.execute(
                "INSERT INTO personas (name, description, feeds_json) VALUES (?, ?, ?)",
                params![persona.name, persona.description, feeds_json],
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
}
