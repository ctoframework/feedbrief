use chrono::{Local, NaiveDate};
use eframe::egui::{self, Color32, FontFamily, FontId, RichText, Stroke, Vec2};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

use crate::feeds::Persona;
use crate::fetcher::Article;
use crate::llm::{LlmConfig, ProviderType, check_llm_provider};
use crate::pipeline::{PipelineConfig, run_pipeline};
use crate::progress::{BriefStats, ProgressEvent};
use crate::storage::{Storage, StoredBrief};
use crate::publish::{build_publish_payload, do_publish_http};

const BG: Color32 = Color32::from_rgb(14, 13, 10);
const BG_RAISED: Color32 = Color32::from_rgb(22, 20, 15);
const BG_PAPER: Color32 = Color32::from_rgb(28, 26, 20);
const INK: Color32 = Color32::from_rgb(244, 237, 224);
const INK_DIM: Color32 = Color32::from_rgb(184, 175, 155);
const INK_FAINT: Color32 = Color32::from_rgb(107, 101, 85);
const RULE: Color32 = Color32::from_rgb(42, 38, 32);
const ACCENT: Color32 = Color32::from_rgb(255, 87, 34);
const GOLD: Color32 = Color32::from_rgb(212, 168, 87);
const GREEN: Color32 = Color32::from_rgb(126, 184, 143);

#[derive(PartialEq, Clone, Debug)]
pub enum PersonasSubView {
    List,
    Form { index: Option<usize> },
}

#[derive(PartialEq, Clone, Debug)]
enum View {
    Idle,
    Loading,
    Results,
    PersonasConfig(PersonasSubView),
    History,
}

// Published structs imported from publish module

pub struct FeedbriefApp {
    runtime: Arc<tokio::runtime::Runtime>,
    storage: Storage,
    view: View,

    personas: Vec<Persona>,
    selected_persona_idx: usize,
    editing_persona: Persona,
    editing_persona_idx: Option<usize>,
    editing_feeds_text: String,
    delete_confirm_target: Option<usize>,
    delete_brief_confirm_target: Option<NaiveDate>,
    persona_export_path: String,

    persona_import_path: String,
    persona_message: String,
    persona_message_is_error: bool,

    llm_config: LlmConfig,
    llm_settings_open: bool,
    hours: i64,
    top_n: usize,

    progress_rx: Option<UnboundedReceiver<ProgressEvent>>,
    progress_log: Arc<Mutex<Vec<String>>>,
    current_stage: String,
    current_message: String,
    current_percent: u8,

    current_brief: Option<DisplayedBrief>,
    topic_filter: String,

    llm_ok: bool,
    last_llm_check: std::time::Instant,
    llm_check_rx: Option<tokio::sync::oneshot::Receiver<bool>>,

    available_dates: Vec<NaiveDate>,

    publish_endpoint: String,
    publish_token: String,
    publish_settings_open: bool,
    publish_in_progress: bool,
    publish_result_msg: Option<(bool, String)>,
    publish_rx: Option<tokio::sync::oneshot::Receiver<Result<String, String>>>,
}

#[derive(Clone)]
struct DisplayedBrief {
    date: NaiveDate,
    headline: String,
    brief: String,
    articles: Vec<Article>,
    stats: BriefStats,
    model: String,
}

impl DisplayedBrief {
    fn from_stored(s: StoredBrief) -> Self {
        let headline = if s.headline.is_empty() {
            "Daily Intelligence Briefing".to_string()
        } else {
            s.headline
        };
        Self {
            date: s.date,
            headline,
            brief: s.brief,
            articles: s.articles,
            stats: s.stats,
            model: s.model,
        }
    }
}

impl FeedbriefApp {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        configure_fonts(&cc.egui_ctx);
        configure_style(&cc.egui_ctx);

        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("tokio runtime"),
        );
        let storage = Storage::open().expect("open storage");
        let personas = storage
            .list_personas()
            .unwrap_or_else(|_| vec![Persona::default()]);
        let selected_persona_idx = 0;
        let selected_persona = &personas[selected_persona_idx];
        let publish_endpoint = selected_persona.publish_endpoint.clone();
        let publish_token = selected_persona.publish_token.clone();

        let available_dates = storage
            .all_dates(selected_persona.id.unwrap_or(1))
            .unwrap_or_default();
        let today = Local::now().date_naive();
        let current_brief = storage
            .load(today, selected_persona.id.unwrap_or(1))
            .ok()
            .flatten()
            .map(DisplayedBrief::from_stored);
        let view = if current_brief.is_some() {
            View::Results
        } else {
            View::Idle
        };

        let llm_config = storage.load_llm_config().unwrap_or_default();

        Self {
            runtime,
            storage,
            view,
            personas,
            selected_persona_idx,
            editing_persona: Persona::default(),
            editing_persona_idx: None,
            editing_feeds_text: String::new(),
            delete_confirm_target: None,
            delete_brief_confirm_target: None,
            persona_export_path: Storage::personas_config_path().display().to_string(),
            persona_import_path: Storage::personas_config_path().display().to_string(),
            persona_message: String::new(),
            persona_message_is_error: false,
            llm_config,
            llm_settings_open: false,
            hours: 24,
            top_n: 20,
            progress_rx: None,
            progress_log: Arc::new(Mutex::new(Vec::new())),
            current_stage: String::new(),
            current_message: String::new(),
            current_percent: 0,
            current_brief,
            topic_filter: "all".to_string(),
            llm_ok: false,
            last_llm_check: std::time::Instant::now() - std::time::Duration::from_secs(60),
            llm_check_rx: None,
            available_dates,
            publish_endpoint,
            publish_token,
            publish_settings_open: false,
            publish_in_progress: false,
            publish_result_msg: None,
            publish_rx: None,
        }
    }

    fn start_fetch(&mut self) {
        let (tx, rx) = unbounded_channel();
        self.progress_rx = Some(rx);
        self.progress_log.lock().unwrap().clear();
        self.current_stage = "INIT".to_string();
        self.current_message = "Starting…".to_string();
        self.current_percent = 0;
        self.view = View::Loading;

        let persona = self.personas[self.selected_persona_idx].clone();
        let cfg = PipelineConfig {
            llm_config: self.llm_config.clone(),
            hours: self.hours,
            top_n: self.top_n,
            persona,
        };
        self.runtime.spawn(async move {
            run_pipeline(cfg, tx).await;
        });
    }

    fn poll_progress(&mut self, ctx: &egui::Context) {
        let mut completed = false;
        if let Some(rx) = self.progress_rx.as_mut() {
            while let Ok(event) = rx.try_recv() {
                match event {
                    ProgressEvent::Stage {
                        stage,
                        message,
                        percent,
                    } => {
                        self.current_stage = stage.clone();
                        self.current_message = message.clone();
                        self.current_percent = percent;
                        self.progress_log
                            .lock()
                            .unwrap()
                            .push(format!("[{}] {}", stage, message));
                    }
                    ProgressEvent::Done {
                        headline,
                        brief,
                        articles,
                        stats,
                    } => {
                        let today = Local::now().date_naive();
                        let persona_id = self.personas[self.selected_persona_idx].id.unwrap_or(1);
                        let _ = self.storage.save(
                            today,
                            persona_id,
                            &headline,
                            &brief,
                            &articles,
                            &stats,
                            &self.llm_config.active_model_display(),
                        );
                        self.available_dates =
                            self.storage.all_dates(persona_id).unwrap_or_default();
                        self.current_brief = Some(DisplayedBrief {
                            date: today,
                            headline,
                            brief,
                            articles,
                            stats,
                            model: self.llm_config.active_model_display(),
                        });
                        self.topic_filter = "all".to_string();
                        self.view = View::Results;
                        completed = true;
                    }
                    ProgressEvent::Error(e) => {
                        self.current_stage = "ERROR".to_string();
                        self.current_message = e;
                        self.current_percent = 0;
                    }
                }
            }
        }
        if completed {
            self.progress_rx = None;
        }
        if self.view == View::Loading {
            ctx.request_repaint_after(std::time::Duration::from_millis(100));
        }
    }

    fn poll_llm(&mut self) {
        // Receive previous check result if any
        if let Some(rx) = self.llm_check_rx.as_mut() {
            if let Ok(result) = rx.try_recv() {
                self.llm_ok = result;
                self.llm_check_rx = None;
            }
        }

        // Kick off a new check periodically
        if self.llm_check_rx.is_none()
            && self.last_llm_check.elapsed() > std::time::Duration::from_secs(10)
        {
            let config = self.llm_config.clone();
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.llm_check_rx = Some(rx);
            self.last_llm_check = std::time::Instant::now();
            self.runtime.spawn(async move {
                let ok = check_llm_provider(&config).await;
                let _ = tx.send(ok);
            });
        }
    }

    fn navigate(&mut self, target: NaiveDate) {
        let persona_id = self.personas[self.selected_persona_idx].id.unwrap_or(1);
        if let Ok(Some(stored)) = self.storage.load(target, persona_id) {
            self.current_brief = Some(DisplayedBrief::from_stored(stored));
            self.topic_filter = "all".to_string();
            self.view = View::Results;
        }
    }

    pub fn remove_article(&mut self, url: &str) {
        if let Some(brief) = &mut self.current_brief {
            let orig_len = brief.articles.len();
            brief.articles.retain(|a| a.url != url);
            if brief.articles.len() < orig_len {
                brief.stats.articles_kept = brief.articles.len();
                let persona_id = self.personas[self.selected_persona_idx].id.unwrap_or(1);
                if let Err(e) = self.storage.save(
                    brief.date,
                    persona_id,
                    &brief.headline,
                    &brief.brief,
                    &brief.articles,
                    &brief.stats,
                    &brief.model,
                ) {
                    eprintln!("Failed to save brief after article removal: {}", e);
                }
                self.available_dates = self.storage.all_dates(persona_id).unwrap_or_default();
                if self.topic_filter != "all"
                    && !brief
                        .articles
                        .iter()
                        .any(|a| a.topic_tag.as_deref() == Some(&self.topic_filter))
                {
                    self.topic_filter = "all".to_string();
                }
            }
        }
    }
}


impl eframe::App for FeedbriefApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_progress(ctx);
        self.poll_llm();
        self.poll_publish();

        egui::CentralPanel::default()
            .frame(
                egui::Frame::none()
                    .fill(BG)
                    .inner_margin(egui::Margin::ZERO),
            )
            .show(ctx, |ui| {
                self.draw_masthead(ui);
                egui::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .show(ui, |ui| match self.view.clone() {
                        View::Idle => self.draw_idle(ui),
                        View::Loading => self.draw_loading(ui),
                        View::Results => self.draw_results(ui),
                        View::PersonasConfig(sub) => self.draw_personas_config(ui, &sub),
                        View::History => self.draw_history(ui),
                    });
            });

        if self.delete_confirm_target.is_some() {
            self.draw_delete_confirm_modal(ctx);
        }
        if self.delete_brief_confirm_target.is_some() {
            self.draw_delete_brief_confirm_modal(ctx);
        }
        if self.llm_settings_open {
            self.draw_llm_settings(ctx);
        }
    }
}

impl FeedbriefApp {
    fn selected_persona_id(&self) -> i64 {
        self.personas
            .get(self.selected_persona_idx)
            .and_then(|persona| persona.id)
            .unwrap_or(1)
    }

    fn select_persona_by_index(&mut self, idx: usize) {
        if self.personas.is_empty() {
            self.personas = vec![Persona::default()];
        }
        self.selected_persona_idx = idx.min(self.personas.len() - 1);
        if let Some(persona) = self.personas.get(self.selected_persona_idx) {
            self.publish_endpoint = persona.publish_endpoint.clone();
            self.publish_token = persona.publish_token.clone();
        }
        let persona_id = self.selected_persona_id();
        self.available_dates = self.storage.all_dates(persona_id).unwrap_or_default();
        let today = Local::now().date_naive();
        if let Ok(Some(stored)) = self.storage.load(today, persona_id) {
            self.current_brief = Some(DisplayedBrief::from_stored(stored));
            self.view = View::Results;
        } else {
            self.current_brief = None;
            self.view = View::Idle;
        }
    }

    fn reload_personas(&mut self, preserve_persona_id: Option<i64>) {
        let selected_id = preserve_persona_id.unwrap_or_else(|| self.selected_persona_id());
        self.personas = self
            .storage
            .list_personas()
            .unwrap_or_else(|_| vec![Persona::default()]);
        if self.personas.is_empty() {
            self.personas = vec![Persona::default()];
        }
        let idx = self
            .personas
            .iter()
            .position(|persona| persona.id == Some(selected_id))
            .unwrap_or(0);
        self.selected_persona_idx = idx.min(self.personas.len() - 1);
        if let Some(persona) = self.personas.get(self.selected_persona_idx) {
            self.publish_endpoint = persona.publish_endpoint.clone();
            self.publish_token = persona.publish_token.clone();
        }
        let persona_id = self.selected_persona_id();
        self.available_dates = self.storage.all_dates(persona_id).unwrap_or_default();
    }

    fn resolve_persona_path(input: &str, default: PathBuf) -> PathBuf {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            default
        } else {
            PathBuf::from(trimmed)
        }
    }

    fn export_personas_config(&mut self) {
        let path =
            Self::resolve_persona_path(&self.persona_export_path, Storage::personas_config_path());
        match self.storage.export_personas_to_path(&path) {
            Ok(()) => {
                self.persona_message_is_error = false;
                self.persona_message = format!("Exported personas to {}", path.display());
            }
            Err(err) => {
                self.persona_message_is_error = true;
                self.persona_message = format!("Failed to export personas: {}", err);
            }
        }
    }

    fn import_personas_config(&mut self) {
        let path =
            Self::resolve_persona_path(&self.persona_import_path, Storage::personas_config_path());
        let selected_id = self.selected_persona_id();
        match self.storage.import_personas_from_path(&path) {
            Ok(count) => {
                self.reload_personas(Some(selected_id));
                self.persona_message_is_error = false;
                self.persona_message =
                    format!("Imported {} personas from {}", count, path.display());
            }
            Err(err) => {
                self.persona_message_is_error = true;
                self.persona_message = format!("Failed to import personas: {}", err);
            }
        }
    }

    fn draw_masthead(&mut self, ui: &mut egui::Ui) {
        egui::Frame::none()
            .fill(BG)
            .inner_margin(egui::Margin {
                left: 36.0,
                right: 36.0,
                top: 22.0,
                bottom: 16.0,
            })
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(
                        RichText::new("▲")
                            .font(FontId::proportional(24.0))
                            .color(ACCENT),
                    );
                    ui.add_space(12.0);
                    ui.vertical(|ui| {
                        ui.label(
                            RichText::new("FEEDBRIEF")
                                .font(FontId::new(26.0, FontFamily::Name("serif-bold".into())))
                                .color(INK),
                        );
                        ui.horizontal(|ui| {
                            ui.label(
                                RichText::new("By")
                                    .font(FontId::new(9.5, FontFamily::Monospace))
                                    .color(INK_FAINT),
                            );
                            ui.add_space(4.0);
                            ui.hyperlink_to(
                                RichText::new("CTOFramework.com")
                                    .font(FontId::new(9.5, FontFamily::Monospace))
                                    .color(INK_FAINT),
                                "https://ctoframework.com",
                            );
                            ui.add_space(4.0);
                            ui.label(
                                RichText::new(format!("- v{}", env!("CARGO_PKG_VERSION")))
                                    .font(FontId::new(9.5, FontFamily::Monospace))
                                    .color(INK_FAINT),
                            );
                        });
                    });

                    ui.add_space(40.0);

                    // Persona Selector
                    ui.vertical(|ui| {
                        ui.label(
                            RichText::new("PERSONA")
                                .font(FontId::new(9.5, FontFamily::Monospace))
                                .color(INK_FAINT),
                        );
                        let selected_name = self.personas[self.selected_persona_idx].name.clone();
                        egui::ComboBox::from_id_salt("persona_select")
                            .selected_text(
                                RichText::new(selected_name)
                                    .font(FontId::new(
                                        16.0,
                                        FontFamily::Name("serif-italic".into()),
                                    ))
                                    .color(GOLD),
                            )
                            .show_ui(ui, |ui| {
                                for i in 0..self.personas.len() {
                                    if ui
                                        .selectable_label(
                                            self.selected_persona_idx == i,
                                            &self.personas[i].name,
                                        )
                                        .clicked()
                                    {
                                        self.select_persona_by_index(i);
                                    }
                                }
                                ui.separator();
                                if ui.button("⚙ Personas Config").clicked() {
                                    self.view = View::PersonasConfig(PersonasSubView::List);
                                    ui.close_menu();
                                }
                            });
                    });

                    // Navigation Tabs
                    ui.add_space(24.0);
                    ui.vertical(|ui| {
                        ui.label(
                            RichText::new("NAVIGATE")
                                .font(FontId::new(9.5, FontFamily::Monospace))
                                .color(INK_FAINT),
                        );
                        ui.horizontal(|ui| {
                            let is_history = self.view == View::History;
                            let is_personas = matches!(self.view, View::PersonasConfig(_));
                            let is_brief = !is_history && !is_personas;
                            if ui
                                .selectable_label(
                                    is_brief,
                                    RichText::new("📰 Brief")
                                        .font(FontId::new(13.0, FontFamily::Monospace))
                                        .color(if is_brief { GOLD } else { INK_DIM }),
                                )
                                .clicked()
                            {
                                self.view = if self.current_brief.is_some() {
                                    View::Results
                                } else {
                                    View::Idle
                                };
                            }
                            if ui
                                .selectable_label(
                                    is_history,
                                    RichText::new("📜 History")
                                        .font(FontId::new(13.0, FontFamily::Monospace))
                                        .color(if is_history { GOLD } else { INK_DIM }),
                                )
                                .clicked()
                            {
                                self.view = View::History;
                            }
                            if ui
                                .selectable_label(
                                    is_personas,
                                    RichText::new("⚙ Config")
                                        .font(FontId::new(13.0, FontFamily::Monospace))
                                        .color(if is_personas { GOLD } else { INK_DIM }),
                                )
                                .clicked()
                            {
                                self.view = View::PersonasConfig(PersonasSubView::List);
                            }
                        });
                    });

                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        let dot_color = if self.llm_ok { GREEN } else { ACCENT };
                        let status_text = if self.llm_ok {
                            format!("{} · {} READY ⚙", self.llm_config.provider_label(), self.llm_config.active_model().to_uppercase())
                        } else {
                            format!("{} OFFLINE / CONFIG ⚙", self.llm_config.provider_label())
                        };
                        if ui.button(
                            RichText::new(status_text)
                                .font(FontId::new(10.0, FontFamily::Monospace))
                                .color(if self.llm_ok { INK_DIM } else { ACCENT }),
                        ).clicked() {
                            self.llm_settings_open = true;
                        }
                        ui.add_space(6.0);
                        let (rect, _) =
                            ui.allocate_exact_size(Vec2::splat(8.0), egui::Sense::hover());
                        ui.painter().circle_filled(rect.center(), 4.0, dot_color);
                        ui.add_space(16.0);
                        ui.label(
                            RichText::new(Local::now().format("%A · %B %d, %Y").to_string())
                                .font(FontId::new(13.0, FontFamily::Name("serif-italic".into())))
                                .color(INK_DIM),
                        );
                    });
                });
            });
        // Bottom rule under the masthead
        draw_rule(ui);
    }

    fn start_edit_persona(&mut self, idx: usize) {
        if let Some(persona) = self.personas.get(idx) {
            self.editing_persona_idx = Some(idx);
            self.editing_persona = persona.clone();
            self.editing_feeds_text = persona
                .feeds
                .iter()
                .map(|f| f.url.clone())
                .collect::<Vec<_>>()
                .join("\n");
            self.persona_message.clear();
            self.view = View::PersonasConfig(PersonasSubView::Form { index: Some(idx) });
        }
    }

    fn start_add_persona(&mut self) {
        self.editing_persona_idx = None;
        self.editing_persona = Persona {
            id: None,
            name: "New Persona".into(),
            description: "".into(),
            feeds: vec![],
            publish_endpoint: "http://localhost:3000/api/news-digest".to_string(),
            publish_token: "YOUR_SECRET_KEY".to_string(),
        };
        self.editing_feeds_text = String::new();
        self.persona_message.clear();
        self.view = View::PersonasConfig(PersonasSubView::Form { index: None });
    }

    fn save_editing_persona(&mut self) {
        self.editing_persona.feeds = self
            .editing_feeds_text
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| crate::feeds::FeedSource {
                name: l.split('/').last().unwrap_or("Feed").to_string(),
                url: l.trim().to_string(),
                category: "General".to_string(),
            })
            .collect();

        match self.storage.save_persona(&self.editing_persona) {
            Ok(saved_id) => {
                self.editing_persona.id = Some(saved_id);
                self.reload_personas(Some(saved_id));
                self.persona_message_is_error = false;
                self.persona_message =
                    format!("Persona '{}' saved successfully.", self.editing_persona.name);
                self.view = View::PersonasConfig(PersonasSubView::List);
            }
            Err(err) => {
                self.persona_message_is_error = true;
                self.persona_message = format!("Failed to save persona: {}", err);
            }
        }
    }

    fn draw_delete_confirm_modal(&mut self, ctx: &egui::Context) {
        let mut open = true;
        let target_idx = match self.delete_confirm_target {
            Some(idx) => idx,
            None => return,
        };

        let persona_name = self
            .personas
            .get(target_idx)
            .map(|p| p.name.clone())
            .unwrap_or_default();

        egui::Window::new("Confirm Deletion")
            .open(&mut open)
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, egui::Vec2::ZERO)
            .default_width(420.0)
            .show(ctx, |ui| {
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    ui.label(RichText::new("⚠️").font(FontId::proportional(26.0)));
                    ui.add_space(8.0);
                    ui.vertical(|ui| {
                        ui.label(
                            RichText::new("Delete Persona?")
                                .font(FontId::new(16.0, FontFamily::Name("serif-bold".into())))
                                .color(INK),
                        );
                        ui.add_space(4.0);
                        ui.label(
                            RichText::new(format!(
                                "Are you sure you want to delete \"{}\"? This action cannot be undone.",
                                persona_name
                            ))
                            .font(FontId::new(13.0, FontFamily::Name("serif".into())))
                            .color(INK_DIM),
                        );
                    });
                });
                ui.add_space(16.0);
                ui.horizontal(|ui| {
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui
                            .add(
                                egui::Button::new(
                                    RichText::new("Delete Persona")
                                        .font(FontId::new(12.0, FontFamily::Monospace))
                                        .color(Color32::WHITE),
                                )
                                .fill(ACCENT),
                            )
                            .clicked()
                        {
                            if let Some(persona) = self.personas.get(target_idx) {
                                if let Some(id) = persona.id {
                                    let _ = self.storage.delete_persona(id);
                                }
                            }
                            self.personas.remove(target_idx);
                            if self.personas.is_empty() {
                                self.personas.push(Persona::default());
                            }
                            if self.selected_persona_idx >= self.personas.len() {
                                self.selected_persona_idx = 0;
                            }
                            let selected_id = self.selected_persona_id();
                            self.reload_personas(Some(selected_id));
                            self.persona_message_is_error = false;
                            self.persona_message = format!("Persona '{}' deleted.", persona_name);
                            self.delete_confirm_target = None;
                        }
                        ui.add_space(8.0);
                        if ui
                            .add(egui::Button::new(
                                RichText::new("Cancel")
                                    .font(FontId::new(12.0, FontFamily::Monospace))
                                    .color(INK_DIM),
                            ))
                            .clicked()
                        {
                            self.delete_confirm_target = None;
                        }
                    });
                });
            });

        if !open {
            self.delete_confirm_target = None;
        }
    }

    fn draw_personas_config(&mut self, ui: &mut egui::Ui, sub: &PersonasSubView) {
        match sub {
            PersonasSubView::List => self.draw_personas_list(ui),
            PersonasSubView::Form { index } => self.draw_persona_form(ui, *index),
        }
    }

    fn draw_personas_list(&mut self, ui: &mut egui::Ui) {
        ui.add_space(24.0);
        ui.vertical_centered(|ui| {
            ui.set_max_width(920.0);
            ui.vertical(|ui| {
                ui.horizontal(|ui| {
                    ui.vertical(|ui| {
                        ui.label(overline("CONFIGURATION"));
                        ui.add_space(4.0);
                        ui.label(
                            RichText::new("Personas & Feeds")
                                .font(FontId::new(32.0, FontFamily::Name("serif-bold".into())))
                                .color(INK),
                        );
                        ui.add_space(4.0);
                        ui.label(
                            RichText::new(
                                "Configure topic personas, news feeds, and publishing endpoints.",
                            )
                            .font(FontId::new(14.0, FontFamily::Name("serif".into())))
                            .color(INK_DIM),
                        );
                    });
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        let back_label = if self.current_brief.is_some() {
                            "← Back to Brief"
                        } else {
                            "← Back to Home"
                        };
                        if ui
                            .button(
                                RichText::new(back_label)
                                    .font(FontId::new(12.0, FontFamily::Monospace))
                                    .color(INK_DIM),
                            )
                            .clicked()
                        {
                            self.view = if self.current_brief.is_some() {
                                View::Results
                            } else {
                                View::Idle
                            };
                        }
                        ui.add_space(12.0);
                        if ui
                            .add(
                                egui::Button::new(
                                    RichText::new("+ Add New Persona")
                                        .font(FontId::new(12.0, FontFamily::Monospace))
                                        .color(BG),
                                )
                                .fill(GOLD),
                            )
                            .clicked()
                        {
                            self.start_add_persona();
                        }
                    });
                });

                ui.add_space(16.0);
                draw_rule(ui);
                ui.add_space(16.0);

                if !self.persona_message.is_empty() {
                    let color = if self.persona_message_is_error {
                        ACCENT
                    } else {
                        GREEN
                    };
                    egui::Frame::none()
                        .fill(BG_RAISED)
                        .stroke(Stroke::new(1.0, color))
                        .inner_margin(egui::Margin::same(10.0))
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                ui.label(
                                    RichText::new(&self.persona_message)
                                        .font(FontId::new(11.0, FontFamily::Monospace))
                                        .color(color),
                                );
                            });
                        });
                    ui.add_space(16.0);
                }

                // Table Header
                egui::Frame::none()
                    .fill(BG_RAISED)
                    .inner_margin(egui::Margin {
                        left: 16.0,
                        right: 16.0,
                        top: 8.0,
                        bottom: 8.0,
                    })
                    .show(ui, |ui| {
                        ui.horizontal(|ui| {
                            ui.allocate_ui_with_layout(
                                Vec2::new(45.0, 18.0),
                                egui::Layout::left_to_right(egui::Align::Center),
                                |ui| {
                                    ui.label(
                                        RichText::new("ID")
                                            .font(FontId::new(9.5, FontFamily::Monospace))
                                            .color(INK_FAINT),
                                    );
                                },
                            );
                            ui.allocate_ui_with_layout(
                                Vec2::new(280.0, 18.0),
                                egui::Layout::left_to_right(egui::Align::Center),
                                |ui| {
                                    ui.label(
                                        RichText::new("NAME & FOCUS DESCRIPTION")
                                            .font(FontId::new(9.5, FontFamily::Monospace))
                                            .color(INK_FAINT),
                                    );
                                },
                            );
                            ui.allocate_ui_with_layout(
                                Vec2::new(90.0, 18.0),
                                egui::Layout::left_to_right(egui::Align::Center),
                                |ui| {
                                    ui.label(
                                        RichText::new("FEEDS")
                                            .font(FontId::new(9.5, FontFamily::Monospace))
                                            .color(INK_FAINT),
                                    );
                                },
                            );
                            ui.allocate_ui_with_layout(
                                Vec2::new(200.0, 18.0),
                                egui::Layout::left_to_right(egui::Align::Center),
                                |ui| {
                                    ui.label(
                                        RichText::new("PUBLISH ENDPOINT")
                                            .font(FontId::new(9.5, FontFamily::Monospace))
                                            .color(INK_FAINT),
                                    );
                                },
                            );
                            ui.with_layout(
                                egui::Layout::right_to_left(egui::Align::Center),
                                |ui| {
                                    ui.label(
                                        RichText::new("ACTIONS")
                                            .font(FontId::new(9.5, FontFamily::Monospace))
                                            .color(INK_FAINT),
                                    );
                                },
                            );
                        });
                    });
                ui.add_space(4.0);

                let persona_count = self.personas.len();
                let mut edit_target = None;
                let mut delete_target = None;
                let mut select_target = None;

                for i in 0..persona_count {
                    let persona = &self.personas[i];
                    let is_selected = self.selected_persona_idx == i;
                    let is_default = persona.id == Some(1);

                    egui::Frame::none()
                        .fill(if is_selected { BG_PAPER } else { BG_RAISED })
                        .stroke(Stroke::new(1.0, if is_selected { GOLD } else { RULE }))
                        .inner_margin(egui::Margin {
                            left: 16.0,
                            right: 16.0,
                            top: 12.0,
                            bottom: 12.0,
                        })
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                // ID Column
                                ui.allocate_ui_with_layout(
                                    Vec2::new(45.0, 36.0),
                                    egui::Layout::left_to_right(egui::Align::Center),
                                    |ui| {
                                        let id_str = persona
                                            .id
                                            .map(|id| format!("#{}", id))
                                            .unwrap_or_else(|| "New".into());
                                        ui.label(
                                            RichText::new(id_str)
                                                .font(FontId::new(11.0, FontFamily::Monospace))
                                                .color(GOLD),
                                        );
                                    },
                                );

                                // Name & Description Column
                                ui.allocate_ui_with_layout(
                                    Vec2::new(280.0, 36.0),
                                    egui::Layout::left_to_right(egui::Align::Center),
                                    |ui| {
                                        ui.vertical(|ui| {
                                            ui.horizontal(|ui| {
                                                ui.label(
                                                    RichText::new(&persona.name)
                                                        .font(FontId::new(
                                                            15.0,
                                                            FontFamily::Name("serif-bold".into()),
                                                        ))
                                                        .color(INK),
                                                );
                                                if is_selected {
                                                    ui.label(
                                                        RichText::new("● Active")
                                                            .font(FontId::new(
                                                                9.0,
                                                                FontFamily::Monospace,
                                                            ))
                                                            .color(GREEN),
                                                    );
                                                }
                                            });
                                            let desc_preview = if persona.description.len() > 65 {
                                                format!("{}...", &persona.description[..65])
                                            } else {
                                                persona.description.clone()
                                            };
                                            ui.label(
                                                RichText::new(desc_preview)
                                                    .font(FontId::new(
                                                        11.0,
                                                        FontFamily::Name("serif".into()),
                                                    ))
                                                    .color(INK_DIM),
                                            );
                                        });
                                    },
                                );

                                // Feeds Column
                                ui.allocate_ui_with_layout(
                                    Vec2::new(90.0, 36.0),
                                    egui::Layout::left_to_right(egui::Align::Center),
                                    |ui| {
                                        ui.label(
                                            RichText::new(format!("{} feeds", persona.feeds.len()))
                                                .font(FontId::new(11.0, FontFamily::Monospace))
                                                .color(INK_DIM),
                                        );
                                    },
                                );

                                // Endpoint Column
                                ui.allocate_ui_with_layout(
                                    Vec2::new(200.0, 36.0),
                                    egui::Layout::left_to_right(egui::Align::Center),
                                    |ui| {
                                        let ep_short = if persona.publish_endpoint.len() > 25 {
                                            format!("{}...", &persona.publish_endpoint[..25])
                                        } else {
                                            persona.publish_endpoint.clone()
                                        };
                                        ui.label(
                                            RichText::new(ep_short)
                                                .font(FontId::new(10.0, FontFamily::Monospace))
                                                .color(INK_FAINT),
                                        );
                                    },
                                );

                                // Actions Column
                                ui.with_layout(
                                    egui::Layout::right_to_left(egui::Align::Center),
                                    |ui| {
                                        let can_delete = !is_default && persona_count > 1;
                                        let delete_btn = egui::Button::new(
                                            RichText::new("🗑 Delete")
                                                .font(FontId::new(11.0, FontFamily::Monospace))
                                                .color(if can_delete { ACCENT } else { INK_FAINT }),
                                        );
                                        if ui.add_enabled(can_delete, delete_btn).clicked() {
                                            delete_target = Some(i);
                                        }

                                        ui.add_space(6.0);

                                        if ui
                                            .button(
                                                RichText::new("✏ Edit")
                                                    .font(FontId::new(11.0, FontFamily::Monospace))
                                                    .color(GOLD),
                                            )
                                            .clicked()
                                        {
                                            edit_target = Some(i);
                                        }

                                        if !is_selected {
                                            ui.add_space(6.0);
                                            if ui
                                                .button(
                                                    RichText::new("Use Persona")
                                                        .font(FontId::new(
                                                            10.0,
                                                            FontFamily::Monospace,
                                                        ))
                                                        .color(INK_DIM),
                                                )
                                                .clicked()
                                            {
                                                select_target = Some(i);
                                            }
                                        }
                                    },
                                );
                            });
                        });
                    ui.add_space(6.0);
                }

                if let Some(i) = edit_target {
                    self.start_edit_persona(i);
                }
                if let Some(i) = delete_target {
                    self.delete_confirm_target = Some(i);
                }
                if let Some(i) = select_target {
                    self.select_persona_by_index(i);
                }

                ui.add_space(20.0);

                // JSON Backup / Export & Import
                egui::CollapsingHeader::new(
                    RichText::new("⚙ Persona Config Backup & Restore (JSON Export / Import)")
                        .font(FontId::new(11.0, FontFamily::Monospace))
                        .color(INK_DIM),
                )
                .show(ui, |ui| {
                    ui.add_space(8.0);
                    ui.group(|ui| {
                        ui.horizontal(|ui| {
                            ui.vertical(|ui| {
                                ui.label(
                                    RichText::new("EXPORT JSON PATH")
                                        .font(FontId::new(9.0, FontFamily::Monospace))
                                        .color(INK_FAINT),
                                );
                                ui.text_edit_singleline(&mut self.persona_export_path);
                            });
                            if ui.button("Export JSON").clicked() {
                                self.export_personas_config();
                            }
                        });
                        ui.add_space(6.0);
                        ui.horizontal(|ui| {
                            ui.vertical(|ui| {
                                ui.label(
                                    RichText::new("IMPORT JSON PATH")
                                        .font(FontId::new(9.0, FontFamily::Monospace))
                                        .color(INK_FAINT),
                                );
                                ui.text_edit_singleline(&mut self.persona_import_path);
                            });
                            if ui.button("Import JSON").clicked() {
                                self.import_personas_config();
                            }
                        });
                    });
                });

                ui.add_space(40.0);
            });
        });
    }

    fn draw_persona_form(&mut self, ui: &mut egui::Ui, index: Option<usize>) {
        ui.add_space(24.0);
        ui.vertical_centered(|ui| {
            ui.set_max_width(780.0);
            ui.vertical(|ui| {
                let is_new = index.is_none();
                let title_text = if is_new {
                    "Create New Persona".to_string()
                } else {
                    format!("Edit Persona: {}", self.editing_persona.name)
                };

                ui.horizontal(|ui| {
                    ui.vertical(|ui| {
                        ui.label(overline(if is_new { "NEW PERSONA" } else { "EDIT PERSONA" }));
                        ui.add_space(4.0);
                        ui.label(
                            RichText::new(title_text)
                                .font(FontId::new(28.0, FontFamily::Name("serif-bold".into())))
                                .color(INK),
                        );
                    });
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui
                            .button(
                                RichText::new("← Back to Personas List")
                                    .font(FontId::new(12.0, FontFamily::Monospace))
                                    .color(INK_DIM),
                            )
                            .clicked()
                        {
                            self.persona_message.clear();
                            self.view = View::PersonasConfig(PersonasSubView::List);
                        }
                    });
                });

                ui.add_space(16.0);
                draw_rule(ui);
                ui.add_space(16.0);

                if !self.persona_message.is_empty() {
                    let color = if self.persona_message_is_error {
                        ACCENT
                    } else {
                        GREEN
                    };
                    ui.label(
                        RichText::new(&self.persona_message)
                            .font(FontId::new(11.0, FontFamily::Monospace))
                            .color(color),
                    );
                    ui.add_space(10.0);
                }

                // Form Container
                egui::Frame::none()
                    .fill(BG_RAISED)
                    .stroke(Stroke::new(1.0, RULE))
                    .inner_margin(egui::Margin::same(20.0))
                    .show(ui, |ui| {
                        ui.vertical(|ui| {
                            // PERSONA NAME
                            ui.label(
                                RichText::new("PERSONA NAME")
                                    .font(FontId::new(9.5, FontFamily::Monospace))
                                    .color(INK_FAINT),
                            );
                            ui.add_space(4.0);
                            ui.add(
                                egui::TextEdit::singleline(&mut self.editing_persona.name)
                                    .desired_width(f32::INFINITY)
                                    .font(FontId::new(14.0, FontFamily::Name("serif".into()))),
                            );

                            ui.add_space(16.0);

                            // DESCRIPTION
                            ui.label(
                                RichText::new("TOPIC & FOCUS DESCRIPTION")
                                    .font(FontId::new(9.5, FontFamily::Monospace))
                                    .color(INK_FAINT),
                            );
                            ui.add_space(2.0);
                            ui.label(
                                RichText::new(
                                    "Describe key topics, technologies, or domains for summary distillation.",
                                )
                                .font(FontId::new(11.0, FontFamily::Name("serif-italic".into())))
                                .color(INK_FAINT),
                            );
                            ui.add_space(4.0);
                            ui.add(
                                egui::TextEdit::multiline(&mut self.editing_persona.description)
                                    .desired_rows(3)
                                    .desired_width(f32::INFINITY)
                                    .font(FontId::new(13.0, FontFamily::Name("serif".into()))),
                            );

                            ui.add_space(16.0);

                            // FEEDS
                            let feed_count = self
                                .editing_feeds_text
                                .lines()
                                .filter(|l| !l.trim().is_empty())
                                .count();
                            ui.horizontal(|ui| {
                                ui.label(
                                    RichText::new("FEED SOURCES (RSS/ATOM URLS - ONE PER LINE)")
                                        .font(FontId::new(9.5, FontFamily::Monospace))
                                        .color(INK_FAINT),
                                );
                                ui.with_layout(
                                    egui::Layout::right_to_left(egui::Align::Center),
                                    |ui| {
                                        ui.label(
                                            RichText::new(format!("{} feeds", feed_count))
                                                .font(FontId::new(9.5, FontFamily::Monospace))
                                                .color(GOLD),
                                        );
                                    },
                                );
                            });
                            ui.add_space(4.0);
                            ui.add(
                                egui::TextEdit::multiline(&mut self.editing_feeds_text)
                                    .desired_rows(8)
                                    .desired_width(f32::INFINITY)
                                    .font(FontId::new(12.0, FontFamily::Monospace)),
                            );

                            ui.add_space(16.0);

                            // PUBLISHING ENDPOINT & TOKEN
                            ui.label(
                                RichText::new("PUBLISHING CONFIGURATION")
                                    .font(FontId::new(9.5, FontFamily::Monospace))
                                    .color(INK_FAINT),
                            );
                            ui.add_space(6.0);
                            ui.horizontal(|ui| {
                                ui.vertical(|ui| {
                                    ui.label(
                                        RichText::new("ENDPOINT URL")
                                            .font(FontId::new(9.0, FontFamily::Monospace))
                                            .color(INK_FAINT),
                                    );
                                    ui.add_space(2.0);
                                    ui.add(
                                        egui::TextEdit::singleline(
                                            &mut self.editing_persona.publish_endpoint,
                                        )
                                        .desired_width(340.0)
                                        .font(FontId::new(12.0, FontFamily::Monospace)),
                                    );
                                });
                                ui.add_space(16.0);
                                ui.vertical(|ui| {
                                    ui.label(
                                        RichText::new("BEARER TOKEN")
                                            .font(FontId::new(9.0, FontFamily::Monospace))
                                            .color(INK_FAINT),
                                    );
                                    ui.add_space(2.0);
                                    ui.add(
                                        egui::TextEdit::singleline(
                                            &mut self.editing_persona.publish_token,
                                        )
                                        .desired_width(340.0)
                                        .password(true)
                                        .font(FontId::new(12.0, FontFamily::Monospace)),
                                    );
                                });
                            });

                            ui.add_space(24.0);
                            draw_rule(ui);
                            ui.add_space(16.0);

                            // Action buttons
                            ui.horizontal(|ui| {
                                if ui
                                    .add(
                                        egui::Button::new(
                                            RichText::new("💾 Save Persona")
                                                .font(FontId::new(12.0, FontFamily::Monospace))
                                                .color(BG),
                                        )
                                        .fill(GOLD),
                                    )
                                    .clicked()
                                {
                                    self.save_editing_persona();
                                }
                                ui.add_space(12.0);
                                if ui
                                    .button(
                                        RichText::new("Cancel")
                                            .font(FontId::new(12.0, FontFamily::Monospace))
                                            .color(INK_DIM),
                                    )
                                    .clicked()
                                {
                                    self.persona_message.clear();
                                    self.view = View::PersonasConfig(PersonasSubView::List);
                                }
                            });
                        });
                    });

                ui.add_space(40.0);
            });
        });
    }

    fn draw_llm_settings(&mut self, ctx: &egui::Context) {
        let mut open = self.llm_settings_open;
        let mut config_changed = false;
        let mut should_close = false;
        egui::Window::new("LLM & Model Settings")
            .open(&mut open)
            .collapsible(false)
            .resizable(true)
            .default_width(540.0)
            .show(ctx, |ui| {
                ui.label("Configure local models via Ollama or cloud LLMs via OpenRouter.");
                ui.add_space(12.0);

                ui.horizontal(|ui| {
                    ui.label(
                        RichText::new("PROVIDER")
                            .font(FontId::new(10.0, FontFamily::Monospace))
                            .color(INK_FAINT),
                    );
                    ui.add_space(10.0);
                    if ui
                        .selectable_label(
                            self.llm_config.provider == ProviderType::Ollama,
                            "Ollama (Local)",
                        )
                        .clicked()
                    {
                        if self.llm_config.provider != ProviderType::Ollama {
                            self.llm_config.provider = ProviderType::Ollama;
                            config_changed = true;
                        }
                    }
                    if ui
                        .selectable_label(
                            self.llm_config.provider == ProviderType::OpenRouter,
                            "OpenRouter (Cloud)",
                        )
                        .clicked()
                    {
                        if self.llm_config.provider != ProviderType::OpenRouter {
                            self.llm_config.provider = ProviderType::OpenRouter;
                            config_changed = true;
                        }
                    }
                });

                ui.add_space(12.0);
                draw_rule(ui);
                ui.add_space(12.0);

                match self.llm_config.provider {
                    ProviderType::Ollama => {
                        ui.label(
                            RichText::new("OLLAMA CONFIGURATION")
                                .font(FontId::new(11.0, FontFamily::Monospace))
                                .color(GOLD),
                        );
                        ui.add_space(8.0);

                        ui.label("Base Endpoint URL:");
                        if ui
                            .add(
                                egui::TextEdit::singleline(&mut self.llm_config.ollama_url)
                                    .hint_text("http://localhost:11434"),
                            )
                            .changed()
                        {
                            config_changed = true;
                        }

                        ui.add_space(8.0);
                        ui.label("Model Preset:");
                        let presets = [
                            "llama3.1:8b",
                            "qwen2.5:7b",
                            "qwen2.5:14b",
                            "mistral:7b",
                            "gemma2:9b",
                            "deepseek-r1:8b",
                            "phi4:14b",
                        ];
                        egui::ComboBox::from_id_salt("ollama_model_preset")
                            .selected_text(&self.llm_config.ollama_model)
                            .show_ui(ui, |ui| {
                                for p in presets {
                                    if ui
                                        .selectable_label(self.llm_config.ollama_model == p, p)
                                        .clicked()
                                    {
                                        self.llm_config.ollama_model = p.to_string();
                                        config_changed = true;
                                    }
                                }
                            });
                        ui.add_space(4.0);
                        ui.horizontal(|ui| {
                            ui.label(
                                RichText::new("Custom Model:")
                                    .font(FontId::new(11.0, FontFamily::Monospace))
                                    .color(INK_FAINT),
                            );
                            if ui
                                .add(egui::TextEdit::singleline(
                                    &mut self.llm_config.ollama_model,
                                ))
                                .changed()
                            {
                                config_changed = true;
                            }
                        });

                        ui.add_space(12.0);
                        let status_str = if self.llm_ok {
                            format!(
                                "✔ Connected to Ollama — model '{}' ready",
                                self.llm_config.ollama_model
                            )
                        } else {
                            "✖ Cannot connect to Ollama server or model not found. Ensure `ollama serve` is running"
                                .to_string()
                        };
                        let color = if self.llm_ok { GREEN } else { ACCENT };
                        ui.label(RichText::new(status_str).color(color));
                    }
                    ProviderType::OpenRouter => {
                        ui.label(
                            RichText::new("OPENROUTER CONFIGURATION")
                                .font(FontId::new(11.0, FontFamily::Monospace))
                                .color(GOLD),
                        );
                        ui.add_space(8.0);

                        ui.label("API Key:");
                        if ui
                            .add(
                                egui::TextEdit::singleline(
                                    &mut self.llm_config.openrouter_api_key,
                                )
                                .password(true)
                                .hint_text("sk-or-v1-..."),
                            )
                            .changed()
                        {
                            config_changed = true;
                        }

                        ui.add_space(8.0);
                        ui.label("Model Preset:");
                        let presets = [
                            "openai/gpt-4o-mini",
                            "anthropic/claude-3.5-sonnet",
                            "meta-llama/llama-3.1-8b-instruct",
                            "meta-llama/llama-3.3-70b-instruct",
                            "google/gemini-2.0-flash-001",
                            "deepseek/deepseek-r1",
                            "qwen/qwen-2.5-72b-instruct",
                            "mistralai/mistral-large",
                        ];
                        egui::ComboBox::from_id_salt("openrouter_model_preset")
                            .selected_text(&self.llm_config.openrouter_model)
                            .show_ui(ui, |ui| {
                                for p in presets {
                                    if ui
                                        .selectable_label(self.llm_config.openrouter_model == p, p)
                                        .clicked()
                                    {
                                        self.llm_config.openrouter_model = p.to_string();
                                        config_changed = true;
                                    }
                                }
                            });
                        ui.add_space(4.0);
                        ui.horizontal(|ui| {
                            ui.label(
                                RichText::new("Custom Model:")
                                    .font(FontId::new(11.0, FontFamily::Monospace))
                                    .color(INK_FAINT),
                            );
                            if ui
                                .add(egui::TextEdit::singleline(
                                    &mut self.llm_config.openrouter_model,
                                ))
                                .changed()
                            {
                                config_changed = true;
                            }
                        });

                        ui.add_space(8.0);
                        ui.label("API Base URL:");
                        if ui
                            .add(
                                egui::TextEdit::singleline(&mut self.llm_config.openrouter_url)
                                    .hint_text("https://openrouter.ai/api/v1"),
                            )
                            .changed()
                        {
                            config_changed = true;
                        }

                        ui.add_space(12.0);
                        let status_str = if self.llm_ok {
                            format!(
                                "✔ Connected to OpenRouter — Key verified & model '{}' set",
                                self.llm_config.openrouter_model
                            )
                        } else {
                            "✖ API Key missing or unverified".to_string()
                        };
                        let color = if self.llm_ok { GREEN } else { ACCENT };
                        ui.label(RichText::new(status_str).color(color));
                    }
                }

                ui.add_space(16.0);
                draw_rule(ui);
                ui.add_space(10.0);
                ui.horizontal(|ui| {
                    if ui.button("Close").clicked() {
                        should_close = true;
                    }
                });
            });

        if should_close {
            open = false;
        }
        self.llm_settings_open = open;
        if config_changed {
            let _ = self.storage.save_llm_config(&self.llm_config);
            self.last_llm_check = std::time::Instant::now() - std::time::Duration::from_secs(60);
        }
    }

    fn draw_idle(&mut self, ui: &mut egui::Ui) {
        ui.add_space(60.0);
        ui.vertical_centered(|ui| {
            ui.set_max_width(720.0);
            ui.vertical(|ui| {
                ui.label(overline("TODAY'S BRIEFING"));
                ui.add_space(14.0);
                ui.label(RichText::new("What matters")
                    .font(FontId::new(72.0, FontFamily::Name("serif".into())))
                    .color(INK));
                ui.label(RichText::new("in your world.")
                    .font(FontId::new(72.0, FontFamily::Name("serif-italic".into())))
                    .color(GOLD));
                ui.add_space(20.0);
                ui.label(RichText::new("A synthesis of AI, research, startups, hardware, security and emerging tech — distilled by configurable intelligence.")
                    .font(FontId::new(17.0, FontFamily::Name("serif".into())))
                    .color(INK_DIM));

                ui.add_space(36.0);
                draw_rule(ui);
                ui.add_space(14.0);

                ui.horizontal(|ui| {
                    // PROVIDER
                    ui.vertical(|ui| {
                        ui.label(
                            RichText::new("PROVIDER")
                                .font(FontId::new(9.5, FontFamily::Monospace))
                                .color(INK_FAINT),
                        );
                        let prev_provider = self.llm_config.provider.clone();
                        egui::ComboBox::from_id_salt("idle_provider_select")
                            .selected_text(
                                RichText::new(self.llm_config.provider.to_string())
                                    .font(FontId::new(15.0, FontFamily::Name("serif-italic".into())))
                                    .color(GOLD),
                            )
                            .show_ui(ui, |ui| {
                                if ui.selectable_label(self.llm_config.provider == ProviderType::Ollama, "Ollama").clicked() {
                                    self.llm_config.provider = ProviderType::Ollama;
                                }
                                if ui.selectable_label(self.llm_config.provider == ProviderType::OpenRouter, "OpenRouter").clicked() {
                                    self.llm_config.provider = ProviderType::OpenRouter;
                                }
                            });
                        if self.llm_config.provider != prev_provider {
                            let _ = self.storage.save_llm_config(&self.llm_config);
                            self.last_llm_check = std::time::Instant::now() - std::time::Duration::from_secs(60);
                        }
                    });
                    ui.add_space(20.0);

                    // MODEL
                    ui.vertical(|ui| {
                        ui.label(
                            RichText::new("MODEL")
                                .font(FontId::new(9.5, FontFamily::Monospace))
                                .color(INK_FAINT),
                        );
                        let active_model = self.llm_config.active_model().to_string();
                        let mut selected_model = active_model.clone();
                        let options: Vec<&str> = match self.llm_config.provider {
                            ProviderType::Ollama => vec!["llama3.1:8b", "qwen2.5:7b", "qwen2.5:14b", "mistral:7b", "gemma2:9b", "deepseek-r1:8b"],
                            ProviderType::OpenRouter => vec![
                                "openai/gpt-4o-mini",
                                "anthropic/claude-3.5-sonnet",
                                "meta-llama/llama-3.1-8b-instruct",
                                "meta-llama/llama-3.3-70b-instruct",
                                "google/gemini-2.0-flash-001",
                                "deepseek/deepseek-r1",
                            ],
                        };
                        egui::ComboBox::from_id_salt("idle_model_select")
                            .selected_text(
                                RichText::new(&active_model)
                                    .font(FontId::new(15.0, FontFamily::Name("serif-italic".into())))
                                    .color(GOLD),
                            )
                            .show_ui(ui, |ui| {
                                for opt in options {
                                    if ui.selectable_label(selected_model == opt, opt).clicked() {
                                        selected_model = opt.to_string();
                                    }
                                }
                            });
                        if selected_model != active_model {
                            match self.llm_config.provider {
                                ProviderType::Ollama => self.llm_config.ollama_model = selected_model,
                                ProviderType::OpenRouter => self.llm_config.openrouter_model = selected_model,
                            }
                            let _ = self.storage.save_llm_config(&self.llm_config);
                            self.last_llm_check = std::time::Instant::now() - std::time::Duration::from_secs(60);
                        }
                    });
                    ui.add_space(10.0);

                    ui.vertical(|ui| {
                        ui.label(RichText::new(" ").font(FontId::new(9.5, FontFamily::Monospace)));
                        if ui.button("⚙ Config").clicked() {
                            self.llm_settings_open = true;
                        }
                    });

                    ui.add_space(24.0);

                    let mut hours_str = self.hours.to_string();
                    control_select(ui, "WINDOW (HRS)", &mut hours_str, &["12", "24", "48", "72"]);
                    self.hours = hours_str.parse().unwrap_or(24);
                    ui.add_space(24.0);

                    let mut top_str = self.top_n.to_string();
                    control_select(ui, "DEPTH (TOP N)", &mut top_str, &["10", "15", "20", "30"]);
                    self.top_n = top_str.parse().unwrap_or(20);
                });

                ui.add_space(14.0);
                draw_rule(ui);
                ui.add_space(36.0);

                if fetch_button(ui, "FETCH WHAT MATTERS TODAY  →").clicked() {
                    self.start_fetch();
                }
                ui.add_space(16.0);
                let helper_text = match self.llm_config.provider {
                    ProviderType::Ollama => "⌘ Make sure Ollama is running locally with the chosen model pulled.",
                    ProviderType::OpenRouter => "⌘ OpenRouter cloud LLM enabled. Configure API key via ⚙ Config if needed.",
                };
                ui.label(RichText::new(helper_text)
                    .font(FontId::new(11.0, FontFamily::Monospace)).color(INK_FAINT));

                if !self.available_dates.is_empty() {
                    ui.add_space(40.0);
                    ui.horizontal(|ui| {
                        ui.label(overline("HISTORY"));
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui
                                .button(
                                    RichText::new("View Full History →")
                                        .font(FontId::new(11.0, FontFamily::Monospace))
                                        .color(GOLD),
                                )
                                .clicked()
                            {
                                self.view = View::History;
                            }
                        });
                    });
                    ui.add_space(10.0);
                    self.draw_history_grid(ui);
                }
            });
        });
        ui.add_space(60.0);
    }

    fn draw_history_grid(&mut self, ui: &mut egui::Ui) {
        let persona_id = self.selected_persona_id();
        let summaries = self.storage.list_brief_summaries(persona_id).unwrap_or_default();

        if summaries.is_empty() {
            egui::Frame::none()
                .fill(BG_RAISED)
                .stroke(Stroke::new(1.0, RULE))
                .inner_margin(egui::Margin::same(24.0))
                .show(ui, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.label(
                            RichText::new("No archived briefings for this persona.")
                                .font(FontId::new(14.0, FontFamily::Name("serif-italic".into())))
                                .color(INK_FAINT),
                        );
                    });
                });
            return;
        }

        egui::Frame::none()
            .fill(BG_RAISED)
            .stroke(Stroke::new(1.0, RULE))
            .inner_margin(egui::Margin::same(16.0))
            .show(ui, |ui| {
                // Header Bar
                ui.horizontal(|ui| {
                    ui.add_space(8.0);
                    ui.allocate_ui_with_layout(
                        Vec2::new(110.0, 20.0),
                        egui::Layout::left_to_right(egui::Align::Center),
                        |ui| {
                            ui.label(overline("DATE"));
                        },
                    );
                    ui.allocate_ui_with_layout(
                        Vec2::new((ui.available_width() - 280.0).max(120.0), 20.0),
                        egui::Layout::left_to_right(egui::Align::Center),
                        |ui| {
                            ui.label(overline("HEADLINE"));
                        },
                    );
                    ui.allocate_ui_with_layout(
                        Vec2::new(110.0, 20.0),
                        egui::Layout::left_to_right(egui::Align::Center),
                        |ui| {
                            ui.label(overline("ARTICLES"));
                        },
                    );
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.label(overline("ACTIONS"));
                    });
                });

                ui.add_space(6.0);
                draw_rule(ui);
                ui.add_space(8.0);

                let mut navigate_target = None;
                let mut delete_target = None;

                for (idx, item) in summaries.iter().enumerate() {
                    if idx > 0 {
                        ui.add_space(6.0);
                    }
                    let date_str = item.date.format("%b %d, %Y").to_string();

                    egui::Frame::none()
                        .fill(BG_PAPER)
                        .stroke(Stroke::new(1.0, RULE))
                        .inner_margin(egui::Margin {
                            left: 12.0,
                            right: 12.0,
                            top: 10.0,
                            bottom: 10.0,
                        })
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                // Date Column (~110px)
                                ui.allocate_ui_with_layout(
                                    Vec2::new(110.0, 24.0),
                                    egui::Layout::left_to_right(egui::Align::Center),
                                    |ui| {
                                        ui.label(
                                            RichText::new(&date_str)
                                                .font(FontId::new(13.0, FontFamily::Monospace))
                                                .color(GOLD),
                                        );
                                    },
                                );

                                // Headline Column
                                let headline_w = (ui.available_width() - 270.0).max(120.0);
                                ui.allocate_ui_with_layout(
                                    Vec2::new(headline_w, 24.0),
                                    egui::Layout::left_to_right(egui::Align::Center),
                                    |ui| {
                                        ui.label(
                                            RichText::new(&item.headline)
                                                .font(FontId::new(14.0, FontFamily::Name("serif-bold".into())))
                                                .color(INK),
                                        );
                                    },
                                );

                                // Articles Column
                                ui.allocate_ui_with_layout(
                                    Vec2::new(100.0, 24.0),
                                    egui::Layout::left_to_right(egui::Align::Center),
                                    |ui| {
                                        ui.label(
                                            RichText::new(format!("{} articles", item.articles_kept))
                                                .font(FontId::new(12.0, FontFamily::Monospace))
                                                .color(INK_FAINT),
                                        );
                                    },
                                );

                                // Actions Column
                                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                    if ui
                                        .add(
                                            egui::Button::new(
                                                RichText::new("🗑 Delete")
                                                    .font(FontId::new(11.0, FontFamily::Monospace))
                                                    .color(ACCENT),
                                            )
                                            .fill(BG_RAISED)
                                            .stroke(Stroke::new(1.0, RULE)),
                                        )
                                        .clicked()
                                    {
                                        delete_target = Some(item.date);
                                    }

                                    ui.add_space(8.0);

                                    if ui
                                        .add(
                                            egui::Button::new(
                                                RichText::new("👁 View")
                                                    .font(FontId::new(11.0, FontFamily::Monospace))
                                                    .color(INK),
                                            )
                                            .fill(BG_RAISED)
                                            .stroke(Stroke::new(1.0, RULE)),
                                        )
                                        .clicked()
                                    {
                                        navigate_target = Some(item.date);
                                    }
                                });
                            });
                        });
                }

                if let Some(target) = navigate_target {
                    self.navigate(target);
                }
                if let Some(target) = delete_target {
                    self.delete_brief_confirm_target = Some(target);
                }
            });
    }

    fn draw_history(&mut self, ui: &mut egui::Ui) {
        ui.add_space(24.0);
        ui.vertical_centered(|ui| {
            ui.set_max_width(920.0);
            ui.vertical(|ui| {
                ui.horizontal(|ui| {
                    ui.vertical(|ui| {
                        ui.label(overline("ARCHIVE"));
                        ui.add_space(4.0);
                        ui.label(
                            RichText::new("Briefing History")
                                .font(FontId::new(32.0, FontFamily::Name("serif-bold".into())))
                                .color(INK),
                        );
                        ui.add_space(4.0);
                        let persona_name = self.personas[self.selected_persona_idx].name.clone();
                        ui.label(
                            RichText::new(format!(
                                "Browse and manage stored daily intelligence briefings for persona: \"{}\".",
                                persona_name
                            ))
                            .font(FontId::new(14.0, FontFamily::Name("serif".into())))
                            .color(INK_DIM),
                        );
                    });
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        let back_label = if self.current_brief.is_some() {
                            "← Back to Brief"
                        } else {
                            "← Back to Home"
                        };
                        if ui
                            .button(
                                RichText::new(back_label)
                                    .font(FontId::new(12.0, FontFamily::Monospace))
                                    .color(INK_DIM),
                            )
                            .clicked()
                        {
                            self.view = if self.current_brief.is_some() {
                                View::Results
                            } else {
                                View::Idle
                            };
                        }
                    });
                });

                ui.add_space(16.0);
                draw_rule(ui);
                ui.add_space(16.0);

                self.draw_history_grid(ui);
            });
        });
        ui.add_space(60.0);
    }

    fn draw_delete_brief_confirm_modal(&mut self, ctx: &egui::Context) {
        let mut open = true;
        let target_date = match self.delete_brief_confirm_target {
            Some(d) => d,
            None => return,
        };

        let date_str = target_date.format("%B %d, %Y").to_string();

        egui::Window::new("Confirm Deletion")
            .open(&mut open)
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, egui::Vec2::ZERO)
            .default_width(420.0)
            .show(ctx, |ui| {
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    ui.label(RichText::new("⚠️").font(FontId::proportional(26.0)));
                    ui.add_space(8.0);
                    ui.vertical(|ui| {
                        ui.label(
                            RichText::new("Delete Briefing?")
                                .font(FontId::new(16.0, FontFamily::Name("serif-bold".into())))
                                .color(INK),
                        );
                        ui.add_space(4.0);
                        ui.label(
                            RichText::new(format!(
                                "Are you sure you want to delete the briefing for {}? This action cannot be undone.",
                                date_str
                            ))
                            .font(FontId::new(13.0, FontFamily::Name("serif".into())))
                            .color(INK_DIM),
                        );
                    });
                });
                ui.add_space(16.0);
                ui.horizontal(|ui| {
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui
                            .add(
                                egui::Button::new(
                                    RichText::new("Delete Briefing")
                                        .font(FontId::new(12.0, FontFamily::Monospace))
                                        .color(Color32::WHITE),
                                )
                                .fill(ACCENT),
                            )
                            .clicked()
                        {
                            let persona_id = self.selected_persona_id();
                            let _ = self.storage.delete_brief(target_date, persona_id);
                            self.available_dates =
                                self.storage.all_dates(persona_id).unwrap_or_default();
                            if self.current_brief.as_ref().map(|b| b.date) == Some(target_date) {
                                self.current_brief = None;
                                if self.view == View::Results {
                                    self.view = View::Idle;
                                }
                            }
                            self.delete_brief_confirm_target = None;
                        }
                        ui.add_space(8.0);
                        if ui
                            .button(
                                RichText::new("Cancel")
                                    .font(FontId::new(12.0, FontFamily::Monospace))
                                    .color(INK_DIM),
                            )
                            .clicked()
                        {
                            self.delete_brief_confirm_target = None;
                        }
                    });
                });
            });

        if !open {
            self.delete_brief_confirm_target = None;
        }
    }

    fn draw_loading(&mut self, ui: &mut egui::Ui) {
        ui.add_space(40.0);
        ui.vertical_centered(|ui| {
            ui.set_max_width(820.0);
            ui.vertical(|ui| {
                ui.label(overline(&self.current_stage));
                ui.add_space(10.0);
                ui.label(
                    RichText::new(&self.current_message)
                        .font(FontId::new(22.0, FontFamily::Name("serif-italic".into())))
                        .color(INK),
                );
                ui.add_space(28.0);

                let track_h = 3.0;
                let (rect, _) = ui.allocate_exact_size(
                    Vec2::new(ui.available_width(), track_h),
                    egui::Sense::hover(),
                );
                ui.painter().rect_filled(rect, 0.0, RULE);
                let fill_w = rect.width() * (self.current_percent as f32 / 100.0);
                let fill_rect = egui::Rect::from_min_size(rect.min, Vec2::new(fill_w, track_h));
                ui.painter().rect_filled(fill_rect, 0.0, ACCENT);

                ui.add_space(8.0);
                ui.label(
                    RichText::new(format!("{}%", self.current_percent))
                        .font(FontId::new(11.0, FontFamily::Monospace))
                        .color(INK_FAINT),
                );

                ui.add_space(36.0);
                draw_rule(ui);
                ui.add_space(20.0);
                ui.label(overline("LIVE LOG"));
                ui.add_space(8.0);

                let log = self.progress_log.lock().unwrap().clone();
                let visible: Vec<&String> = log.iter().rev().take(15).collect();
                for line in visible.iter().rev() {
                    ui.label(
                        RichText::new(line.as_str())
                            .font(FontId::new(11.5, FontFamily::Monospace))
                            .color(INK_DIM),
                    );
                }
            });
        });
        ui.add_space(60.0);
    }

    fn poll_publish(&mut self) {
        if let Some(rx) = self.publish_rx.as_mut() {
            if let Ok(res) = rx.try_recv() {
                self.publish_in_progress = false;
                match res {
                    Ok(msg) => self.publish_result_msg = Some((false, msg)),
                    Err(err) => self.publish_result_msg = Some((true, err)),
                }
                self.publish_rx = None;
            }
        }
    }

    fn start_publish(&mut self, brief: &DisplayedBrief) {
        let payload = build_publish_payload(brief.date, &brief.headline, &brief.brief, &brief.articles);

        let endpoint = self.publish_endpoint.clone();
        let token = self.publish_token.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.publish_rx = Some(rx);
        self.publish_in_progress = true;
        self.publish_result_msg = None;

        self.runtime.spawn(async move {
            let res = do_publish_http(&endpoint, &token, &payload).await;
            let _ = tx.send(res);
        });
    }

    fn draw_publish_bar(&mut self, ui: &mut egui::Ui, brief: &DisplayedBrief) {
        egui::Frame::none()
            .fill(BG_RAISED)
            .inner_margin(egui::Margin::same(16.0))
            .stroke(Stroke::new(1.0, RULE))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(overline("PUBLISH DIGEST TO REMOTE"));
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        let settings_text = if self.publish_settings_open {
                            "Hide Config ⚙"
                        } else {
                            "Configure Endpoint ⚙"
                        };
                        if ui
                            .add(
                                egui::Button::new(
                                    RichText::new(settings_text)
                                        .font(FontId::new(9.5, FontFamily::Monospace))
                                        .color(INK_FAINT),
                                )
                                .fill(Color32::TRANSPARENT),
                            )
                            .clicked()
                        {
                            self.publish_settings_open = !self.publish_settings_open;
                        }
                    });
                });

                if self.publish_settings_open {
                    ui.add_space(8.0);
                    let mut config_changed = false;
                    ui.horizontal(|ui| {
                        ui.vertical(|ui| {
                            ui.label(
                                RichText::new("ENDPOINT URL")
                                    .font(FontId::new(9.0, FontFamily::Monospace))
                                    .color(INK_FAINT),
                            );
                            if ui.text_edit_singleline(&mut self.publish_endpoint).changed() {
                                config_changed = true;
                            }
                        });
                        ui.add_space(16.0);
                        ui.vertical(|ui| {
                            ui.label(
                                RichText::new("BEARER TOKEN")
                                    .font(FontId::new(9.0, FontFamily::Monospace))
                                    .color(INK_FAINT),
                            );
                            if ui.text_edit_singleline(&mut self.publish_token).changed() {
                                config_changed = true;
                            }
                        });
                    });
                    if config_changed {
                        if let Some(persona) = self.personas.get_mut(self.selected_persona_idx) {
                            persona.publish_endpoint = self.publish_endpoint.clone();
                            persona.publish_token = self.publish_token.clone();
                            let _ = self.storage.save_persona(persona);
                        }
                    }
                    ui.add_space(6.0);
                }

                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if self.publish_in_progress {
                        ui.add(egui::Spinner::new());
                        ui.add_space(6.0);
                        ui.label(
                            RichText::new("Pushing digest payload via HTTP POST...")
                                .font(FontId::new(12.0, FontFamily::Monospace))
                                .color(GOLD),
                        );
                    } else {
                        let pub_btn = ui.add(
                            egui::Button::new(
                                RichText::new("🚀  PUBLISH DIGEST")
                                    .font(FontId::new(11.0, FontFamily::Monospace))
                                    .color(BG),
                            )
                            .fill(GOLD)
                            .min_size(Vec2::new(160.0, 34.0)),
                        );

                        if pub_btn.clicked() {
                            self.start_publish(brief);
                        }

                        ui.add_space(12.0);
                        ui.label(
                            RichText::new(format!("Target: {}", self.publish_endpoint))
                                .font(FontId::new(10.0, FontFamily::Monospace))
                                .color(INK_FAINT),
                        );
                    }
                });

                if let Some((is_err, ref msg)) = self.publish_result_msg {
                    ui.add_space(8.0);
                    let color = if is_err { ACCENT } else { GREEN };
                    ui.label(
                        RichText::new(msg)
                            .font(FontId::new(11.0, FontFamily::Monospace))
                            .color(color),
                    );
                }
            });
    }

    fn draw_results(&mut self, ui: &mut egui::Ui) {
        let brief = match &self.current_brief {
            Some(b) => b.clone(),
            None => return,
        };

        ui.add_space(20.0);
        ui.vertical_centered(|ui| {
            ui.set_max_width(1000.0);
            ui.vertical(|ui| {
                self.draw_date_nav(ui, brief.date);

                ui.add_space(20.0);
                draw_double_rule(ui);
                ui.add_space(28.0);

                ui.label(overline("TODAY'S HEADLINE"));
                ui.add_space(8.0);
                ui.label(
                    RichText::new(&brief.headline)
                        .font(FontId::new(32.0, FontFamily::Name("serif-bold".into())))
                        .color(GOLD),
                );

                ui.add_space(24.0);

                ui.label(overline("EXECUTIVE BRIEFING"));
                ui.add_space(16.0);
                ui.label(
                    RichText::new(&brief.brief)
                        .font(FontId::new(20.0, FontFamily::Name("serif".into())))
                        .color(INK),
                );

                ui.add_space(20.0);
                ui.label(
                    RichText::new(format!(
                        "{} feeds · {} articles scanned · {} surfaced · model {}",
                        brief.stats.feeds_fetched,
                        brief.stats.total_articles,
                        brief.stats.articles_kept,
                        brief.model,
                    ))
                    .font(FontId::new(10.0, FontFamily::Monospace))
                    .color(INK_FAINT),
                );

                ui.add_space(24.0);
                draw_rule(ui);
                ui.add_space(20.0);

                self.draw_publish_bar(ui, &brief);

                ui.add_space(24.0);
                draw_rule(ui);
                ui.add_space(20.0);

                self.draw_filter_bar(ui, &brief.articles);
                ui.add_space(24.0);
                if let Some(url_to_remove) = self.draw_articles(ui, &brief.articles) {
                    self.remove_article(&url_to_remove);
                }

                ui.add_space(40.0);
                ui.vertical_centered(|ui| {
                    if ghost_button(ui, "FETCH AGAIN").clicked() {
                        self.start_fetch();
                    }
                });
                ui.add_space(40.0);
            });
        });
    }

    fn draw_date_nav(&mut self, ui: &mut egui::Ui, current: NaiveDate) {
        let persona_id = self.personas[self.selected_persona_idx].id.unwrap_or(1);
        let prev = self
            .storage
            .previous_date(current, persona_id)
            .ok()
            .flatten();
        let next = self.storage.next_date(current, persona_id).ok().flatten();
        let today = Local::now().date_naive();

        ui.horizontal(|ui| {
            let prev_label = match prev {
                Some(d) => format!("←  {}", d.format("%b %d")),
                None => "←  no earlier".into(),
            };
            let prev_resp = ui.add_enabled(
                prev.is_some(),
                egui::Button::new(
                    RichText::new(prev_label)
                        .font(FontId::new(10.5, FontFamily::Monospace))
                        .color(INK_DIM),
                )
                .fill(Color32::TRANSPARENT)
                .stroke(Stroke::NONE),
            );
            if prev_resp.clicked() {
                if let Some(d) = prev {
                    self.navigate(d);
                }
            }

            ui.add_space(12.0);
            ui.label(
                RichText::new(current.format("%A · %B %d, %Y").to_string())
                    .font(FontId::new(20.0, FontFamily::Name("serif-italic".into())))
                    .color(GOLD),
            );
            ui.add_space(12.0);

            let next_label = match next {
                Some(d) => format!("{}  →", d.format("%b %d")),
                None => "no later  →".into(),
            };
            let next_resp = ui.add_enabled(
                next.is_some(),
                egui::Button::new(
                    RichText::new(next_label)
                        .font(FontId::new(10.5, FontFamily::Monospace))
                        .color(INK_DIM),
                )
                .fill(Color32::TRANSPARENT)
                .stroke(Stroke::NONE),
            );
            if next_resp.clicked() {
                if let Some(d) = next {
                    self.navigate(d);
                }
            }

            ui.add_space(20.0);
            if current != today && ghost_button(ui, "JUMP TO TODAY").clicked() {
                if let Ok(Some(stored)) = self.storage.load(today, persona_id) {
                    self.current_brief = Some(DisplayedBrief::from_stored(stored));
                } else {
                    self.view = View::Idle;
                }
            }
        });
    }

    fn draw_filter_bar(&mut self, ui: &mut egui::Ui, articles: &[Article]) {
        let mut topics: Vec<String> = articles
            .iter()
            .filter_map(|a| a.topic_tag.clone())
            .collect();
        topics.sort();
        topics.dedup();

        ui.horizontal_wrapped(|ui| {
            if topic_pill(ui, "all", self.topic_filter == "all").clicked() {
                self.topic_filter = "all".to_string();
            }
            for t in &topics {
                if topic_pill(ui, t, self.topic_filter == *t).clicked() {
                    self.topic_filter = t.clone();
                }
            }
        });
    }

    fn draw_articles(&mut self, ui: &mut egui::Ui, articles: &[Article]) -> Option<String> {
        if articles.is_empty() {
            egui::Frame::none()
                .fill(BG_RAISED)
                .stroke(Stroke::new(1.0, RULE))
                .inner_margin(egui::Margin::same(24.0))
                .show(ui, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.label(
                            RichText::new("No articles remaining in this briefing.")
                                .font(FontId::new(14.0, FontFamily::Name("serif-italic".into())))
                                .color(INK_FAINT),
                        );
                    });
                });
            return None;
        }

        let filter = self.topic_filter.clone();
        let filtered: Vec<&Article> = articles
            .iter()
            .filter(|a| filter == "all" || a.topic_tag.as_deref() == Some(&filter))
            .collect();

        if filtered.is_empty() {
            egui::Frame::none()
                .fill(BG_RAISED)
                .stroke(Stroke::new(1.0, RULE))
                .inner_margin(egui::Margin::same(24.0))
                .show(ui, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.label(
                            RichText::new("No articles matching topic filter.")
                                .font(FontId::new(14.0, FontFamily::Name("serif-italic".into())))
                                .color(INK_FAINT),
                        );
                    });
                });
            return None;
        }

        let mut article_to_remove = None;
        let col_w = (ui.available_width() - 24.0) / 2.0;
        for chunk in filtered.chunks(2) {
            ui.horizontal_top(|ui| {
                for article in chunk {
                    ui.allocate_ui_with_layout(
                        Vec2::new(col_w, 0.0),
                        egui::Layout::top_down(egui::Align::LEFT),
                        |ui| {
                            if draw_article_card(ui, article) {
                                article_to_remove = Some(article.url.clone());
                            }
                        },
                    );
                    ui.add_space(20.0);
                }
            });
            ui.add_space(20.0);
        }

        article_to_remove
    }
}

fn draw_rule(ui: &mut egui::Ui) {
    let (rect, _) =
        ui.allocate_exact_size(Vec2::new(ui.available_width(), 1.0), egui::Sense::hover());
    ui.painter().line_segment(
        [rect.left_center(), rect.right_center()],
        Stroke::new(1.0, RULE),
    );
}

fn draw_double_rule(ui: &mut egui::Ui) {
    for _ in 0..2 {
        let (rect, _) =
            ui.allocate_exact_size(Vec2::new(ui.available_width(), 1.0), egui::Sense::hover());
        ui.painter().line_segment(
            [rect.left_center(), rect.right_center()],
            Stroke::new(1.0, RULE),
        );
        ui.add_space(2.0);
    }
}

fn overline(text: &str) -> RichText {
    RichText::new(text)
        .font(FontId::new(10.5, FontFamily::Monospace))
        .color(ACCENT)
}

fn control_select(ui: &mut egui::Ui, label: &str, value: &mut String, options: &[&str]) {
    ui.vertical(|ui| {
        ui.label(
            RichText::new(label)
                .font(FontId::new(9.5, FontFamily::Monospace))
                .color(INK_FAINT),
        );
        egui::ComboBox::from_id_salt(label)
            .selected_text(
                RichText::new(value.as_str())
                    .font(FontId::new(15.0, FontFamily::Name("serif-italic".into())))
                    .color(GOLD),
            )
            .show_ui(ui, |ui| {
                for opt in options {
                    if ui.selectable_label(value == opt, *opt).clicked() {
                        *value = opt.to_string();
                    }
                }
            });
    });
}

fn fetch_button(ui: &mut egui::Ui, text: &str) -> egui::Response {
    let (rect, response) = ui.allocate_exact_size(Vec2::new(360.0, 56.0), egui::Sense::click());
    let bg = if response.hovered() { GOLD } else { ACCENT };
    ui.painter().rect_filled(rect, 0.0, bg);
    let galley = ui.painter().layout_no_wrap(
        text.to_string(),
        FontId::new(13.0, FontFamily::Monospace),
        BG,
    );
    let pos = rect.center() - galley.size() / 2.0;
    ui.painter().galley(pos, galley, BG);
    if response.hovered() {
        ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
    }
    response
}

fn ghost_button(ui: &mut egui::Ui, text: &str) -> egui::Response {
    ui.add(
        egui::Button::new(
            RichText::new(text)
                .font(FontId::new(10.5, FontFamily::Monospace))
                .color(INK_DIM),
        )
        .fill(Color32::TRANSPARENT)
        .stroke(Stroke::new(1.0, RULE))
        .min_size(Vec2::new(0.0, 36.0)),
    )
}

fn topic_pill(ui: &mut egui::Ui, text: &str, active: bool) -> egui::Response {
    let bg = if active { INK } else { Color32::TRANSPARENT };
    let fg = if active { BG } else { INK_DIM };
    let resp = ui.add(
        egui::Button::new(
            RichText::new(text.to_uppercase())
                .font(FontId::new(9.5, FontFamily::Monospace))
                .color(fg),
        )
        .fill(bg)
        .stroke(Stroke::new(1.0, RULE)),
    );
    ui.add_space(4.0);
    resp
}

#[allow(dead_code)]
fn history_pill(ui: &mut egui::Ui, text: &str) -> egui::Response {
    let resp = ui.add(
        egui::Button::new(
            RichText::new(text.to_uppercase())
                .font(FontId::new(10.0, FontFamily::Monospace))
                .color(INK_DIM),
        )
        .fill(BG_RAISED)
        .stroke(Stroke::new(1.0, RULE)),
    );
    ui.add_space(6.0);
    resp
}

fn draw_article_card(ui: &mut egui::Ui, article: &Article) -> bool {
    let mut remove_clicked = false;
    egui::Frame::none()
        .fill(BG_PAPER)
        .inner_margin(egui::Margin::same(20.0))
        .stroke(Stroke::new(1.0, RULE))
        .show(ui, |ui| {
            if let Some(topic) = &article.topic_tag {
                ui.horizontal(|ui| {
                    egui::Frame::none()
                        .fill(Color32::from_rgba_premultiplied(255, 87, 34, 30))
                        .inner_margin(egui::Margin {
                            left: 8.0,
                            right: 8.0,
                            top: 3.0,
                            bottom: 3.0,
                        })
                        .show(ui, |ui| {
                            ui.label(
                                RichText::new(topic.to_uppercase())
                                    .font(FontId::new(9.0, FontFamily::Monospace))
                                    .color(GOLD),
                            );
                        });
                });
                ui.add_space(8.0);
            }

            ui.horizontal(|ui| {
                let date_str = article
                    .published
                    .with_timezone(&Local)
                    .format("%b %d · %H:%M")
                    .to_string();
                ui.label(
                    RichText::new(format!("{} · {}", article.source.to_uppercase(), date_str))
                        .font(FontId::new(9.5, FontFamily::Monospace))
                        .color(INK_FAINT),
                );
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    let score = article.relevance.unwrap_or(0.0);
                    ui.label(
                        RichText::new(format!("{:.1}/10", score))
                            .font(FontId::new(10.0, FontFamily::Monospace))
                            .color(ACCENT),
                    );
                });
            });

            ui.add_space(10.0);
            ui.label(
                RichText::new(&article.title)
                    .font(FontId::new(18.0, FontFamily::Name("serif-bold".into())))
                    .color(INK),
            );
            ui.add_space(10.0);

            let summary = article
                .ai_summary
                .clone()
                .unwrap_or_else(|| article.summary.clone());
            ui.label(
                RichText::new(summary)
                    .font(FontId::new(13.5, FontFamily::Name("serif".into())))
                    .color(INK_DIM),
            );

            ui.add_space(14.0);
            ui.horizontal(|ui| {
                let resp = ui.add(
                    egui::Label::new(
                        RichText::new("READ AT SOURCE  →")
                            .font(FontId::new(9.5, FontFamily::Monospace))
                            .color(ACCENT),
                    )
                    .sense(egui::Sense::click()),
                );
                if resp.hovered() {
                    ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
                }
                if resp.clicked() {
                    let _ = open::that(&article.url);
                }

                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    let remove_btn = ui.add(
                        egui::Button::new(
                            RichText::new("🗑 Remove")
                                .font(FontId::new(10.0, FontFamily::Monospace))
                                .color(ACCENT),
                        )
                        .fill(BG_RAISED)
                        .stroke(Stroke::new(1.0, RULE)),
                    );
                    if remove_btn.hovered() {
                        ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
                    }
                    if remove_btn.clicked() {
                        remove_clicked = true;
                    }
                });
            });
        });
    remove_clicked
}


fn configure_style(ctx: &egui::Context) {
    let mut style = (*ctx.style()).clone();
    style.visuals.panel_fill = BG;
    style.visuals.window_fill = BG;
    style.visuals.extreme_bg_color = BG;
    style.visuals.faint_bg_color = BG_RAISED;
    style.visuals.override_text_color = Some(INK);
    style.visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0, RULE);
    style.visuals.widgets.inactive.bg_fill = BG_RAISED;
    style.visuals.widgets.inactive.weak_bg_fill = BG_RAISED;
    style.visuals.widgets.inactive.bg_stroke = Stroke::new(1.0, RULE);
    style.visuals.widgets.hovered.bg_fill = BG_PAPER;
    style.visuals.widgets.hovered.weak_bg_fill = BG_PAPER;
    style.visuals.widgets.hovered.bg_stroke = Stroke::new(1.0, INK_DIM);
    style.visuals.widgets.active.bg_fill = ACCENT;
    style.visuals.widgets.active.weak_bg_fill = ACCENT;
    style.visuals.selection.bg_fill = ACCENT;
    style.visuals.selection.stroke = Stroke::new(1.0, ACCENT);
    style.spacing.item_spacing = Vec2::new(4.0, 6.0);
    ctx.set_style(style);
}

fn configure_fonts(ctx: &egui::Context) {
    let mut fonts = egui::FontDefinitions::default();

    let try_load =
        |name: &str, path: &std::path::Path, fonts: &mut egui::FontDefinitions| -> bool {
            match std::fs::read(path) {
                Ok(bytes) => {
                    fonts
                        .font_data
                        .insert(name.to_string(), egui::FontData::from_owned(bytes));
                    true
                }
                Err(_) => false,
            }
        };

    let assets = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("assets");
    let has_serif = try_load("serif", &assets.join("Fraunces-Regular.ttf"), &mut fonts);
    let has_serif_bold = try_load("serif_bold", &assets.join("Fraunces-Bold.ttf"), &mut fonts);
    let has_serif_italic = try_load(
        "serif_italic",
        &assets.join("Fraunces-Italic.ttf"),
        &mut fonts,
    );
    let has_mono = try_load(
        "mono",
        &assets.join("JetBrainsMono-Regular.ttf"),
        &mut fonts,
    );

    let default_prop: Vec<String> = fonts
        .families
        .get(&FontFamily::Proportional)
        .cloned()
        .unwrap_or_default();

    if has_serif {
        fonts
            .families
            .insert(FontFamily::Name("serif".into()), vec!["serif".into()]);
    } else {
        fonts
            .families
            .insert(FontFamily::Name("serif".into()), default_prop.clone());
    }

    if has_serif_bold {
        fonts.families.insert(
            FontFamily::Name("serif-bold".into()),
            vec!["serif_bold".into()],
        );
    } else {
        let fallback = fonts
            .families
            .get(&FontFamily::Name("serif".into()))
            .cloned()
            .unwrap_or(default_prop.clone());
        fonts
            .families
            .insert(FontFamily::Name("serif-bold".into()), fallback);
    }

    if has_serif_italic {
        fonts.families.insert(
            FontFamily::Name("serif-italic".into()),
            vec!["serif_italic".into()],
        );
    } else {
        let fallback = fonts
            .families
            .get(&FontFamily::Name("serif".into()))
            .cloned()
            .unwrap_or(default_prop);
        fonts
            .families
            .insert(FontFamily::Name("serif-italic".into()), fallback);
    }

    if has_mono {
        fonts
            .families
            .entry(FontFamily::Monospace)
            .or_default()
            .insert(0, "mono".into());
    }

    ctx.set_fonts(fonts);
}

// Tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_personas_subview_transitions() {
        let storage = Storage::open_in_memory().expect("open memory storage");
        let personas = storage.list_personas().unwrap_or_else(|_| vec![Persona::default()]);
        let mut app = FeedbriefApp {
            runtime: Arc::new(tokio::runtime::Builder::new_current_thread().build().unwrap()),
            storage,
            view: View::Idle,
            personas: personas.clone(),
            selected_persona_idx: 0,
            editing_persona: Persona::default(),
            editing_persona_idx: None,
            editing_feeds_text: String::new(),
            delete_confirm_target: None,
            delete_brief_confirm_target: None,
            persona_export_path: String::new(),
            persona_import_path: String::new(),
            persona_message: String::new(),
            persona_message_is_error: false,
            llm_config: LlmConfig::default(),
            llm_settings_open: false,
            hours: 24,
            top_n: 20,
            progress_rx: None,
            progress_log: Arc::new(Mutex::new(Vec::new())),
            current_stage: String::new(),
            current_message: String::new(),
            current_percent: 0,
            current_brief: None,
            topic_filter: String::new(),
            llm_ok: false,
            last_llm_check: std::time::Instant::now(),
            llm_check_rx: None,
            available_dates: vec![],
            publish_endpoint: String::new(),
            publish_token: String::new(),
            publish_settings_open: false,
            publish_in_progress: false,
            publish_result_msg: None,
            publish_rx: None,
        };

        // Initially in Idle view
        assert_eq!(app.view, View::Idle);

        // Start add persona -> switches to Form view with None index
        app.start_add_persona();
        assert_eq!(app.view, View::PersonasConfig(PersonasSubView::Form { index: None }));
        assert_eq!(app.editing_persona.name, "New Persona");

        // Start edit persona -> switches to Form view with Some(0) index
        app.start_edit_persona(0);
        assert_eq!(app.view, View::PersonasConfig(PersonasSubView::Form { index: Some(0) }));
        assert_eq!(app.editing_persona.name, personas[0].name);

        // Edit feeds and save
        app.editing_feeds_text = "https://example.com/rss.xml\nhttps://test.com/feed".to_string();
        app.save_editing_persona();

        // After saving -> returns to List view
        assert_eq!(app.view, View::PersonasConfig(PersonasSubView::List));
        assert_eq!(app.personas[0].feeds.len(), 2);
    }

    #[test]
    fn test_history_view_navigation() {
        let storage = Storage::open_in_memory().expect("open memory storage");
        let personas = storage.list_personas().unwrap_or_else(|_| vec![Persona::default()]);

        // Save a brief into storage
        let date = NaiveDate::from_ymd_opt(2026, 8, 15).unwrap();
        let stats = BriefStats {
            feeds_fetched: 3,
            total_articles: 25,
            articles_kept: 5,
        };
        storage.save(date, 1, "Tech Breakthroughs", "Brief content...", &[], &stats, "ollama:llama3").unwrap();

        let mut app = FeedbriefApp {
            runtime: Arc::new(tokio::runtime::Builder::new_current_thread().build().unwrap()),
            storage,
            view: View::Idle,
            personas,
            selected_persona_idx: 0,
            editing_persona: Persona::default(),
            editing_persona_idx: None,
            editing_feeds_text: String::new(),
            delete_confirm_target: None,
            delete_brief_confirm_target: None,
            persona_export_path: String::new(),
            persona_import_path: String::new(),
            persona_message: String::new(),
            persona_message_is_error: false,
            llm_config: LlmConfig::default(),
            llm_settings_open: false,
            hours: 24,
            top_n: 20,
            progress_rx: None,
            progress_log: Arc::new(Mutex::new(Vec::new())),
            current_stage: String::new(),
            current_message: String::new(),
            current_percent: 0,
            current_brief: None,
            topic_filter: String::new(),
            llm_ok: false,
            last_llm_check: std::time::Instant::now(),
            llm_check_rx: None,
            available_dates: vec![date],
            publish_endpoint: String::new(),
            publish_token: String::new(),
            publish_settings_open: false,
            publish_in_progress: false,
            publish_result_msg: None,
            publish_rx: None,
        };

        // Switch to History view
        app.view = View::History;
        assert_eq!(app.view, View::History);

        // Navigate to date
        app.navigate(date);
        assert_eq!(app.view, View::Results);
        assert!(app.current_brief.is_some());
        assert_eq!(app.current_brief.unwrap().headline, "Tech Breakthroughs");
    }

    #[test]
    fn test_app_remove_article() {
        let storage = Storage::open_in_memory().expect("open memory storage");
        let personas = storage.list_personas().unwrap_or_else(|_| vec![Persona::default()]);
        let date = NaiveDate::from_ymd_opt(2026, 8, 15).unwrap();

        let a1 = Article {
            id: "a".to_string(),
            title: "Item A".to_string(),
            url: "https://example.com/a".to_string(),
            source: "Source A".to_string(),
            category: "Tech".to_string(),
            published: chrono::Utc::now(),
            summary: "Summary A".to_string(),
            ai_summary: None,
            relevance: Some(9.0),
            topic_tag: Some("tech".to_string()),
        };
        let a2 = Article {
            id: "b".to_string(),
            title: "Item B".to_string(),
            url: "https://example.com/b".to_string(),
            source: "Source B".to_string(),
            category: "AI".to_string(),
            published: chrono::Utc::now(),
            summary: "Summary B".to_string(),
            ai_summary: None,
            relevance: Some(8.0),
            topic_tag: Some("ai".to_string()),
        };


        let stats = BriefStats {
            feeds_fetched: 2,
            total_articles: 10,
            articles_kept: 2,
        };

        storage.save(date, 1, "Brief 1", "Executive summary...", &[a1.clone(), a2.clone()], &stats, "model").unwrap();

        let mut app = FeedbriefApp {
            runtime: Arc::new(tokio::runtime::Builder::new_current_thread().build().unwrap()),
            storage,
            view: View::Idle,
            personas,
            selected_persona_idx: 0,
            editing_persona: Persona::default(),
            editing_persona_idx: None,
            editing_feeds_text: String::new(),
            delete_confirm_target: None,
            delete_brief_confirm_target: None,
            persona_export_path: String::new(),
            persona_import_path: String::new(),
            persona_message: String::new(),
            persona_message_is_error: false,
            llm_config: LlmConfig::default(),
            llm_settings_open: false,
            hours: 24,
            top_n: 20,
            progress_rx: None,
            progress_log: Arc::new(Mutex::new(Vec::new())),
            current_stage: String::new(),
            current_message: String::new(),
            current_percent: 0,
            current_brief: None,
            topic_filter: "tech".to_string(),
            llm_ok: false,
            last_llm_check: std::time::Instant::now(),
            llm_check_rx: None,
            available_dates: vec![date],
            publish_endpoint: String::new(),
            publish_token: String::new(),
            publish_settings_open: false,
            publish_in_progress: false,
            publish_result_msg: None,
            publish_rx: None,
        };

        app.navigate(date);
        assert_eq!(app.current_brief.as_ref().unwrap().articles.len(), 2);

        // Remove item A by URL
        app.remove_article("https://example.com/a");

        let cur = app.current_brief.as_ref().unwrap();
        assert_eq!(cur.articles.len(), 1);
        assert_eq!(cur.articles[0].url, "https://example.com/b");
        assert_eq!(cur.stats.articles_kept, 1);

        // Since topic filter was "tech" and item A (tech) was removed, topic_filter auto-resets to "all"
        assert_eq!(app.topic_filter, "all");

        // Verify payload built for publish now excludes item A
        let payload = crate::publish::build_publish_payload(cur.date, &cur.headline, &cur.brief, &cur.articles);
        assert_eq!(payload.sources.len(), 1);
        assert_eq!(payload.sources[0].url, "https://example.com/b");
    }
}



