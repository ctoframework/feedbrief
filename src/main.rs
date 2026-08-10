#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 {
        if let Err(e) = run_cli(&args) {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    } else {
        if let Err(e) = run_gui() {
            eprintln!("GUI Error: {}", e);
            std::process::exit(1);
        }
    }
}

fn run_gui() -> Result<(), eframe::Error> {
    let options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_inner_size([1200.0, 850.0])
            .with_min_inner_size([900.0, 600.0])
            .with_title("Feedbrief"),
        ..Default::default()
    };
    eframe::run_native(
        "Feedbrief",
        options,
        Box::new(|cc| Ok(Box::new(feedbrief::app::FeedbriefApp::new(cc)))),
    )
}

fn parse_args(args: &[String]) -> Result<(String, String, Option<String>), anyhow::Error> {
    let mut persona = "Default".to_string();
    let mut cmd = None;
    let mut sub_arg = None;

    let mut i = 1;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-p" || arg == "--persona" {
            if i + 1 < args.len() {
                persona = args[i + 1].clone();
                i += 2;
            } else {
                anyhow::bail!("Missing value for persona option");
            }
        } else if cmd.is_none() {
            cmd = Some(arg.clone());
            i += 1;
        } else if sub_arg.is_none() {
            sub_arg = Some(arg.clone());
            i += 1;
        } else {
            anyhow::bail!("Unexpected argument: {}", arg);
        }
    }

    let command = match cmd {
        Some(c) => c,
        None => anyhow::bail!("No command specified. Choose fetch, publish, or view."),
    };

    Ok((persona, command, sub_arg))
}

fn run_cli(args: &[String]) -> anyhow::Result<()> {
    let (persona_name, command, sub_arg) = parse_args(args)?;

    let storage = feedbrief::storage::Storage::open()?;
    let personas = storage.list_personas()?;
    let persona = personas
        .iter()
        .find(|p| p.name.eq_ignore_ascii_case(&persona_name))
        .ok_or_else(|| {
            let names: Vec<String> = personas.iter().map(|p| p.name.clone()).collect();
            anyhow::anyhow!(
                "Persona '{}' not found. Available personas: {}",
                persona_name,
                names.join(", ")
            )
        })?;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    match command.as_str() {
        "fetch" => {
            rt.block_on(async {
                println!("Fetching feeds for persona \"{}\"...", persona.name);
                let llm_config = storage.load_llm_config().unwrap_or_default();
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                let cfg = feedbrief::pipeline::PipelineConfig {
                    llm_config,
                    hours: 24,
                    top_n: 20,
                    persona: persona.clone(),
                };

                let handle = tokio::spawn(feedbrief::pipeline::run_pipeline(cfg, tx));

                while let Some(event) = rx.recv().await {
                    match event {
                        feedbrief::progress::ProgressEvent::Stage { stage, message, percent } => {
                            println!("[{}] {} ({}%)", stage, message, percent);
                        }
                        feedbrief::progress::ProgressEvent::Done { headline, brief, articles, stats } => {
                            let today = chrono::Local::now().date_naive();
                            let persona_id = persona.id.unwrap_or(1);
                            let llm_config = storage.load_llm_config().unwrap_or_default();
                            if let Err(e) = storage.save(
                                today,
                                persona_id,
                                &headline,
                                &brief,
                                &articles,
                                &stats,
                                &llm_config.active_model_display(),
                            ) {
                                eprintln!("Failed to save brief: {}", e);
                            } else {
                                println!("\nDone! Brief saved to database for persona \"{}\" on date {}", persona.name, today);
                                println!("Headline: {}", headline);
                                println!("Articles kept: {} (out of {} fetched)", stats.articles_kept, stats.total_articles);
                            }
                        }
                        feedbrief::progress::ProgressEvent::Error(err_msg) => {
                            eprintln!("\nPipeline Error: {}", err_msg);
                        }
                    }
                }
                let _ = handle.await;
                Ok::<(), anyhow::Error>(())
            })?;
        }
        "publish" => {
            rt.block_on(async {
                let today = chrono::Local::now().date_naive();
                let persona_id = persona.id.unwrap_or(1);
                let brief_opt = storage.load(today, persona_id)?;
                let brief = match brief_opt {
                    Some(b) => b,
                    None => {
                        anyhow::bail!("No brief found for persona \"{}\" on today's date ({}). Run 'fetch' first.", persona.name, today);
                    }
                };

                println!("Publishing brief for persona \"{}\" (date: {})...", persona.name, today);
                let payload = feedbrief::publish::build_publish_payload(
                    brief.date,
                    &brief.headline,
                    &brief.brief,
                    &brief.articles,
                );

                match feedbrief::publish::do_publish_http(&persona.publish_endpoint, &persona.publish_token, &payload).await {
                    Ok(msg) => {
                        println!("Success: {}", msg);
                    }
                    Err(err) => {
                        anyhow::bail!("Publish failed: {}", err);
                    }
                }
                Ok(())
            })?;
        }
        "view" => {
            let date = match sub_arg {
                Some(ref s) => {
                    let mut s_clean = s.as_str();
                    if s_clean.starts_with('[') && s_clean.ends_with(']') {
                        s_clean = &s_clean[1..s_clean.len() - 1];
                    }
                    chrono::NaiveDate::parse_from_str(s_clean, "%Y-%m-%d")
                        .map_err(|e| anyhow::anyhow!("Invalid date format '{}'. Expected YYYY-MM-DD. Error: {}", s, e))?
                }
                None => chrono::Local::now().date_naive(),
            };

            let persona_id = persona.id.unwrap_or(1);
            let brief_opt = storage.load(date, persona_id)?;
            let brief = match brief_opt {
                Some(b) => b,
                None => {
                    anyhow::bail!("No brief found for persona \"{}\" on date {}.", persona.name, date);
                }
            };

            println!("================================================================================");
            println!("DAILY BRIEFING: {} - Persona: {}", date, persona.name);
            println!("================================================================================");
            println!("Headline: {}", brief.headline);
            println!("Model   : {}", brief.model);
            println!("Stats   : {} kept / {} fetched across {} feeds", brief.stats.articles_kept, brief.stats.total_articles, brief.stats.feeds_fetched);
            println!("--------------------------------------------------------------------------------");
            println!("\nExecutive Briefing:\n");
            println!("{}", brief.brief);
            println!("\n--------------------------------------------------------------------------------");
            println!("Key Stories:");
            for (idx, a) in brief.articles.iter().enumerate() {
                println!("\n[{}] {} ({})", idx + 1, a.title, a.url);
                let summary = a.ai_summary.as_deref().unwrap_or(&a.summary);
                println!("    Summary: {}", summary);
            }
            println!("================================================================================");
        }
        other => {
            anyhow::bail!("Unknown command '{}'. Choose fetch, publish, or view.", other);
        }
    }

    Ok(())
}
