use polymarket_rs::engine::{operator_command_channel, run_operational_runtime};
use polymarket_rs::error::Result;
use polymarket_rs::gui::run_gui;
use polymarket_rs::runtime::{build_startup_report, require_cli_startup_safe};
use polymarket_rs::state::{shared_runtime_state, RuntimeState};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    dotenvy::dotenv().ok();
    init_tracing();
    let args = CliArgs::parse(std::env::args().skip(1));
    if let Err(error) = run(args).await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

async fn run(args: CliArgs) -> Result<()> {
    let report = build_startup_report(&args.config_path)?;
    if !args.gui {
        require_cli_startup_safe(&report)?;
    }
    let started_at_ms = polymarket_rs::engine::system_now_ms();
    let mut runtime_state = RuntimeState::new(
        report.settings.mode,
        started_at_ms,
        report.settings.enable_external_signal && !report.settings.external_symbols.is_empty(),
        !report.settings.watched_wallets.is_empty(),
    );
    if args.start_paper {
        if report.settings.mode != polymarket_rs::types::BotMode::Paper {
            return Err(polymarket_rs::error::BotError::Config(
                "--start-paper requires paper mode".to_string(),
            ));
        }
        runtime_state.paused = false;
        runtime_state.strategy_enabled = true;
    }
    let shared_state = shared_runtime_state(runtime_state);
    let (command_sender, command_receiver) = operator_command_channel();
    let shutdown = CancellationToken::new();

    let signal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_shutdown.cancel();
        }
    });

    let gui_handle = if args.gui {
        let bind_addr = args.bind_addr.clone();
        let gui_report = report.clone();
        let gui_state = shared_state.clone();
        let gui_commands = command_sender.clone();
        let gui_shutdown = shutdown.clone();
        Some(tokio::task::spawn_blocking(move || {
            run_gui(
                &bind_addr,
                gui_report,
                gui_state,
                gui_commands,
                gui_shutdown,
            )
        }))
    } else {
        None
    };

    println!(
        "polymarket-rs runtime starting in {} mode with {} configured market asset(s); {}",
        report.mode_label(),
        report.settings.asset_ids.len(),
        if args.start_paper {
            "paper strategy requested active"
        } else {
            "strategy starts paused"
        }
    );
    let runtime_future = run_operational_runtime(
        report.settings.clone(),
        args.config_path,
        shared_state,
        command_receiver,
        shutdown.clone(),
    );
    tokio::pin!(runtime_future);
    let runtime_result = if let Some(mut handle) = gui_handle {
        tokio::select! {
            result = &mut runtime_future => {
                shutdown.cancel();
                match handle.await {
                    Ok(gui_result) => gui_result?,
                    Err(error) => {
                        return Err(polymarket_rs::error::BotError::Execution(format!(
                            "gui_task_join:{error}"
                        )));
                    }
                }
                result
            }
            gui_join = &mut handle => {
                shutdown.cancel();
                let result = runtime_future.await;
                match gui_join {
                    Ok(gui_result) => gui_result?,
                    Err(error) => {
                        return Err(polymarket_rs::error::BotError::Execution(format!(
                            "gui_task_join:{error}"
                        )));
                    }
                }
                result
            }
        }
    } else {
        runtime_future.await
    };
    shutdown.cancel();
    runtime_result
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("info,hyper=warn,reqwest=warn,rustls=warn,tungstenite=warn")
    });
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliArgs {
    config_path: String,
    gui: bool,
    bind_addr: String,
    start_paper: bool,
}

impl CliArgs {
    fn parse(args: impl Iterator<Item = String>) -> Self {
        let mut gui = false;
        let mut start_paper = false;
        let mut bind_addr = "127.0.0.1:8787".to_string();
        let mut config_path = None;
        let mut pending_bind = false;
        for arg in args {
            if pending_bind {
                bind_addr = arg;
                pending_bind = false;
                continue;
            }
            match arg.as_str() {
                "--gui" => gui = true,
                "--gui-bind" => pending_bind = true,
                "--start-paper" => start_paper = true,
                "--help" | "-h" => {
                    println!(
                        "usage: polymarket-rs [--gui] [--gui-bind 127.0.0.1:8787] [--start-paper] [config.toml]"
                    );
                    std::process::exit(0);
                }
                value => config_path = Some(value.to_string()),
            }
        }
        if pending_bind {
            eprintln!("--gui-bind requires an address");
            std::process::exit(2);
        }
        Self {
            config_path: config_path.unwrap_or_else(|| "config.example.toml".to_string()),
            gui,
            bind_addr,
            start_paper,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_args_keep_existing_config_position() {
        let args = CliArgs::parse(["config.example.toml".to_string()].into_iter());
        assert_eq!(args.config_path, "config.example.toml");
        assert!(!args.gui);
        assert!(!args.start_paper);
    }

    #[test]
    fn cli_args_parse_gui_bind_and_explicit_start() {
        let args = CliArgs::parse(
            [
                "--gui".to_string(),
                "--start-paper".to_string(),
                "--gui-bind".to_string(),
                "127.0.0.1:9999".to_string(),
                "config.example.toml".to_string(),
            ]
            .into_iter(),
        );
        assert!(args.gui);
        assert!(args.start_paper);
        assert_eq!(args.bind_addr, "127.0.0.1:9999");
    }

    #[test]
    fn paper_example_is_cli_safe() {
        let report = build_startup_report("config.example.toml").unwrap();
        assert!(require_cli_startup_safe(&report).is_ok());
    }
}
