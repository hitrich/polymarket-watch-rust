use crate::compliance::{ComplianceState, EligibilityStatus};
use crate::config::Settings;
use crate::error::{BotError, Result};
use crate::heartbeat::HeartbeatState;
use crate::journal::{verify_journal, JournalVerification};
use crate::readiness::{validate_protocol_compatibility, ReadinessState};
use crate::reconcile::{recover_after_crash, RecoveryReport, RemoteSnapshot};
use crate::secrets::SecretStatus;
use crate::types::{BotMode, WalletMode};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct StartupReport {
    pub config_path: String,
    pub generated_at_ms: u64,
    pub settings: Settings,
    pub readiness: ReadinessState,
    pub compliance: ComplianceState,
    pub heartbeat: HeartbeatState,
    pub recovery: RecoveryReport,
    pub journal: JournalVerification,
    pub secrets: SecretStatus,
    pub live_submission_enabled: bool,
    pub live_lock_reasons: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateStatus {
    pub name: &'static str,
    pub passed: bool,
    pub blocking: bool,
    pub detail: String,
}

impl StartupReport {
    pub fn mode_label(&self) -> &'static str {
        match self.settings.mode {
            BotMode::Paper => "PAPER",
            BotMode::Live => "LIVE",
        }
    }

    pub fn readiness_gates(&self) -> Vec<GateStatus> {
        vec![
            gate(
                "Protocol config",
                self.readiness.protocol_verified,
                "V2 host/collateral",
            ),
            gate(
                "Wallet path",
                self.readiness.wallet_path_verified,
                wallet_readiness_detail(self.settings.wallet_mode),
            ),
            gate(
                "Signer authorized",
                self.readiness.signer_authorized,
                "order signer",
            ),
            gate(
                "Funder verified",
                self.readiness.funder_verified,
                "funding wallet",
            ),
            gate(
                "Balance",
                self.readiness.balance_verified,
                "collateral balance",
            ),
            gate(
                "Allowance",
                self.readiness.allowance_verified,
                "token allowance",
            ),
            gate(
                "API credentials",
                self.readiness.api_credentials_verified,
                "L2 auth keys",
            ),
            gate(
                "Market parameters",
                self.readiness.market_parameters_verified,
                "tick/min/active",
            ),
            gate("Clock", self.readiness.clock_synced, "local drift"),
            gate("Journal", self.readiness.journal_verified, "hash chain"),
            gate("Heartbeat", self.readiness.heartbeat_ready, "dead-man"),
            gate(
                "Startup reconciliation",
                self.readiness.reconciled_after_startup,
                "remote account state",
            ),
        ]
    }

    pub fn readiness_passed_count(&self) -> usize {
        self.readiness_gates()
            .iter()
            .filter(|gate| gate.passed)
            .count()
    }

    pub fn readiness_total_count(&self) -> usize {
        self.readiness_gates().len()
    }

    pub fn live_status_label(&self) -> &'static str {
        if self.live_submission_enabled {
            "LIVE ENABLED"
        } else {
            "LIVE LOCKED"
        }
    }
}

fn gate(name: &'static str, passed: bool, detail: &str) -> GateStatus {
    GateStatus {
        name,
        passed,
        blocking: !passed,
        detail: detail.to_string(),
    }
}

fn wallet_readiness_detail(wallet_mode: WalletMode) -> &'static str {
    match wallet_mode {
        WalletMode::DepositWallet => "deposit deployment",
        WalletMode::GnosisSafe => "Safe deployment/owner",
        WalletMode::Proxy => "proxy deployment/owner",
        WalletMode::Eoa => "EOA signer/funder",
    }
}

pub fn build_startup_report(config_path: &str) -> Result<StartupReport> {
    let generated_at_ms = system_now_ms();
    let settings = Settings::load(config_path)?;
    let protocol_result = validate_protocol_compatibility(&settings);
    let journal = verify_journal(&settings.journal_path)?;

    // The cold startup report never touches authenticated account APIs. The
    // operational runtime performs those proofs after secrets stay server-side.
    let remote = RemoteSnapshot::default();
    let recovery = recover_after_crash(&settings.journal_path, &remote)?;
    let secrets = SecretStatus::from_environment(settings.wallet_mode);
    let readiness = ReadinessState {
        protocol_verified: protocol_result.is_ok(),
        journal_verified: true,
        reconciled_after_startup: recovery.live_unlock_allowed,
        ..ReadinessState::default()
    };
    let compliance = ComplianceState::default();
    let heartbeat = HeartbeatState::default();

    let mut live_lock_reasons = Vec::new();
    if let Err(err) = protocol_result {
        live_lock_reasons.push(err.to_string());
    }
    if settings.mode != BotMode::Live {
        live_lock_reasons.push("configured_mode_is_paper".to_string());
    }
    for missing in secrets.missing_names() {
        live_lock_reasons.push(format!("missing_secret:{missing}"));
    }
    for gate in readiness_gates_for_state(&readiness) {
        if !gate.passed {
            live_lock_reasons.push(format!("readiness:{}", snake(gate.name)));
        }
    }
    match compliance.eligibility_status_at(generated_at_ms, settings.max_geoblock_age_ms) {
        EligibilityStatus::Unverified => {
            live_lock_reasons.push("compliance:geoblock_not_checked".to_string());
        }
        EligibilityStatus::InvalidFuture => {
            live_lock_reasons.push("compliance:geoblock_timestamp_in_future".to_string());
        }
        EligibilityStatus::Stale => {
            live_lock_reasons.push("compliance:geoblock_check_stale".to_string());
        }
        EligibilityStatus::Blocked => {
            live_lock_reasons.push("compliance:geoblocked".to_string());
        }
        EligibilityStatus::VenueReview => {
            live_lock_reasons.push("compliance:venue_not_allowed".to_string());
        }
        EligibilityStatus::Eligible => {}
    }
    if heartbeat.require_healthy().is_err() {
        live_lock_reasons.push("heartbeat:not_live_enabled".to_string());
    }
    if !recovery.live_unlock_allowed {
        live_lock_reasons.push(format!("recovery:{}", recovery.reason));
    }
    if !settings.live_confirmation_valid() {
        live_lock_reasons.push("execution:live_confirmation_not_valid".to_string());
    }
    live_lock_reasons.push("execution:runtime_readiness_proofs_pending".to_string());
    live_lock_reasons.sort();
    live_lock_reasons.dedup();

    let warnings = vec![
        "browser GUI never receives private keys or API secrets".to_string(),
        "international Polymarket order placement must pass geoblock and venue checks before live trading".to_string(),
        "behavioral-pressure logic is a rejection guard and never attempts to move market prices".to_string(),
    ];

    Ok(StartupReport {
        config_path: config_path.to_string(),
        generated_at_ms,
        settings,
        readiness,
        compliance,
        heartbeat,
        recovery,
        journal,
        secrets,
        live_submission_enabled: false,
        live_lock_reasons,
        warnings,
    })
}

fn readiness_gates_for_state(readiness: &ReadinessState) -> Vec<GateStatus> {
    let report = StartupReport {
        config_path: String::new(),
        generated_at_ms: 0,
        settings: Settings::default(),
        readiness: readiness.clone(),
        compliance: ComplianceState::default(),
        heartbeat: HeartbeatState::default(),
        recovery: RecoveryReport {
            live_unlock_allowed: false,
            reason: String::new(),
            replayed_records: 0,
            in_flight_attempts: 0,
        },
        journal: JournalVerification {
            next_sequence: 0,
            last_hash: String::new(),
        },
        secrets: SecretStatus { keys: Vec::new() },
        live_submission_enabled: false,
        live_lock_reasons: Vec::new(),
        warnings: Vec::new(),
    };
    report.readiness_gates()
}

fn system_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn snake(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .split('_')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("_")
}

pub fn require_cli_startup_safe(report: &StartupReport) -> Result<()> {
    if report.settings.mode == BotMode::Live && !report.settings.live_confirmation_valid() {
        return Err(BotError::Readiness(format!(
            "live_mode_locked:{}",
            report.live_lock_reasons.join(",")
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_config(contents: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|value| value.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("polymarket_rs_runtime_{unique}.toml"));
        let journal = std::env::temp_dir().join(format!("polymarket_rs_runtime_{unique}.jsonl"));
        let baseline =
            std::env::temp_dir().join(format!("polymarket_rs_runtime_{unique}_baseline.json"));
        let compliance =
            std::env::temp_dir().join(format!("polymarket_rs_runtime_{unique}_compliance.json"));
        let isolated = format!(
            "{contents}\njournal_path = {:?}\nlive_risk_baseline_path = {:?}\ncompliance_latch_path = {:?}\n",
            journal.to_string_lossy(),
            baseline.to_string_lossy(),
            compliance.to_string_lossy()
        );
        std::fs::write(&path, isolated).unwrap();
        path
    }

    fn valid_config(mode: &str) -> String {
        valid_config_for_wallet(mode, "deposit_wallet", "poly1271")
    }

    fn valid_config_for_wallet(mode: &str, wallet_mode: &str, signature_type: &str) -> String {
        format!(
            r#"
mode = "{mode}"
venue = "polymarket_international"
clob_protocol = "v2"
clob_host = "https://clob.polymarket.com"
chain_id = 137
collateral_asset = "pUSD"
wallet_mode = "{wallet_mode}"
signature_type = "{signature_type}"
funder_address = "0x1111111111111111111111111111111111111111"
geoblock_url = "https://polymarket.com/api/geoblock"
max_geoblock_age_ms = 60000
max_book_age_ms = 250
max_event_lag_ms = 500
max_order_usdc = "10"
max_market_exposure_usdc = "100"
max_daily_loss_usdc = "50"
max_slippage_ticks = 2
max_market_impact_bps = 25
min_top_book_size = "5"
order_ttl_ms = 750
max_new_orders_per_second = 3
max_cancels_per_second = 5
max_position_notional_usdc = "250"
neg_risk_enabled = false
asset_ids = ["1"]
condition_ids = ["0x1111111111111111111111111111111111111111111111111111111111111111"]
controlled_wallets = ["0x1111111111111111111111111111111111111111"]
"#
        )
    }

    #[test]
    fn paper_report_is_cli_safe_but_live_locked() {
        let path = temp_config(&valid_config("paper"));
        let report = build_startup_report(path.to_str().unwrap()).unwrap();
        assert!(require_cli_startup_safe(&report).is_ok());
        assert!(!report.live_submission_enabled);
        assert!(report
            .live_lock_reasons
            .contains(&"configured_mode_is_paper".to_string()));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn live_report_can_render_but_cli_startup_rejects() {
        let path = temp_config(&valid_config("live"));
        let report = build_startup_report(path.to_str().unwrap()).unwrap();
        assert!(require_cli_startup_safe(&report).is_err());
        assert!(report
            .live_lock_reasons
            .iter()
            .any(|reason| reason.contains("geoblock")));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn wallet_readiness_detail_matches_each_supported_v2_path() {
        assert_eq!(
            wallet_readiness_detail(WalletMode::DepositWallet),
            "deposit deployment"
        );
        assert_eq!(
            wallet_readiness_detail(WalletMode::GnosisSafe),
            "Safe deployment/owner"
        );
        assert_eq!(
            wallet_readiness_detail(WalletMode::Proxy),
            "proxy deployment/owner"
        );
        assert_eq!(
            wallet_readiness_detail(WalletMode::Eoa),
            "EOA signer/funder"
        );
    }

    #[test]
    fn runtime_secret_requirements_match_each_supported_wallet_path() {
        for (wallet_mode, signature_type, expects_deposit_secret) in [
            ("deposit_wallet", "poly1271", true),
            ("gnosis_safe", "gnosis_safe", false),
            ("proxy", "proxy", false),
            ("eoa", "eoa", false),
        ] {
            let path = temp_config(&valid_config_for_wallet(
                "paper",
                wallet_mode,
                signature_type,
            ));
            let report = build_startup_report(path.to_str().unwrap()).unwrap();
            let has_deposit_secret = report
                .secrets
                .keys
                .iter()
                .any(|key| key.name == "DEPOSIT_WALLET_ADDRESS");
            let has_deposit_lock = report
                .live_lock_reasons
                .iter()
                .any(|reason| reason == "missing_secret:DEPOSIT_WALLET_ADDRESS");
            assert_eq!(has_deposit_secret, expects_deposit_secret, "{wallet_mode}");
            assert_eq!(
                has_deposit_lock,
                expects_deposit_secret
                    && std::env::var("DEPOSIT_WALLET_ADDRESS")
                        .map(|value| value.trim().is_empty())
                        .unwrap_or(true),
                "{wallet_mode}"
            );
            let _ = std::fs::remove_file(path);
        }
    }
}
