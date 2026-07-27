use crate::compliance::{
    check_compliance, fetch_geoblock_state, ComplianceGuard, ComplianceLatchState, ComplianceState,
    EligibilityStatus,
};
use crate::config::Settings;
use crate::discovery::{discover_configured_markets, MarketCatalog};
use crate::error::{BotError, Result};
use crate::execution::{
    CheckedSubmission, ExecutionAdapter, ExecutionContext, ExecutionResult, ExecutionRouter,
    LiveCancelResult, LiveExecution, LiveRiskSnapshot, PaperExecution,
};
use crate::external_feeds::{run_coinbase_feed, ExternalFeatureCache, ExternalFeedMessage};
use crate::heartbeat::HeartbeatState;
use crate::journal::{Journal, JournalEventKind, JournalRecord, JournalStartup};
use crate::latency::LatencyRecorder;
use crate::market_ws::{apply_market_event, run_market_feed, MarketEvent, MarketFeedMessage};
use crate::matching_engine::MatchingEngineState;
use crate::paper::{PaperEngine, PaperFill, PaperTransition, PAPER_TRANSITION_SCHEMA_VERSION};
use crate::rate_limit::SlidingWindowRateLimiter;
use crate::readiness::{require_live_ready, validate_protocol_compatibility, ReadinessState};
use crate::reconcile::{
    recover_after_crash_records, recover_open_order_mappings_records, unresolved_order_attempts,
    AuthoritativeTrade, RecoveredOpenOrder, RecoveryReport, RemoteSnapshot, UnresolvedOrderAttempt,
};
use crate::signal::{ConservativeMaker, Strategy as _};
use crate::state::{
    ConnectionStatus, MarketRuntimeView, RuntimeEventLevel, RuntimeFill, RuntimePhase,
    RuntimeState, SharedRuntimeState,
};
use crate::types::{AssetId, BookState, BotMode, OrderIntent, Side, TimeInForce};
use crate::user_ws::{
    apply_user_event, order_status_is_terminal, run_user_feed,
    user_event_requires_holdings_reconciliation, OwnOrderCacheState, UserEvent, UserFeedMessage,
    UserWsAuth,
};
use crate::wallet_watch::{
    run_wallet_watch, should_copy_size, WalletTradeObservation, WalletWatchMessage,
};
use async_trait::async_trait;
use chrono::Utc;
use fs2::FileExt as _;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const CHANNEL_CAPACITY_MARKET: usize = 2_048;
const CHANNEL_CAPACITY_COLD: usize = 256;
const PAPER_JOURNAL_COMPACT_RECORDS: u64 = 512;
const PAPER_JOURNAL_COMPACT_BYTES: u64 = 8 * 1024 * 1024;
const LIVE_TRADE_AUDIT_INTERVAL_MS: u64 = 5_000;

struct LiveRuntime {
    router: ExecutionRouter<Box<dyn LiveAdapter>>,
    baseline: DailyRiskBaselineGuard,
    own_orders: OwnOrderCacheState,
    signer_address: String,
    order_clients: BTreeMap<String, RecoveredOpenOrder>,
    safety_latched: bool,
    account_revision: Arc<AtomicU64>,
    processed_account_revision: u64,
    last_account_snapshot: LiveRiskSnapshot,
    pending_account_reconciliation: Option<PendingAccountReconciliation>,
    trade_ledger: BTreeMap<String, TradeLedgerEntry>,
    known_trade_order_ids: BTreeSet<String>,
    owned_order_ids: BTreeSet<String>,
    last_trade_audit_at_ms: u64,
}

#[async_trait]
trait LiveAdapter: ExecutionAdapter + Sync {
    async fn post_heartbeat(&mut self) -> Result<()>;
    async fn cancel_all(&self) -> Result<Vec<String>>;
    async fn cancel_orders_tracked(&self, order_ids: &[String]) -> Result<LiveCancelResult>;
    async fn remote_snapshot(&self) -> Result<RemoteSnapshot>;
    async fn risk_snapshot(
        &self,
        data_api_host: &str,
        funder_address: &str,
        market_asset: &AssetId,
        daily_baseline_equity_usdc: crate::fixed::Fixed,
        prohibited_conduct_flag: bool,
        now_ms: u64,
    ) -> Result<LiveRiskSnapshot>;
}

#[async_trait]
impl LiveAdapter for LiveExecution {
    async fn post_heartbeat(&mut self) -> Result<()> {
        LiveExecution::post_heartbeat(self).await
    }

    async fn cancel_all(&self) -> Result<Vec<String>> {
        LiveExecution::cancel_all(self).await
    }

    async fn cancel_orders_tracked(&self, order_ids: &[String]) -> Result<LiveCancelResult> {
        LiveExecution::cancel_orders_tracked(self, order_ids).await
    }

    async fn remote_snapshot(&self) -> Result<RemoteSnapshot> {
        LiveExecution::remote_snapshot(self).await
    }

    async fn risk_snapshot(
        &self,
        data_api_host: &str,
        funder_address: &str,
        market_asset: &AssetId,
        daily_baseline_equity_usdc: crate::fixed::Fixed,
        prohibited_conduct_flag: bool,
        now_ms: u64,
    ) -> Result<LiveRiskSnapshot> {
        LiveExecution::risk_snapshot(
            self,
            data_api_host,
            funder_address,
            market_asset,
            daily_baseline_equity_usdc,
            prohibited_conduct_flag,
            now_ms,
        )
        .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PendingTradeMutation {
    asset_id: AssetId,
    side: Side,
    price: crate::fixed::Fixed,
    size: crate::fixed::Fixed,
    order_ids: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TradeLedgerEntry {
    mutation: PendingTradeMutation,
    latest_status: String,
    reconciled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AccountMutationJournal {
    schema_version: u32,
    event: UserEvent,
    baseline: LiveRiskSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AccountReconciliationJournal {
    schema_version: u32,
    reconciled_at_ms: u64,
    trade_ids: Vec<String>,
}

const ACCOUNT_MUTATION_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone)]
struct PendingAccountReconciliation {
    baseline: LiveRiskSnapshot,
    trades: BTreeMap<String, PendingTradeMutation>,
    authoritative_order_ids: BTreeSet<String>,
    unresolved_order_ids: BTreeSet<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct AuthoritativePendingTradeProof {
    trade_ids: BTreeSet<String>,
    cash_fee_lower_bound_usdc: crate::fixed::Fixed,
    cash_fee_upper_bound_usdc: crate::fixed::Fixed,
}

struct RecoveredAccountLedger {
    trade_ledger: BTreeMap<String, TradeLedgerEntry>,
    known_trade_order_ids: BTreeSet<String>,
    pending: Option<PendingAccountReconciliation>,
    safety_latched: bool,
}

#[derive(Clone, Copy)]
struct LiveSafetyContext<'a> {
    settings: &'a Settings,
    compliance: &'a ComplianceState,
    heartbeat: &'a HeartbeatState,
    readiness: &'a ReadinessState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DailyRiskBaseline {
    schema_version: u32,
    utc_date: String,
    baseline_equity_usdc: crate::fixed::Fixed,
    written_at_ms: u64,
}

struct DailyRiskBaselineGuard {
    _lock: File,
    path: PathBuf,
    value: DailyRiskBaseline,
}

impl DailyRiskBaselineGuard {
    fn observe_and_rollover(
        &mut self,
        current_equity: crate::fixed::Fixed,
        now_ms: u64,
    ) -> Result<bool> {
        if current_equity <= crate::fixed::Fixed::ZERO {
            return Err(BotError::Readiness(
                "live_equity_must_be_positive".to_string(),
            ));
        }
        let utc_date = utc_date_for_ms(now_ms)?;
        let date_changed = self.value.utc_date != utc_date;
        let high_water = std::cmp::max(self.value.baseline_equity_usdc, current_equity);
        if !date_changed && high_water == self.value.baseline_equity_usdc {
            return Ok(false);
        }
        let fresh = DailyRiskBaseline {
            schema_version: 1,
            utc_date,
            // Never lower the observed equity high-water at a UTC boundary.
            // Resetting to a post-loss snapshot would erase the loss immediately
            // before the order gate. This deliberately remains conservative
            // across days when no trustworthy boundary snapshot exists.
            baseline_equity_usdc: high_water,
            written_at_ms: now_ms,
        };
        write_baseline_atomic(&self.path, &fresh)?;
        self.value = fresh;
        Ok(date_changed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorCommand {
    Pause,
    ResumePaper,
    ResumeLive,
    CancelStale,
    CancelAll,
    FlattenPaper,
    ReduceRisk,
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandOutcome {
    pub action: String,
    pub status: String,
    pub message: String,
    pub affected_orders: usize,
}

pub struct OperatorCommandRequest {
    pub command: OperatorCommand,
    pub response: std_mpsc::Sender<Result<CommandOutcome>>,
}

pub type OperatorCommandSender = mpsc::UnboundedSender<OperatorCommandRequest>;
pub type OperatorCommandReceiver = mpsc::UnboundedReceiver<OperatorCommandRequest>;

pub fn operator_command_channel() -> (OperatorCommandSender, OperatorCommandReceiver) {
    mpsc::unbounded_channel()
}

#[allow(clippy::type_complexity)]
async fn initialize_live_runtime(
    settings: &Settings,
    shared_state: &SharedRuntimeState,
    journal: &mut Journal,
    compliance_latch: &ComplianceLatchState,
) -> Result<(
    LiveRuntime,
    ReadinessState,
    ComplianceState,
    RecoveryReport,
    MarketCatalog,
    HeartbeatState,
    LiveRiskSnapshot,
)> {
    let now_ms = system_now_ms();
    let mut compliance = fetch_geoblock_state(&settings.geoblock_url, now_ms).await?;
    compliance.apply_persistent_latch(compliance_latch);
    check_compliance(settings, &compliance, now_ms)?;
    update_compliance_state(shared_state, &compliance, settings)?;

    let catalog = discover_configured_markets(
        &settings.clob_host,
        settings
            .configured_markets()
            .map(|(asset, condition)| (asset.clone(), condition.clone()))
            .collect::<Vec<_>>(),
    )
    .await?;
    if catalog.is_empty() {
        return Err(BotError::Readiness(
            "configured_market_catalog_empty".to_string(),
        ));
    }
    if catalog.markets().any(|market| market.meta.neg_risk) {
        return Err(BotError::Readiness(
            "negative_risk_live_accounting_not_supported".to_string(),
        ));
    }

    let mut adapter = LiveExecution::from_environment(settings).await?;
    let proof = adapter.verify_account(now_ms).await?;
    if !settings
        .controlled_wallets
        .iter()
        .any(|wallet| wallet.eq_ignore_ascii_case(&proof.signer_address))
    {
        return Err(BotError::Readiness(
            "controlled_wallets_must_declare_verified_signer".to_string(),
        ));
    }
    if proof.closed_only {
        return Err(BotError::Readiness("account_is_closed_only".to_string()));
    }
    if proof.balance_usdc <= crate::fixed::Fixed::ZERO {
        return Err(BotError::Readiness(
            "collateral_balance_not_positive".to_string(),
        ));
    }
    if !proof.allowance_configured {
        return Err(BotError::Readiness(
            "collateral_allowance_not_configured".to_string(),
        ));
    }
    if proof.server_clock_drift_ms > settings.max_clock_drift_ms {
        return Err(BotError::Readiness(format!(
            "server_clock_drift_exceeded:{}ms",
            proof.server_clock_drift_ms
        )));
    }

    let mut records = journal.records_locked()?;
    if records
        .iter()
        .any(|record| record.event_kind == JournalEventKind::PaperTransition)
    {
        return Err(BotError::Readiness(
            "live_mode_requires_a_dedicated_non_paper_journal".to_string(),
        ));
    }
    let unresolved_attempts = unresolved_order_attempts(&records);
    let unresolved_order_ids = unresolved_attempts
        .iter()
        .map(|attempt| attempt.exchange_order_id.clone())
        .collect::<Vec<_>>();
    let (remote, order_statuses) = tokio::join!(
        adapter.remote_snapshot(),
        adapter.order_statuses(&unresolved_order_ids)
    );
    let remote = remote?;
    let order_statuses = order_statuses?;
    if journal_authoritative_terminal_orders(
        journal,
        &unresolved_attempts,
        &order_statuses,
        &remote,
        now_ms,
    )? > 0
    {
        records = journal.records_locked()?;
    }
    let recovery = recover_after_crash_records(&records, &remote)?;
    if !recovery.live_unlock_allowed {
        return Err(BotError::Readiness(format!(
            "startup_reconciliation:{}",
            recovery.reason
        )));
    }
    let recovered_orders = recover_open_order_mappings_records(&records, &remote.open_order_ids)?;
    let mut owned_order_ids = journal_owned_order_ids(&records);

    let configured_asset = settings
        .asset_ids
        .first()
        .ok_or_else(|| BotError::Config("missing_asset_ids".to_string()))?;
    let initial_risk = adapter
        .risk_snapshot(
            &settings.data_api_host,
            &settings.funder_address,
            configured_asset,
            crate::fixed::Fixed::ZERO,
            compliance.prohibited_conduct_flag || compliance.confidential_info_flag,
            now_ms,
        )
        .await?;
    let baseline = open_daily_risk_baseline(
        &settings.live_risk_baseline_path,
        initial_risk.current_equity_usdc,
        now_ms,
    )?;
    let mut risk = initial_risk;
    risk.risk_state.daily_loss_usdc = daily_loss(
        baseline.value.baseline_equity_usdc,
        risk.current_equity_usdc,
    )?;
    let mut recovered_account = recover_account_ledger(&records)?;
    owned_order_ids.extend(recovered_account.known_trade_order_ids.iter().cloned());
    let mut startup_trade_order_ids = owned_order_ids.clone();
    if let Some(pending) = recovered_account.pending.as_ref() {
        startup_trade_order_ids.extend(pending.unresolved_order_ids.iter().cloned());
    }
    let startup_trade_baseline = recovered_account
        .pending
        .as_ref()
        .map_or_else(|| risk.clone(), |pending| pending.baseline.clone());
    let startup_trade_ids = append_startup_authoritative_trades(
        journal,
        &startup_trade_baseline,
        &startup_trade_order_ids,
        &recovered_account.trade_ledger,
        &remote,
    )?;
    if !startup_trade_ids.is_empty() {
        records = journal.records_locked()?;
        recovered_account = recover_account_ledger(&records)?;
    }
    if recovered_account.safety_latched {
        return Err(BotError::Readiness(
            "account_trade_failed_or_unknown_requires_manual_reconciliation".to_string(),
        ));
    }

    let readiness = ReadinessState {
        protocol_verified: true,
        wallet_path_verified: true,
        signer_authorized: true,
        funder_verified: true,
        balance_verified: true,
        allowance_verified: true,
        api_credentials_verified: true,
        market_parameters_verified: true,
        clock_synced: true,
        journal_verified: true,
        heartbeat_ready: true,
        reconciled_after_startup: true,
    };
    require_live_ready(settings, &readiness)?;
    let heartbeat = HeartbeatState {
        live_enabled: true,
        consecutive_failures: 0,
        max_failures: 3,
        degraded: false,
    };
    let mut own_orders = OwnOrderCacheState::default();
    own_orders.apply_authoritative_snapshot(remote.open_order_ids.clone());
    let mut runtime = LiveRuntime {
        router: ExecutionRouter::new(Box::new(adapter) as Box<dyn LiveAdapter>),
        baseline,
        own_orders,
        signer_address: proof.signer_address,
        order_clients: recovered_orders,
        safety_latched: false,
        account_revision: Arc::new(AtomicU64::new(0)),
        processed_account_revision: 0,
        last_account_snapshot: risk.clone(),
        pending_account_reconciliation: recovered_account.pending,
        trade_ledger: recovered_account.trade_ledger,
        known_trade_order_ids: recovered_account.known_trade_order_ids,
        owned_order_ids,
        last_trade_audit_at_ms: now_ms,
    };
    if let Some(pending) = &runtime.pending_account_reconciliation {
        let authoritative_trade_proof = authoritative_pending_trade_proof(pending, &remote)?;
        if !pending_account_reconciled(pending, &risk, &authoritative_trade_proof)? {
            return Err(BotError::Readiness(
                "startup_account_mutations_not_reflected_in_data_api".to_string(),
            ));
        }
        commit_account_reconciliation(&mut runtime, journal, now_ms)?;
    }
    Ok((
        runtime, readiness, compliance, recovery, catalog, heartbeat, risk,
    ))
}

pub async fn run_operational_runtime(
    settings: Settings,
    config_path: String,
    shared_state: SharedRuntimeState,
    mut commands: OperatorCommandReceiver,
    shutdown: CancellationToken,
) -> Result<()> {
    validate_protocol_compatibility(&settings)?;
    let config_hash = hash_file(&config_path)?;
    let binary_hash = hash_current_binary()?;
    let mut journal = Journal::open(
        &settings.journal_path,
        JournalStartup::new(config_hash, binary_hash),
    )?;
    let mut compliance_guard = ComplianceGuard::open(
        &settings.compliance_latch_path,
        &settings.controlled_wallets,
        system_now_ms(),
    )?;
    update_state(&shared_state, |state| {
        state.journal_next_sequence = journal.next_sequence();
        state.journal_last_hash = journal.last_hash().to_string();
        state.push_event(
            system_now_ms(),
            RuntimeEventLevel::Info,
            "runtime",
            "durable journal opened, exclusively locked, and verified",
        );
    })?;
    let mut books = BTreeMap::<AssetId, BookState>::new();
    let mut readiness = ReadinessState {
        protocol_verified: true,
        journal_verified: true,
        ..ReadinessState::default()
    };
    let mut compliance = ComplianceState::default();
    compliance.apply_persistent_latch(compliance_guard.state());
    let mut recovery = RecoveryReport {
        live_unlock_allowed: false,
        reason: "paper_mode_does_not_require_remote_recovery".to_string(),
        replayed_records: 0,
        in_flight_attempts: 0,
    };
    let mut heartbeat = HeartbeatState::default();
    let mut catalog: Option<MarketCatalog> = None;
    let mut live_runtime = None;

    if settings.mode == BotMode::Live {
        match initialize_live_runtime(
            &settings,
            &shared_state,
            &mut journal,
            compliance_guard.state(),
        )
        .await
        {
            Ok((runtime, ready, eligible, recovered, verified_catalog, heartbeat_state, risk)) => {
                readiness = ready;
                compliance = eligible;
                recovery = recovered;
                heartbeat = heartbeat_state;
                install_catalog(&verified_catalog, &mut books, &shared_state)?;
                catalog = Some(verified_catalog);
                update_state(&shared_state, |state| {
                    state.live_submission_enabled = false;
                    state.live_account = Some(risk);
                    state.phase = RuntimePhase::Paused;
                    state.push_event(
                        system_now_ms(),
                        RuntimeEventLevel::Info,
                        "execution",
                        "live account authenticated; awaiting user-stream resynchronization",
                    );
                })?;
                live_runtime = Some(runtime);
            }
            Err(error) => {
                update_state(&shared_state, |state| {
                    state.live_submission_enabled = false;
                    state.phase = RuntimePhase::Degraded;
                    state.user_feed.degraded("live_startup_failed");
                    state.push_event(
                        system_now_ms(),
                        RuntimeEventLevel::Error,
                        "live_startup",
                        error.to_string(),
                    );
                })?;
            }
        }
    }

    let mut paper_router = if settings.mode == BotMode::Paper {
        let engine = recover_paper_engine(&settings, &mut journal)?;
        Some(ExecutionRouter::new(PaperExecution::new(engine)))
    } else {
        None
    };

    let matching = MatchingEngineState::normal_after_verified_startup();
    update_state(&shared_state, |state| {
        state.readiness = readiness_flags(&readiness);
    })?;
    let mut external_cache = ExternalFeatureCache::default();
    let mut latency = LatencyRecorder::default();
    let mut new_order_limiter =
        SlidingWindowRateLimiter::per_second(settings.max_new_orders_per_second);
    let mut cancel_limiter = SlidingWindowRateLimiter::per_second(settings.max_cancels_per_second);
    let mut repairing_books = BTreeSet::<AssetId>::new();

    let (market_tx, mut market_rx) = mpsc::channel(CHANNEL_CAPACITY_MARKET);
    let (user_tx, mut user_rx) = mpsc::channel(CHANNEL_CAPACITY_COLD);
    let (external_tx, mut external_rx) = mpsc::channel(CHANNEL_CAPACITY_COLD);
    let (wallet_tx, mut wallet_rx) = mpsc::channel(CHANNEL_CAPACITY_COLD);
    let (compliance_tx, mut compliance_rx) = mpsc::channel(CHANNEL_CAPACITY_COLD);
    let (catalog_tx, mut catalog_rx) = mpsc::channel(CHANNEL_CAPACITY_COLD);
    let mut tasks = Vec::<JoinHandle<()>>::new();

    tasks.push(spawn_market_task(
        &settings,
        market_tx.clone(),
        shutdown.child_token(),
    ));
    if let Some(runtime) = &live_runtime {
        match spawn_user_task(
            &settings,
            runtime.signer_address.clone(),
            Arc::clone(&runtime.account_revision),
            user_tx.clone(),
            shutdown.child_token(),
        ) {
            Ok(task) => tasks.push(task),
            Err(error) => {
                update_state(&shared_state, |state| {
                    state.live_submission_enabled = false;
                    state.user_feed.degraded(error.to_string());
                    state.push_event(
                        system_now_ms(),
                        RuntimeEventLevel::Error,
                        "user_ws",
                        error.to_string(),
                    );
                })?;
                live_runtime = None;
            }
        }
    }
    if settings.enable_external_signal && !settings.external_symbols.is_empty() {
        tasks.push(spawn_external_task(
            &settings,
            external_tx.clone(),
            shutdown.child_token(),
        ));
    }
    if !settings.watched_wallets.is_empty() {
        tasks.push(spawn_wallet_task(
            &settings,
            wallet_tx.clone(),
            shutdown.child_token(),
        ));
    }
    tasks.push(spawn_compliance_task(
        settings.geoblock_url.clone(),
        settings.max_geoblock_age_ms,
        compliance_tx,
        shutdown.child_token(),
    ));
    tasks.push(spawn_catalog_task(
        settings.clob_host.clone(),
        settings
            .configured_markets()
            .map(|(asset, condition)| (asset.clone(), condition.clone()))
            .collect(),
        catalog_tx,
        shutdown.child_token(),
    ));

    let mut maintenance = interval(Duration::from_millis(settings.snapshot_interval_ms));
    maintenance.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut heartbeat_tick = interval(Duration::from_millis(settings.heartbeat_interval_ms));
    heartbeat_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let run_result: Result<()> = async {
        loop {
            tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            Some(request) = commands.recv() => {
                let outcome = handle_operator_command(
                    request.command,
                    &settings,
                    &shared_state,
                    &mut paper_router,
                    &mut live_runtime,
                    &mut books,
                    &mut journal,
                    &mut cancel_limiter,
                ).await;
                let _ = request.response.send(outcome);
            }
            Some(message) = market_rx.recv() => {
                let started = Instant::now();
                handle_market_message(
                    message,
                    &settings,
                    &shared_state,
                    &mut repairing_books,
                    &mut books,
                    catalog.as_ref(),
                    &mut paper_router,
                    &mut live_runtime,
                    &mut journal,
                    &readiness,
                    &compliance,
                    &heartbeat,
                    &recovery,
                    &matching,
                    &external_cache,
                    &mut new_order_limiter,
                    &mut latency,
                ).await?;
                latency.record("market_event", elapsed_us(started));
            }
            Some(message) = external_rx.recv() => {
                handle_external_message(message, &shared_state, &mut external_cache)?;
            }
            Some(message) = user_rx.recv() => {
                handle_user_message(
                    message,
                    &shared_state,
                    &mut live_runtime,
                    &mut journal,
                    LiveSafetyContext {
                        settings: &settings,
                        compliance: &compliance,
                        heartbeat: &heartbeat,
                        readiness: &readiness,
                    },
                ).await?;
            }
            Some(message) = wallet_rx.recv() => {
                handle_wallet_message(
                    message,
                    &settings,
                    &shared_state,
                    &books,
                    catalog.as_ref(),
                    &mut paper_router,
                    &mut live_runtime,
                    &mut journal,
                    &readiness,
                    &mut compliance,
                    &mut compliance_guard,
                    &heartbeat,
                    &recovery,
                    &matching,
                    &mut new_order_limiter,
                    &mut latency,
                ).await?;
            }
            Some(result) = compliance_rx.recv() => {
                match result {
                    Ok(mut value) => {
                        value.apply_persistent_latch(compliance_guard.state());
                        compliance = value;
                        update_compliance_state(&shared_state, &compliance, &settings)?;
                        enforce_live_compliance(
                            &settings,
                            &compliance,
                            &shared_state,
                            &mut live_runtime,
                            &heartbeat,
                            &mut journal,
                        ).await?;
                    }
                    Err(error) => {
                        compliance = ComplianceState::default();
                        compliance.apply_persistent_latch(compliance_guard.state());
                        update_state(&shared_state, |state| {
                            state.compliance_status = "unverified".to_string();
                            state.push_event(system_now_ms(), RuntimeEventLevel::Warning, "compliance", error.to_string());
                        })?;
                        enforce_live_compliance(
                            &settings,
                            &compliance,
                            &shared_state,
                            &mut live_runtime,
                            &heartbeat,
                            &mut journal,
                        ).await?;
                    }
                }
            }
            Some(result) = catalog_rx.recv() => {
                match result {
                    Ok(value) => {
                        install_catalog(&value, &mut books, &shared_state)?;
                        readiness.market_parameters_verified = true;
                        update_state(&shared_state, |state| {
                            state.readiness = readiness_flags(&readiness);
                        })?;
                        catalog = Some(value);
                    }
                    Err(error) => {
                        readiness.market_parameters_verified = false;
                        update_state(&shared_state, |state| {
                            state.readiness = readiness_flags(&readiness);
                        })?;
                        update_state(&shared_state, |state| {
                            state.catalog_ready = false;
                            state.phase = RuntimePhase::Degraded;
                            state.push_event(system_now_ms(), RuntimeEventLevel::Warning, "discovery", error.to_string());
                        })?;
                        if settings.mode == BotMode::Live {
                            pause_and_cancel_live(
                                &shared_state,
                                &mut live_runtime,
                                &mut journal,
                                "market_catalog_refresh_failed",
                            ).await?;
                        }
                    }
                }
            }
            _ = maintenance.tick() => {
                maintain_runtime(
                    &settings,
                    &shared_state,
                    &mut books,
                    &mut paper_router,
                    &mut journal,
                    &mut latency,
                )?;
                maintain_live_order_ttls(
                    &settings,
                    &shared_state,
                    &mut live_runtime,
                    &mut journal,
                    &mut cancel_limiter,
                ).await?;
                maintain_live_account_reconciliation(
                    &settings,
                    &shared_state,
                    &mut live_runtime,
                    &mut journal,
                    &readiness,
                    &compliance,
                    &heartbeat,
                ).await?;
            }
            _ = heartbeat_tick.tick(), if live_runtime.is_some() => {
                maintain_live_heartbeat(
                    &shared_state,
                    &mut live_runtime,
                    &mut heartbeat,
                    &mut journal,
                ).await?;
            }
            }
        }
        Ok(())
    }
    .await;

    let _ = update_state(&shared_state, |state| {
        state.phase = RuntimePhase::ShuttingDown;
        state.strategy_enabled = false;
        state.push_event(
            system_now_ms(),
            RuntimeEventLevel::Info,
            "runtime",
            "shutdown requested; cancelling feed tasks",
        );
    });
    shutdown.cancel();
    let mut cleanup_error = None;
    if let Some(router) = &mut paper_router {
        let now_ms = system_now_ms();
        let ids = router.adapter().engine().open_order_ids();
        if !ids.is_empty() {
            let mut staged = router.adapter().engine().clone();
            staged.cancel_all(now_ms, "runtime_shutdown");
            match commit_paper_transition(
                &mut journal,
                router.adapter_mut().engine_mut(),
                staged,
                &[],
                &ids,
                "runtime_shutdown",
                now_ms,
            ) {
                Ok(()) => {}
                Err(error) => cleanup_error = Some(error),
            }
        }
    }
    if let Some(runtime) = &mut live_runtime {
        let ids = runtime.order_clients.keys().cloned().collect::<Vec<_>>();
        match cancel_tracked_live_orders(runtime, &mut journal, ids, "runtime_shutdown").await {
            Ok(_) => {}
            Err(error) => {
                let detail = error.to_string();
                let _ = update_state(&shared_state, |state| {
                    state.push_event(
                        system_now_ms(),
                        RuntimeEventLevel::Error,
                        "shutdown_cancel",
                        detail,
                    );
                });
                if cleanup_error.is_none() {
                    cleanup_error = Some(error);
                }
            }
        }
    }
    for task in tasks {
        task.abort();
    }
    if let Err(error) = journal.write_checkpoint() {
        if cleanup_error.is_none() {
            cleanup_error = Some(error);
        }
    }
    if let Err(error) = update_state(&shared_state, |state| {
        state.phase = RuntimePhase::Stopped;
        state.market_feed.status = ConnectionStatus::Stopped;
        if state.user_feed.status != ConnectionStatus::Disabled {
            state.user_feed.status = ConnectionStatus::Stopped;
        }
        if state.external_feed.status != ConnectionStatus::Disabled {
            state.external_feed.status = ConnectionStatus::Stopped;
        }
        if state.wallet_feed.status != ConnectionStatus::Disabled {
            state.wallet_feed.status = ConnectionStatus::Stopped;
        }
        state.touch(system_now_ms());
    }) {
        if cleanup_error.is_none() {
            cleanup_error = Some(error);
        }
    }
    match run_result {
        Err(error) => Err(error),
        Ok(()) => cleanup_error.map_or(Ok(()), Err),
    }
}

#[allow(clippy::too_many_arguments)]
async fn cancel_orders_for_market_assets(
    asset_ids: &BTreeSet<AssetId>,
    reason: &str,
    now_ms: u64,
    shared_state: &SharedRuntimeState,
    paper_router: &mut Option<ExecutionRouter<PaperExecution>>,
    live_runtime: &mut Option<LiveRuntime>,
    journal: &mut Journal,
) -> Result<()> {
    if let Some(router) = paper_router.as_mut() {
        for asset_id in asset_ids {
            let mut staged = router.adapter().engine().clone();
            let cancelled = staged.cancel_asset(asset_id, now_ms, reason);
            if !cancelled.is_empty() {
                commit_paper_transition(
                    journal,
                    router.adapter_mut().engine_mut(),
                    staged,
                    &[],
                    &cancelled,
                    reason,
                    now_ms,
                )?;
            }
        }
    }
    if let Some(runtime) = live_runtime.as_mut() {
        for asset_id in asset_ids {
            let ids = runtime
                .order_clients
                .iter()
                .filter(|(_, tracked)| tracked.asset_id == *asset_id)
                .map(|(exchange_id, _)| exchange_id.clone())
                .collect::<Vec<_>>();
            if ids.is_empty() {
                continue;
            }
            if let Err(error) = cancel_tracked_live_orders(runtime, journal, ids, reason).await {
                latch_cancel_uncertainty(runtime, journal, shared_state, now_ms, reason, &error)
                    .await?;
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_market_message(
    message: MarketFeedMessage,
    settings: &Settings,
    shared_state: &SharedRuntimeState,
    repairing_books: &mut BTreeSet<AssetId>,
    books: &mut BTreeMap<AssetId, BookState>,
    catalog: Option<&MarketCatalog>,
    paper_router: &mut Option<ExecutionRouter<PaperExecution>>,
    live_runtime: &mut Option<LiveRuntime>,
    journal: &mut Journal,
    readiness: &ReadinessState,
    compliance: &ComplianceState,
    heartbeat: &HeartbeatState,
    recovery: &RecoveryReport,
    matching: &MatchingEngineState,
    external_cache: &ExternalFeatureCache,
    new_order_limiter: &mut SlidingWindowRateLimiter,
    latency: &mut LatencyRecorder,
) -> Result<()> {
    let now_ms = system_now_ms();
    match message {
        MarketFeedMessage::Connected => {
            update_state(shared_state, |state| {
                state.market_feed.connected(now_ms);
                if state.paused {
                    state.phase = RuntimePhase::Paused;
                }
                state.push_event(
                    now_ms,
                    RuntimeEventLevel::Info,
                    "market_ws",
                    "single ordered Polymarket market stream connected",
                );
            })?;
        }
        MarketFeedMessage::Disconnected(error) => {
            for book in books.values_mut() {
                book.tradeable = false;
            }
            update_state(shared_state, |state| {
                state.market_feed.degraded(error.clone());
                state.phase = RuntimePhase::Degraded;
                state.push_event(now_ms, RuntimeEventLevel::Warning, "market_ws", error);
            })?;
            if settings.mode == BotMode::Live {
                pause_and_cancel_live(
                    shared_state,
                    live_runtime,
                    journal,
                    "market_feed_disconnected",
                )
                .await?;
            }
        }
        MarketFeedMessage::Events { events, processed } => {
            let mut batch_invalid_assets = BTreeSet::new();
            for event in events {
                update_state(shared_state, |state| state.market_feed.message(now_ms))?;
                let mut event_assets = event_asset_ids(&event);
                let resolved_assets = match &event {
                    MarketEvent::MarketResolved { asset_ids, .. } => {
                        asset_ids.iter().cloned().collect::<BTreeSet<_>>()
                    }
                    _ => BTreeSet::new(),
                };
                let mut invalid_assets = BTreeSet::new();
                let mut updated_books = Vec::new();
                for book in books.values_mut() {
                    let affected = event_assets.contains(&book.asset_id);
                    match apply_market_event(book, &event, now_ms) {
                        Ok(()) => {}
                        Err(error) => {
                            if affected {
                                book.tradeable = false;
                                invalid_assets.insert(book.asset_id.clone());
                            }
                            update_state(shared_state, |state| {
                                state.push_event(
                                    now_ms,
                                    RuntimeEventLevel::Warning,
                                    "book",
                                    format!("{}:{error}", book.asset_id),
                                );
                            })?;
                        }
                    }
                    if affected
                        && matches!(&event, MarketEvent::TickSizeChange { .. })
                        && !book.tradeable
                    {
                        invalid_assets.insert(book.asset_id.clone());
                    }
                    if affected
                        && matches!(&event, MarketEvent::Book { .. })
                        && book.tradeable
                        && book.book_hash.is_some()
                    {
                        repairing_books.remove(&book.asset_id);
                        batch_invalid_assets.remove(&book.asset_id);
                    }
                    updated_books.push(book.clone());
                }
                update_state(shared_state, |state| {
                    for book in updated_books {
                        if let Some(view) = state.markets.get_mut(&book.asset_id) {
                            view.book = book;
                        }
                    }
                    state.touch(now_ms);
                })?;

                if !resolved_assets.is_empty() {
                    for asset_id in &resolved_assets {
                        repairing_books.remove(asset_id);
                        batch_invalid_assets.remove(asset_id);
                    }
                    cancel_orders_for_market_assets(
                        &resolved_assets,
                        "market_resolved",
                        now_ms,
                        shared_state,
                        paper_router,
                        live_runtime,
                        journal,
                    )
                    .await?;
                    event_assets.retain(|asset_id| !resolved_assets.contains(asset_id));
                }

                if !invalid_assets.is_empty() {
                    event_assets.retain(|asset_id| !invalid_assets.contains(asset_id));
                    cancel_orders_for_market_assets(
                        &invalid_assets,
                        "market_book_invalidated",
                        now_ms,
                        shared_state,
                        paper_router,
                        live_runtime,
                        journal,
                    )
                    .await?;
                    for asset_id in invalid_assets {
                        batch_invalid_assets.insert(asset_id.clone());
                        if repairing_books.insert(asset_id.clone()) {
                            update_state(shared_state, |state| {
                                state.push_event(
                                now_ms,
                                RuntimeEventLevel::Warning,
                                "book_repair",
                                format!(
                                    "{} locked; cancelling affected orders and forcing an authoritative resubscription",
                                    asset_id
                                ),
                            );
                            })?;
                        }
                    }
                }

                if let Some(router) = paper_router.as_mut() {
                    for asset_id in &event_assets {
                        let Some(book) = books.get(asset_id).cloned() else {
                            continue;
                        };
                        let current_engine = router.adapter().engine().clone();
                        let mut staged_engine = current_engine.clone();
                        let (fills, cancellations) = match &event {
                            MarketEvent::LastTradePrice {
                                price,
                                size,
                                side,
                                timestamp_ms,
                                ..
                            } if book.last_trade_timestamp_ms == Some(*timestamp_ms) => (
                                staged_engine.process_trade(
                                    asset_id,
                                    *price,
                                    *size,
                                    *side,
                                    *timestamp_ms,
                                    now_ms,
                                )?,
                                Vec::new(),
                            ),
                            MarketEvent::Book { .. } | MarketEvent::PriceChange { .. } => {
                                staged_engine.process_book_with_cancellations(&book, now_ms)?
                            }
                            _ => (Vec::new(), Vec::new()),
                        };
                        if staged_engine != current_engine {
                            commit_paper_transition(
                                journal,
                                router.adapter_mut().engine_mut(),
                                staged_engine,
                                &fills,
                                &cancellations,
                                "paper_market_event",
                                now_ms,
                            )?;
                            publish_paper_fills(shared_state, &fills)?;
                        }
                        let paused = read_state(shared_state, |state| state.paused)?;
                        if !paused
                            && !router
                                .adapter()
                                .engine()
                                .has_open_order(asset_id, "conservative-maker")
                            && external_gate_ready(settings, external_cache, now_ms)
                        {
                            if let Some(market) =
                                catalog.and_then(|value| value.by_asset_id(asset_id))
                            {
                                let mut strategy = ConservativeMaker {
                                    strategy_id: "conservative-maker".to_string(),
                                    size: settings.strategy_size,
                                    min_spread: settings.min_spread,
                                    ttl_ms: settings.order_ttl_ms,
                                };
                                if let Some(intent) = strategy.on_book(&book, now_ms) {
                                    submit_paper_intent(
                                        router,
                                        journal,
                                        intent,
                                        settings,
                                        market,
                                        &book,
                                        books,
                                        readiness,
                                        compliance,
                                        heartbeat,
                                        recovery,
                                        matching,
                                        new_order_limiter,
                                        shared_state,
                                        latency,
                                    )
                                    .await?;
                                }
                            }
                        }
                    }
                }
                if let Some(runtime) = live_runtime.as_mut() {
                    let paused = read_state(shared_state, |state| state.paused)?;
                    if !paused
                        && matches!(
                            event,
                            MarketEvent::Book { .. } | MarketEvent::PriceChange { .. }
                        )
                        && external_gate_ready(settings, external_cache, now_ms)
                    {
                        for asset_id in &event_assets {
                            let Some(book) = books.get(asset_id) else {
                                continue;
                            };
                            let Some(market) =
                                catalog.and_then(|value| value.by_asset_id(asset_id))
                            else {
                                continue;
                            };
                            let mut strategy = ConservativeMaker {
                                strategy_id: "conservative-maker".to_string(),
                                size: settings.strategy_size,
                                min_spread: settings.min_spread,
                                ttl_ms: settings.order_ttl_ms,
                            };
                            if let Some(intent) = strategy.on_book(book, now_ms) {
                                submit_live_intent(
                                    runtime,
                                    journal,
                                    intent,
                                    settings,
                                    market,
                                    book,
                                    readiness,
                                    compliance,
                                    heartbeat,
                                    recovery,
                                    matching,
                                    new_order_limiter,
                                    shared_state,
                                    latency,
                                )
                                .await?;
                            }
                        }
                    }
                }
            }
            let unresolved = batch_invalid_assets
                .into_iter()
                .filter(|asset_id| repairing_books.contains(asset_id))
                .collect();
            let _ = processed.send(unresolved);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_wallet_message(
    message: WalletWatchMessage,
    settings: &Settings,
    shared_state: &SharedRuntimeState,
    books: &BTreeMap<AssetId, BookState>,
    catalog: Option<&MarketCatalog>,
    paper_router: &mut Option<ExecutionRouter<PaperExecution>>,
    live_runtime: &mut Option<LiveRuntime>,
    journal: &mut Journal,
    readiness: &ReadinessState,
    compliance: &mut ComplianceState,
    compliance_guard: &mut ComplianceGuard,
    heartbeat: &HeartbeatState,
    recovery: &RecoveryReport,
    matching: &MatchingEngineState,
    new_order_limiter: &mut SlidingWindowRateLimiter,
    latency: &mut LatencyRecorder,
) -> Result<()> {
    let now_ms = system_now_ms();
    match message {
        WalletWatchMessage::Connected => update_state(shared_state, |state| {
            state.wallet_feed.connected(now_ms);
            state.push_event(
                now_ms,
                RuntimeEventLevel::Info,
                "wallet_watch",
                "wallet observer polling successfully",
            );
        })?,
        WalletWatchMessage::Warmed {
            wallet,
            historical_trades,
        } => update_state(shared_state, |state| {
            state.wallet_feed.message(now_ms);
            state.push_event(
                now_ms,
                RuntimeEventLevel::Info,
                "wallet_watch",
                format!(
                    "warmed {wallet} with {historical_trades} historical trades; none replayed"
                ),
            );
        })?,
        WalletWatchMessage::Disconnected(error) => update_state(shared_state, |state| {
            state.wallet_feed.degraded(error.clone());
            state.push_event(now_ms, RuntimeEventLevel::Warning, "wallet_watch", error);
        })?,
        WalletWatchMessage::Trade(trade) => {
            update_state(shared_state, |state| {
                state.wallet_feed.message(now_ms);
                state.push_wallet_trade(trade.clone());
                state.touch(now_ms);
            })?;
            let policy = settings
                .wallet_scores
                .iter()
                .find(|policy| policy.address.eq_ignore_ascii_case(&trade.wallet));
            if compliance_guard.controls_wallet(&trade.wallet) {
                compliance_guard.latch_prohibited(
                    format!("controlled_wallet_observed_in_copy_feed:{}", trade.wallet),
                    now_ms,
                )?;
                compliance.apply_persistent_latch(compliance_guard.state());
                update_compliance_state(shared_state, compliance, settings)?;
                pause_and_cancel_live(
                    shared_state,
                    live_runtime,
                    journal,
                    "controlled_wallet_copy_conflict",
                )
                .await?;
                cancel_all_paper_for_compliance(
                    paper_router,
                    journal,
                    "controlled_wallet_copy_conflict",
                    now_ms,
                )?;
                update_state(shared_state, |state| {
                    state.push_event(
                        now_ms,
                        RuntimeEventLevel::Error,
                        "compliance_latch",
                        "operator-controlled wallet appeared in the copy feed; persistent prohibited-conduct latch set",
                    );
                })?;
                return Ok(());
            }
            if policy.is_some_and(|policy| policy.confidential_info_flag) {
                compliance_guard.latch_confidential(
                    format!("confidential_wallet_signal:{}", trade.wallet),
                    now_ms,
                )?;
                compliance.apply_persistent_latch(compliance_guard.state());
                update_compliance_state(shared_state, compliance, settings)?;
                pause_and_cancel_live(
                    shared_state,
                    live_runtime,
                    journal,
                    "confidential_wallet_signal",
                )
                .await?;
                cancel_all_paper_for_compliance(
                    paper_router,
                    journal,
                    "confidential_wallet_signal",
                    now_ms,
                )?;
                update_state(shared_state, |state| {
                    state.push_event(
                        now_ms,
                        RuntimeEventLevel::Error,
                        "compliance_latch",
                        "wallet policy marked this signal confidential; persistent trading latch set",
                    );
                })?;
                return Ok(());
            }
            if !settings.enable_wallet_copying
                || read_state(shared_state, |state| state.paused)?
                || now_ms
                    .checked_sub(trade.timestamp_ms)
                    .is_none_or(|age| age > settings.wallet_signal_max_age_ms)
            {
                return Ok(());
            }
            let Some(policy) = policy else {
                return Ok(());
            };
            let observed_asset = AssetId::from(trade.asset_id.clone());
            let Some(observed_market) =
                catalog.and_then(|value| value.by_asset_id(&observed_asset))
            else {
                return Ok(());
            };
            if !observed_market
                .condition_id
                .as_ref()
                .eq_ignore_ascii_case(&trade.condition_id)
            {
                return Ok(());
            }
            let Some(book) = books.get(&observed_asset) else {
                return Ok(());
            };
            let current_price = book.last_trade_price.or_else(|| midpoint(book));
            let Some(current_price) = current_price else {
                return Ok(());
            };
            let copy_size = std::cmp::min(trade.size, policy.max_copy_size);
            if !should_copy_size(policy, trade.price, current_price, copy_size) {
                return Ok(());
            }
            let Some(intent) =
                wallet_copy_intent(&trade, book, copy_size, now_ms, settings.order_ttl_ms)
            else {
                return Ok(());
            };
            if let Some(router) = paper_router.as_mut() {
                submit_paper_intent(
                    router,
                    journal,
                    intent,
                    settings,
                    observed_market,
                    book,
                    books,
                    readiness,
                    compliance,
                    heartbeat,
                    recovery,
                    matching,
                    new_order_limiter,
                    shared_state,
                    latency,
                )
                .await?;
            } else if let Some(runtime) = live_runtime.as_mut() {
                submit_live_intent(
                    runtime,
                    journal,
                    intent,
                    settings,
                    observed_market,
                    book,
                    readiness,
                    compliance,
                    heartbeat,
                    recovery,
                    matching,
                    new_order_limiter,
                    shared_state,
                    latency,
                )
                .await?;
            }
        }
    }
    Ok(())
}

fn handle_external_message(
    message: ExternalFeedMessage,
    shared_state: &SharedRuntimeState,
    cache: &mut ExternalFeatureCache,
) -> Result<()> {
    let now_ms = system_now_ms();
    match message {
        ExternalFeedMessage::Connected => update_state(shared_state, |state| {
            state.external_feed.connected(now_ms);
            state.push_event(
                now_ms,
                RuntimeEventLevel::Info,
                "coinbase",
                "public Advanced Trade ticker connected",
            );
        }),
        ExternalFeedMessage::Disconnected(error) => update_state(shared_state, |state| {
            state.external_feed.degraded(error.clone());
            state.push_event(now_ms, RuntimeEventLevel::Warning, "coinbase", error);
        }),
        ExternalFeedMessage::Quote(quote) => {
            cache.update(quote.clone());
            update_state(shared_state, |state| {
                state.external_feed.message(now_ms);
                state.external_quotes.insert(quote.symbol.clone(), quote);
                state.touch(now_ms);
            })
        }
    }
}

async fn handle_user_message(
    message: UserFeedMessage,
    shared_state: &SharedRuntimeState,
    live_runtime: &mut Option<LiveRuntime>,
    journal: &mut Journal,
    safety: LiveSafetyContext<'_>,
) -> Result<()> {
    let now_ms = system_now_ms();
    let Some(runtime) = live_runtime.as_mut() else {
        return Ok(());
    };
    match message {
        UserFeedMessage::Connected { revision } => {
            let snapshot = match runtime.router.adapter().remote_snapshot().await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    fail_user_reconnect(runtime, journal, shared_state, now_ms, revision, error)
                        .await?;
                    return Ok(());
                }
            };
            let validation = if runtime.account_revision.load(Ordering::Acquire) != revision {
                Err(BotError::Readiness(
                    "user_ws_resnapshot_account_changed".to_string(),
                ))
            } else {
                validate_reconnect_snapshot(&runtime.order_clients, &snapshot)
            };
            if let Err(error) = validation {
                fail_user_reconnect(runtime, journal, shared_state, now_ms, revision, error)
                    .await?;
                return Ok(());
            }
            let recovered_trades =
                match ingest_authoritative_remote_trades(runtime, journal, &snapshot) {
                    Ok(count) => count,
                    Err(error) => {
                        fail_user_reconnect(
                            runtime,
                            journal,
                            shared_state,
                            now_ms,
                            revision,
                            error,
                        )
                        .await?;
                        return Ok(());
                    }
                };
            if runtime.safety_latched {
                fail_user_reconnect(
                    runtime,
                    journal,
                    shared_state,
                    now_ms,
                    revision,
                    BotError::Readiness("authoritative_trade_failed_or_unknown".to_string()),
                )
                .await?;
                return Ok(());
            }
            runtime
                .order_clients
                .retain(|exchange_id, _| snapshot.open_order_ids.contains(exchange_id));
            runtime
                .own_orders
                .apply_authoritative_snapshot(snapshot.open_order_ids.clone());
            runtime.processed_account_revision = revision;
            let can_enable = check_compliance(safety.settings, safety.compliance, now_ms).is_ok()
                && safety.heartbeat.require_healthy().is_ok()
                && require_live_ready(safety.settings, safety.readiness).is_ok()
                && !runtime.safety_latched
                && runtime.pending_account_reconciliation.is_none();
            update_state(shared_state, |state| {
                state.user_feed.connected(now_ms);
                state.live_submission_enabled = can_enable;
                state.readiness.insert("API credentials".to_string(), true);
                state.push_event(
                    now_ms,
                    RuntimeEventLevel::Info,
                    "user_ws",
                    format!(
                        "authenticated stream connected and open-order snapshot synchronized; recovered {recovered_trades} missed trade(s)"
                    ),
                );
            })?;
        }
        UserFeedMessage::Disconnected { revision, error } => {
            runtime.processed_account_revision = revision;
            apply_user_event(&mut runtime.own_orders, &UserEvent::Disconnect);
            update_state(shared_state, |state| {
                state.user_feed.degraded(error.clone());
                state.live_submission_enabled = false;
                state.paused = true;
                state.strategy_enabled = false;
                state.phase = RuntimePhase::Degraded;
                state.readiness.insert("API credentials".to_string(), false);
                state.push_event(now_ms, RuntimeEventLevel::Error, "user_ws", error);
            })?;
            let ids = runtime.order_clients.keys().cloned().collect::<Vec<_>>();
            if let Err(error) =
                cancel_tracked_live_orders(runtime, journal, ids, "user_ws_disconnected").await
            {
                latch_cancel_uncertainty(
                    runtime,
                    journal,
                    shared_state,
                    now_ms,
                    "user_ws_disconnected",
                    &error,
                )
                .await?;
            }
        }
        UserFeedMessage::Event { revision, event } => {
            apply_user_event(&mut runtime.own_orders, &event);
            journal_user_event(journal, runtime, &event)?;
            let account_changed = record_pending_account_event(runtime, &event)?;
            if let UserEvent::Order {
                order_id, status, ..
            } = &event
            {
                if order_status_is_terminal(status) {
                    runtime.order_clients.remove(order_id);
                }
            }
            runtime.processed_account_revision = revision;
            let message = match &event {
                UserEvent::Order {
                    order_id, status, ..
                } => format!("order {} {status}", short_identifier(order_id)),
                UserEvent::Trade {
                    trade_id, status, ..
                } => format!("trade {} {status}", short_identifier(trade_id)),
                UserEvent::Disconnect => "disconnect".to_string(),
            };
            update_state(shared_state, |state| {
                state.user_feed.message(now_ms);
                if account_changed {
                    state.live_submission_enabled = false;
                    state.push_event(
                        now_ms,
                        RuntimeEventLevel::Warning,
                        "live_account",
                        "account-changing user event observed; new submissions wait for a changed, stable account snapshot",
                    );
                }
                if let UserEvent::Trade {
                    trade_id,
                    asset_id,
                    side,
                    price,
                    size,
                    timestamp_ms,
                    ..
                } = &event
                {
                    state.push_live_fill(RuntimeFill {
                        fill_id: trade_id.clone(),
                        source: "live_user_ws".to_string(),
                        asset_id: asset_id.clone(),
                        side: *side,
                        price: *price,
                        size: *size,
                        filled_at_ms: timestamp_ms.unwrap_or(now_ms),
                        terminal: false,
                    });
                }
                state.push_event(now_ms, RuntimeEventLevel::Info, "user_ws", message);
            })?;
        }
    }
    Ok(())
}

async fn fail_user_reconnect(
    runtime: &mut LiveRuntime,
    journal: &mut Journal,
    shared_state: &SharedRuntimeState,
    now_ms: u64,
    revision: u64,
    error: BotError,
) -> Result<()> {
    runtime.safety_latched = true;
    runtime.own_orders.certain = false;
    runtime.processed_account_revision = revision;
    update_state(shared_state, |state| {
        state.live_submission_enabled = false;
        state.paused = true;
        state.strategy_enabled = false;
        state.phase = RuntimePhase::Degraded;
        state.user_feed.degraded(error.to_string());
        state.push_event(
            now_ms,
            RuntimeEventLevel::Error,
            "user_ws",
            format!("reconnect reconciliation failed: {error}"),
        );
    })?;
    if let Err(cancel_error) =
        emergency_cancel_all_account(runtime, journal, "user_ws_reconnect_reconciliation_failed")
            .await
    {
        update_state(shared_state, |state| {
            state.push_event(
                now_ms,
                RuntimeEventLevel::Error,
                "user_ws_cancel",
                cancel_error.to_string(),
            );
        })?;
    }
    Ok(())
}

fn record_pending_account_event(runtime: &mut LiveRuntime, event: &UserEvent) -> Result<bool> {
    if !user_event_requires_holdings_reconciliation(event) {
        return Ok(false);
    }
    match event {
        UserEvent::Order { order_id, .. } => {
            if runtime.known_trade_order_ids.contains(order_id) {
                return Ok(false);
            }
            ensure_pending_account_reconciliation(runtime);
            let pending = runtime
                .pending_account_reconciliation
                .as_mut()
                .ok_or_else(|| BotError::Execution("pending_account_state_missing".to_string()))?;
            if !pending.authoritative_order_ids.contains(order_id) {
                pending.unresolved_order_ids.insert(order_id.clone());
            }
        }
        UserEvent::Trade {
            trade_id,
            asset_id,
            side,
            price,
            size,
            status,
            order_ids,
            ..
        } => {
            let order_ids = order_ids.iter().cloned().collect::<BTreeSet<_>>();
            let mutation = PendingTradeMutation {
                asset_id: asset_id.clone(),
                side: *side,
                price: *price,
                size: *size,
                order_ids: order_ids.clone(),
            };
            let existing = runtime.trade_ledger.get(trade_id).cloned();
            if existing
                .as_ref()
                .is_some_and(|entry| entry.mutation != mutation)
            {
                runtime.safety_latched = true;
                return Err(BotError::Protocol(format!(
                    "conflicting_user_trade_identity:{trade_id}"
                )));
            }
            let status_class = classify_trade_status(status);
            if matches!(
                status_class,
                TradeStatusClass::Failed | TradeStatusClass::Unknown
            ) {
                runtime.safety_latched = true;
            }
            let reconciled = existing.as_ref().is_some_and(|entry| entry.reconciled);
            let latest_status = existing
                .as_ref()
                .map(|entry| select_latest_trade_status(&entry.latest_status, status))
                .unwrap_or_else(|| status.clone());
            runtime.trade_ledger.insert(
                trade_id.clone(),
                TradeLedgerEntry {
                    mutation: mutation.clone(),
                    latest_status,
                    reconciled,
                },
            );
            runtime.known_trade_order_ids.extend(order_ids.clone());
            runtime.owned_order_ids.extend(order_ids.clone());
            if reconciled {
                return Ok(matches!(
                    status_class,
                    TradeStatusClass::Failed | TradeStatusClass::Unknown
                ));
            }
            if !matches!(status_class, TradeStatusClass::Active) {
                return Ok(true);
            }
            ensure_pending_account_reconciliation(runtime);
            let pending = runtime
                .pending_account_reconciliation
                .as_mut()
                .ok_or_else(|| BotError::Execution("pending_account_state_missing".to_string()))?;
            pending.trades.insert(trade_id.clone(), mutation);
            pending.authoritative_order_ids.extend(order_ids.clone());
            for order_id in order_ids {
                pending.unresolved_order_ids.remove(&order_id);
            }
        }
        UserEvent::Disconnect => {}
    }
    Ok(true)
}

fn ensure_pending_account_reconciliation(runtime: &mut LiveRuntime) {
    if runtime.pending_account_reconciliation.is_none() {
        runtime.pending_account_reconciliation = Some(PendingAccountReconciliation {
            baseline: runtime.last_account_snapshot.clone(),
            trades: BTreeMap::new(),
            authoritative_order_ids: BTreeSet::new(),
            unresolved_order_ids: BTreeSet::new(),
        });
    }
}

fn authoritative_trade_event(trade: &AuthoritativeTrade) -> UserEvent {
    UserEvent::Trade {
        condition_id: trade.condition_id.clone(),
        trade_id: trade.trade_id.clone(),
        asset_id: trade.asset_id.clone(),
        side: trade.side,
        price: trade.price,
        size: trade.size,
        status: trade.status.clone(),
        order_ids: trade.order_ids.iter().cloned().collect(),
        timestamp_ms: trade.timestamp_ms,
    }
}

fn authoritative_trade_events_for_order_ids(
    order_ids: &BTreeSet<String>,
    trade_ledger: &BTreeMap<String, TradeLedgerEntry>,
    remote: &RemoteSnapshot,
) -> Vec<UserEvent> {
    remote
        .trades
        .values()
        .filter(|trade| !trade.order_ids.is_disjoint(order_ids))
        .filter(|trade| {
            trade_ledger.get(&trade.trade_id).is_none_or(|entry| {
                entry.mutation.asset_id != trade.asset_id
                    || entry.mutation.side != trade.side
                    || entry.mutation.price != trade.price
                    || entry.mutation.size != trade.size
                    || entry.mutation.order_ids != trade.order_ids
                    || select_latest_trade_status(&entry.latest_status, &trade.status)
                        != entry.latest_status
            })
        })
        .map(authoritative_trade_event)
        .collect()
}

fn append_startup_authoritative_trades(
    journal: &mut Journal,
    baseline: &LiveRiskSnapshot,
    order_ids: &BTreeSet<String>,
    trade_ledger: &BTreeMap<String, TradeLedgerEntry>,
    remote: &RemoteSnapshot,
) -> Result<Vec<String>> {
    let events = authoritative_trade_events_for_order_ids(order_ids, trade_ledger, remote);
    let mut trade_ids = Vec::with_capacity(events.len());
    for event in &events {
        let UserEvent::Trade { trade_id, .. } = event else {
            return Err(BotError::Protocol(
                "authoritative_trade_conversion_not_trade".to_string(),
            ));
        };
        append_account_mutation(journal, event, baseline)?;
        trade_ids.push(trade_id.clone());
    }
    Ok(trade_ids)
}

fn ingest_authoritative_remote_trades(
    runtime: &mut LiveRuntime,
    journal: &mut Journal,
    remote: &RemoteSnapshot,
) -> Result<usize> {
    let mut order_ids = runtime.owned_order_ids.clone();
    if let Some(pending) = runtime.pending_account_reconciliation.as_ref() {
        order_ids.extend(pending.unresolved_order_ids.iter().cloned());
    }
    let baseline = runtime.pending_account_reconciliation.as_ref().map_or_else(
        || runtime.last_account_snapshot.clone(),
        |pending| pending.baseline.clone(),
    );
    let events =
        authoritative_trade_events_for_order_ids(&order_ids, &runtime.trade_ledger, remote);
    for event in &events {
        append_account_mutation(journal, event, &baseline)?;
        record_pending_account_event(runtime, event)?;
    }
    Ok(events.len())
}

fn authoritative_pending_trade_proof(
    pending: &PendingAccountReconciliation,
    remote: &RemoteSnapshot,
) -> Result<AuthoritativePendingTradeProof> {
    let mut proof = AuthoritativePendingTradeProof::default();
    for (trade_id, mutation) in &pending.trades {
        let Some(trade) = remote.trades.get(trade_id) else {
            continue;
        };
        if trade.asset_id != mutation.asset_id
            || trade.side != mutation.side
            || trade.price != mutation.price
            || trade.size != mutation.size
            || trade.order_ids != mutation.order_ids
        {
            return Err(BotError::Protocol(format!(
                "authoritative_trade_identity_conflict:{trade_id}"
            )));
        }
        if classify_trade_status(&trade.status) == TradeStatusClass::Active {
            proof.trade_ids.insert(trade_id.clone());
            let (fee_lower_bound, fee_upper_bound) = authoritative_trade_fee_bounds(trade)?;
            proof.cash_fee_lower_bound_usdc = proof
                .cash_fee_lower_bound_usdc
                .checked_add(fee_lower_bound)?;
            proof.cash_fee_upper_bound_usdc = proof
                .cash_fee_upper_bound_usdc
                .checked_add(fee_upper_bound)?;
        }
    }
    Ok(proof)
}

fn authoritative_trade_fee_bounds(
    trade: &AuthoritativeTrade,
) -> Result<(crate::fixed::Fixed, crate::fixed::Fixed)> {
    use crate::fixed::Fixed;

    if trade.size <= Fixed::ZERO
        || trade.price <= Fixed::ZERO
        || trade.price >= Fixed::ONE
        || trade.fee_rate_bps < Fixed::ZERO
        || trade.fee_rate_bps > "10000".parse()?
    {
        return Err(BotError::Protocol(format!(
            "authoritative_trade_fee_terms_invalid:{}",
            trade.trade_id
        )));
    }
    if trade.fee_rate_bps == Fixed::ZERO {
        return Ok((Fixed::ZERO, Fixed::ZERO));
    }

    // V2 platform fees are size * rate * price * (1 - price). The runtime does
    // not attach builder code. Carry floor/ceiling bounds through every fixed-
    // point operation, then widen to the venue's documented five-decimal fee
    // precision so reconciliation can neither ignore a real fee nor accept an
    // unrelated debit outside the authoritative trade's fee envelope.
    let complement = Fixed::ONE.checked_sub(trade.price)?;
    let fee_base_lower = trade
        .size
        .checked_mul(trade.price)?
        .checked_mul(complement)?;
    let fee_base_upper = trade
        .size
        .checked_mul_ceil(trade.price)?
        .checked_mul_ceil(complement)?;
    let fee_lower = checked_basis_point_product(fee_base_lower, trade.fee_rate_bps, false)?;
    let fee_upper = checked_basis_point_product(fee_base_upper, trade.fee_rate_bps, true)?;
    Ok((
        fee_lower.floor_to_decimals(5)?,
        ceil_to_five_decimals(fee_upper)?,
    ))
}

fn checked_basis_point_product(
    amount: crate::fixed::Fixed,
    fee_rate_bps: crate::fixed::Fixed,
    round_up: bool,
) -> Result<crate::fixed::Fixed> {
    use crate::fixed::{Fixed, SCALE};

    let numerator = amount
        .raw()
        .checked_mul(fee_rate_bps.raw())
        .ok_or_else(|| BotError::Risk("fee-bound multiplication overflow".to_string()))?;
    let denominator = SCALE
        .checked_mul(10_000)
        .ok_or_else(|| BotError::Risk("fee-bound denominator overflow".to_string()))?;
    let quotient = numerator.div_euclid(denominator);
    let raw = if round_up && numerator.rem_euclid(denominator) != 0 {
        quotient
            .checked_add(1)
            .ok_or_else(|| BotError::Risk("fee-bound rounding overflow".to_string()))?
    } else {
        quotient
    };
    Ok(Fixed::from_scaled(raw))
}

fn ceil_to_five_decimals(value: crate::fixed::Fixed) -> Result<crate::fixed::Fixed> {
    use crate::fixed::Fixed;

    if value < Fixed::ZERO {
        return Err(BotError::Risk(
            "cannot round a negative fee bound".to_string(),
        ));
    }
    const QUANTUM_RAW: i128 = 10;
    let quotient = value.raw().div_euclid(QUANTUM_RAW);
    let quantized = if value.raw().rem_euclid(QUANTUM_RAW) == 0 {
        quotient
    } else {
        quotient
            .checked_add(1)
            .ok_or_else(|| BotError::Risk("fee-bound quantization overflow".to_string()))?
    };
    let raw = quantized
        .checked_mul(QUANTUM_RAW)
        .ok_or_else(|| BotError::Risk("fee-bound quantization overflow".to_string()))?;
    Ok(Fixed::from_scaled(raw))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TradeStatusClass {
    Active,
    Failed,
    Unknown,
}

fn classify_trade_status(status: &str) -> TradeStatusClass {
    match status.to_ascii_lowercase().as_str() {
        "matched" | "mined" | "confirmed" | "retrying" => TradeStatusClass::Active,
        "failed" => TradeStatusClass::Failed,
        _ => TradeStatusClass::Unknown,
    }
}

fn trade_status_rank(status: &str) -> u8 {
    match status.to_ascii_lowercase().as_str() {
        "matched" => 0,
        "retrying" => 1,
        "mined" => 2,
        "confirmed" => 3,
        "failed" => 4,
        _ => 5,
    }
}

fn select_latest_trade_status(existing: &str, incoming: &str) -> String {
    if trade_status_rank(incoming) >= trade_status_rank(existing) {
        incoming.to_string()
    } else {
        existing.to_string()
    }
}

fn commit_account_reconciliation(
    runtime: &mut LiveRuntime,
    journal: &mut Journal,
    now_ms: u64,
) -> Result<()> {
    let pending = runtime
        .pending_account_reconciliation
        .as_ref()
        .ok_or_else(|| BotError::Execution("pending_account_state_missing".to_string()))?;
    let trade_ids = pending.trades.keys().cloned().collect::<Vec<_>>();
    if trade_ids.is_empty() {
        return Err(BotError::Readiness(
            "account_reconciliation_has_no_authoritative_trades".to_string(),
        ));
    }
    if trade_ids
        .iter()
        .any(|trade_id| !runtime.trade_ledger.contains_key(trade_id))
    {
        return Err(BotError::Journal(
            "reconciled_trade_missing_from_ledger".to_string(),
        ));
    }
    append_account_reconciliation_record(journal, &trade_ids, now_ms, "reconciled")?;
    for trade_id in &trade_ids {
        if let Some(entry) = runtime.trade_ledger.get_mut(trade_id) {
            entry.reconciled = true;
        }
    }
    runtime.pending_account_reconciliation = None;
    Ok(())
}

fn append_account_reconciliation_record(
    journal: &mut Journal,
    trade_ids: &[String],
    now_ms: u64,
    result: &str,
) -> Result<()> {
    if trade_ids.is_empty() {
        return Err(BotError::Readiness(
            "account_reconciliation_has_no_authoritative_trades".to_string(),
        ));
    }
    let payload = AccountReconciliationJournal {
        schema_version: ACCOUNT_MUTATION_SCHEMA_VERSION,
        reconciled_at_ms: now_ms,
        trade_ids: trade_ids.to_vec(),
    };
    let record_id = format!("account-reconciliation-{}", journal.next_sequence());
    journal.append_lifecycle(
        JournalEventKind::Reconciled,
        record_id.clone(),
        record_id,
        None,
        "live_account",
        result,
        serde_json::to_string(&payload).map_err(|error| {
            BotError::Journal(format!("account_reconciliation_serialize:{error}"))
        })?,
    )?;
    Ok(())
}

async fn maintain_live_heartbeat(
    shared_state: &SharedRuntimeState,
    live_runtime: &mut Option<LiveRuntime>,
    heartbeat: &mut HeartbeatState,
    journal: &mut Journal,
) -> Result<()> {
    let Some(runtime) = live_runtime.as_mut() else {
        return Ok(());
    };
    match runtime.router.adapter_mut().post_heartbeat().await {
        Ok(()) => {
            heartbeat.record_success();
            update_state(shared_state, |state| {
                state.readiness.insert("Heartbeat".to_string(), true);
            })?;
        }
        Err(error) => {
            heartbeat.record_failure();
            if heartbeat.degraded {
                update_state(shared_state, |state| {
                    state.readiness.insert("Heartbeat".to_string(), false);
                })?;
            }
            update_state(shared_state, |state| {
                state.push_event(
                    system_now_ms(),
                    RuntimeEventLevel::Warning,
                    "heartbeat",
                    error.to_string(),
                );
            })?;
            if heartbeat.degraded {
                let ids = runtime.order_clients.keys().cloned().collect::<Vec<_>>();
                let cancel_result =
                    cancel_tracked_live_orders(runtime, journal, ids, "heartbeat_degraded").await;
                update_state(shared_state, |state| {
                    state.live_submission_enabled = false;
                    state.paused = true;
                    state.strategy_enabled = false;
                    state.phase = RuntimePhase::Degraded;
                })?;
                match cancel_result {
                    Ok(_) => {}
                    Err(cancel_error) => {
                        latch_cancel_uncertainty(
                            runtime,
                            journal,
                            shared_state,
                            system_now_ms(),
                            "heartbeat_degraded",
                            &cancel_error,
                        )
                        .await?;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn enforce_live_compliance(
    settings: &Settings,
    compliance: &ComplianceState,
    shared_state: &SharedRuntimeState,
    live_runtime: &mut Option<LiveRuntime>,
    heartbeat: &HeartbeatState,
    journal: &mut Journal,
) -> Result<()> {
    if settings.mode != BotMode::Live || live_runtime.is_none() {
        return Ok(());
    }
    let now_ms = system_now_ms();
    if let Err(error) = check_compliance(settings, compliance, now_ms) {
        pause_and_cancel_live(
            shared_state,
            live_runtime,
            journal,
            "compliance_became_ineligible",
        )
        .await?;
        update_state(shared_state, |state| {
            state.push_event(
                now_ms,
                RuntimeEventLevel::Error,
                "compliance",
                error.to_string(),
            );
        })?;
    } else if heartbeat.require_healthy().is_ok()
        && live_runtime.as_ref().is_some_and(|runtime| {
            runtime.own_orders.certain
                && !runtime.safety_latched
                && runtime.pending_account_reconciliation.is_none()
        })
    {
        update_state(shared_state, |state| {
            state.live_submission_enabled = true;
            state.push_event(
                now_ms,
                RuntimeEventLevel::Info,
                "compliance",
                "eligibility restored; live remains paused until explicit resume",
            );
        })?;
    }
    Ok(())
}

async fn pause_and_cancel_live(
    shared_state: &SharedRuntimeState,
    live_runtime: &mut Option<LiveRuntime>,
    journal: &mut Journal,
    reason: &str,
) -> Result<()> {
    update_state(shared_state, |state| {
        state.live_submission_enabled = false;
        state.paused = true;
        state.strategy_enabled = false;
        state.phase = RuntimePhase::Degraded;
    })?;
    let Some(runtime) = live_runtime.as_mut() else {
        return Ok(());
    };
    let ids = runtime.order_clients.keys().cloned().collect::<Vec<_>>();
    match cancel_tracked_live_orders(runtime, journal, ids, reason).await {
        Ok(_) => {}
        Err(error) => {
            latch_cancel_uncertainty(
                runtime,
                journal,
                shared_state,
                system_now_ms(),
                reason,
                &error,
            )
            .await?;
        }
    }
    Ok(())
}

fn cancel_all_paper_for_compliance(
    paper_router: &mut Option<ExecutionRouter<PaperExecution>>,
    journal: &mut Journal,
    reason: &str,
    now_ms: u64,
) -> Result<()> {
    let Some(router) = paper_router.as_mut() else {
        return Ok(());
    };
    let ids = router.adapter().engine().open_order_ids();
    if ids.is_empty() {
        return Ok(());
    }
    let mut staged = router.adapter().engine().clone();
    staged.cancel_all(now_ms, reason);
    commit_paper_transition(
        journal,
        router.adapter_mut().engine_mut(),
        staged,
        &[],
        &ids,
        reason,
        now_ms,
    )
}

#[allow(clippy::too_many_arguments)]
async fn submit_paper_intent(
    router: &mut ExecutionRouter<PaperExecution>,
    journal: &mut Journal,
    intent: OrderIntent,
    settings: &Settings,
    market: &crate::types::MarketMeta,
    book: &BookState,
    books: &BTreeMap<AssetId, BookState>,
    readiness: &ReadinessState,
    compliance: &ComplianceState,
    heartbeat: &HeartbeatState,
    recovery: &RecoveryReport,
    matching: &MatchingEngineState,
    limiter: &mut SlidingWindowRateLimiter,
    shared_state: &SharedRuntimeState,
    latency: &mut LatencyRecorder,
) -> Result<()> {
    let now_ms = system_now_ms();
    if !limiter.try_acquire(now_ms) {
        update_state(shared_state, |state| {
            state.rejected_intents = state.rejected_intents.saturating_add(1);
            state.push_event(
                now_ms,
                RuntimeEventLevel::Warning,
                "rate_limit",
                format!(
                    "new order rate-limited; retry in {}ms",
                    limiter.retry_after_ms(now_ms)
                ),
            );
        })?;
        return Ok(());
    }
    let risk_state = router.adapter().engine().risk_state(
        now_ms,
        &intent.asset_id,
        books,
        compliance.prohibited_conduct_flag || compliance.confidential_info_flag,
    )?;
    let context = ExecutionContext {
        settings,
        readiness,
        compliance,
        heartbeat,
        recovery,
        matching_engine: matching,
        market,
        book: Some(book),
        risk_state: &risk_state,
        account_revision: None,
    };
    let started = Instant::now();
    match router.stage_paper_submission(journal, &intent, &context) {
        Ok((staged, result)) => {
            commit_paper_transition(
                journal,
                router.adapter_mut().engine_mut(),
                staged,
                &[],
                &[],
                "paper_submit",
                now_ms,
            )?;
            update_state(shared_state, |state| {
                state.submitted_orders = state.submitted_orders.saturating_add(1);
                state.push_event(
                    now_ms,
                    RuntimeEventLevel::Info,
                    "execution",
                    format!(
                        "{} {:?}:{}",
                        result.client_order_id, result.status, result.message
                    ),
                );
            })?;
        }
        Err(error) => {
            update_state(shared_state, |state| {
                state.rejected_intents = state.rejected_intents.saturating_add(1);
                if journal.is_poisoned() {
                    state.paused = true;
                    state.strategy_enabled = false;
                    state.phase = RuntimePhase::Degraded;
                }
                state.push_event(
                    now_ms,
                    if journal.is_poisoned() {
                        RuntimeEventLevel::Error
                    } else {
                        RuntimeEventLevel::Warning
                    },
                    "risk",
                    error.to_string(),
                );
            })?;
        }
    }
    latency.record("submit_check", elapsed_us(started));
    update_state(shared_state, |state| {
        state.journal_next_sequence = journal.next_sequence();
        state.journal_last_hash = journal.last_hash().to_string();
    })?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn submit_live_intent(
    runtime: &mut LiveRuntime,
    journal: &mut Journal,
    intent: OrderIntent,
    settings: &Settings,
    market: &crate::types::MarketMeta,
    book: &BookState,
    readiness: &ReadinessState,
    compliance: &ComplianceState,
    heartbeat: &HeartbeatState,
    recovery: &RecoveryReport,
    matching: &MatchingEngineState,
    limiter: &mut SlidingWindowRateLimiter,
    shared_state: &SharedRuntimeState,
    latency: &mut LatencyRecorder,
) -> Result<()> {
    let now_ms = system_now_ms();
    if !limiter.try_acquire(now_ms) {
        update_state(shared_state, |state| {
            state.rejected_intents = state.rejected_intents.saturating_add(1);
            state.push_event(
                now_ms,
                RuntimeEventLevel::Warning,
                "rate_limit",
                format!(
                    "live order rate-limited; retry in {}ms",
                    limiter.retry_after_ms(now_ms)
                ),
            );
        })?;
        return Ok(());
    }

    let observed_account_revision = runtime.account_revision.load(Ordering::Acquire);
    if observed_account_revision != runtime.processed_account_revision {
        update_state(shared_state, |state| {
            state.rejected_intents = state.rejected_intents.saturating_add(1);
            state.push_event(
                now_ms,
                RuntimeEventLevel::Warning,
                "live_account",
                "account event is queued but not yet processed; intent discarded",
            );
        })?;
        return Ok(());
    }
    let expected_account_revision = observed_account_revision;
    let account_started = Instant::now();
    let account_result = runtime
        .router
        .adapter()
        .risk_snapshot(
            &settings.data_api_host,
            &settings.funder_address,
            &intent.asset_id,
            runtime.baseline.value.baseline_equity_usdc,
            compliance.prohibited_conduct_flag || compliance.confidential_info_flag,
            now_ms,
        )
        .await;
    latency.record("live_account_refresh", elapsed_us(account_started));
    let mut account = match account_result {
        Ok(account) => account,
        Err(error) => {
            update_state(shared_state, |state| {
                state.rejected_intents = state.rejected_intents.saturating_add(1);
                state.push_event(
                    now_ms,
                    RuntimeEventLevel::Error,
                    "live_account",
                    format!("fresh account proof unavailable: {error}"),
                );
            })?;
            return Ok(());
        }
    };
    if runtime.account_revision.load(Ordering::Acquire) != expected_account_revision {
        update_state(shared_state, |state| {
            state.rejected_intents = state.rejected_intents.saturating_add(1);
            state.push_event(
                now_ms,
                RuntimeEventLevel::Warning,
                "live_account",
                "authenticated account changed during the risk snapshot; intent discarded",
            );
        })?;
        return Ok(());
    }
    if apply_equity_high_water(&mut runtime.baseline, &mut account, now_ms)? {
        update_state(shared_state, |state| {
            state.push_event(
                now_ms,
                RuntimeEventLevel::Info,
                "daily_risk",
                "UTC date advanced; retained the observed equity high-water for conservative loss control",
            );
        })?;
    }
    if let Some(pending) = &runtime.pending_account_reconciliation {
        if !pending_account_reconciled(
            pending,
            &account,
            &AuthoritativePendingTradeProof::default(),
        )? {
            update_state(shared_state, |state| {
                state.live_account = Some(account.clone());
                state.rejected_intents = state.rejected_intents.saturating_add(1);
                state.push_event(
                    now_ms,
                    RuntimeEventLevel::Warning,
                    "live_account",
                    "specific user-stream trades are not yet reflected in Data API holdings; intent discarded",
                );
            })?;
            return Ok(());
        }
        commit_account_reconciliation(runtime, journal, now_ms)?;
        update_state(shared_state, |state| {
            state.live_submission_enabled = !runtime.safety_latched;
            state.push_event(
                now_ms,
                RuntimeEventLevel::Info,
                "live_account",
                "all pending user-stream trades reconciled against their exact asset deltas",
            );
        })?;
    }
    runtime.last_account_snapshot = account.clone();
    if account
        .risk_state
        .own_resting_orders
        .iter()
        .any(|order| order.asset_id == intent.asset_id)
    {
        update_state(shared_state, |state| {
            state.live_account = Some(account.clone());
            state.rejected_intents = state.rejected_intents.saturating_add(1);
            state.push_event(
                now_ms,
                RuntimeEventLevel::Warning,
                "risk",
                "live duplicate-asset order suppressed until the existing order is terminal",
            );
        })?;
        return Ok(());
    }
    let risk_state = account.risk_state.clone();
    update_state(shared_state, |state| state.live_account = Some(account))?;
    let context = ExecutionContext {
        settings,
        readiness,
        compliance,
        heartbeat,
        recovery,
        matching_engine: matching,
        market,
        book: Some(book),
        risk_state: &risk_state,
        account_revision: Some((&runtime.account_revision, expected_account_revision)),
    };
    let started = Instant::now();
    match runtime
        .router
        .submit_live_checked(journal, &intent, &context)
        .await
    {
        Ok(CheckedSubmission::Recorded(result))
            if result.status == crate::types::OrderStatus::Rejected =>
        {
            update_state(shared_state, |state| {
                state.rejected_intents = state.rejected_intents.saturating_add(1);
                state.push_event(
                    now_ms,
                    RuntimeEventLevel::Warning,
                    "execution",
                    format!("live order rejected: {}", result.message),
                );
            })?;
        }
        Ok(CheckedSubmission::Recorded(result))
            if result.status == crate::types::OrderStatus::Unknown =>
        {
            let reason = format!("ambiguous_order_response:{}", result.message);
            handle_ambiguous_live_submission(
                runtime,
                journal,
                shared_state,
                Some(result),
                intent.asset_id.clone(),
                reason,
                now_ms,
            )
            .await?;
        }
        Ok(CheckedSubmission::Recorded(result)) => {
            if let Some(exchange_id) = &result.exchange_order_id {
                runtime.owned_order_ids.insert(exchange_id.clone());
                runtime.order_clients.insert(
                    exchange_id.clone(),
                    RecoveredOpenOrder {
                        client_order_id: result.client_order_id.clone(),
                        submitted_at_ms: now_ms,
                        asset_id: intent.asset_id.clone(),
                    },
                );
            }
            update_state(shared_state, |state| {
                state.submitted_orders = state.submitted_orders.saturating_add(1);
                state.push_event(
                    now_ms,
                    RuntimeEventLevel::Info,
                    "execution",
                    format!(
                        "{} {:?}:{}",
                        result.client_order_id, result.status, result.message
                    ),
                );
            })?;
        }
        Ok(CheckedSubmission::Ambiguous { result, reason }) => {
            handle_ambiguous_live_submission(
                runtime,
                journal,
                shared_state,
                result,
                intent.asset_id.clone(),
                reason,
                now_ms,
            )
            .await?;
        }
        Err(error) => {
            let journal_failed = journal.is_poisoned();
            if journal_failed {
                runtime.safety_latched = true;
            }
            update_state(shared_state, |state| {
                state.rejected_intents = state.rejected_intents.saturating_add(1);
                if journal_failed {
                    state.live_submission_enabled = false;
                    state.paused = true;
                    state.strategy_enabled = false;
                    state.phase = RuntimePhase::Degraded;
                }
                state.push_event(
                    now_ms,
                    if journal_failed {
                        RuntimeEventLevel::Error
                    } else {
                        RuntimeEventLevel::Warning
                    },
                    "live_risk",
                    error.to_string(),
                );
            })?;
        }
    }
    latency.record("live_submit_check", elapsed_us(started));
    update_state(shared_state, |state| {
        state.journal_next_sequence = journal.next_sequence();
        state.journal_last_hash = journal.last_hash().to_string();
    })?;
    Ok(())
}

async fn handle_ambiguous_live_submission(
    runtime: &mut LiveRuntime,
    journal: &mut Journal,
    shared_state: &SharedRuntimeState,
    result: Option<ExecutionResult>,
    asset_id: AssetId,
    reason: String,
    now_ms: u64,
) -> Result<()> {
    runtime.safety_latched = true;
    if let Some(result) = &result {
        if let Some(exchange_order_id) = &result.exchange_order_id {
            runtime.owned_order_ids.insert(exchange_order_id.clone());
            runtime.order_clients.insert(
                exchange_order_id.clone(),
                RecoveredOpenOrder {
                    client_order_id: result.client_order_id.clone(),
                    submitted_at_ms: now_ms,
                    asset_id,
                },
            );
        }
    }
    update_state(shared_state, |state| {
        state.submitted_orders = state.submitted_orders.saturating_add(1);
        state.live_submission_enabled = false;
        state.paused = true;
        state.strategy_enabled = false;
        state.phase = RuntimePhase::Degraded;
        state.push_event(
            now_ms,
            RuntimeEventLevel::Error,
            "live_execution",
            format!("ambiguous live submission; restart reconciliation required: {reason}"),
        );
    })?;
    if let Err(error) =
        emergency_cancel_all_account(runtime, journal, "ambiguous_live_submission").await
    {
        update_state(shared_state, |state| {
            state.push_event(
                now_ms,
                RuntimeEventLevel::Error,
                "live_cancel",
                format!("account-wide cancel after ambiguous submission failed: {error}"),
            );
        })?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_operator_command(
    command: OperatorCommand,
    settings: &Settings,
    shared_state: &SharedRuntimeState,
    paper_router: &mut Option<ExecutionRouter<PaperExecution>>,
    live_runtime: &mut Option<LiveRuntime>,
    books: &mut BTreeMap<AssetId, BookState>,
    journal: &mut Journal,
    cancel_limiter: &mut SlidingWindowRateLimiter,
) -> Result<CommandOutcome> {
    let now_ms = system_now_ms();
    match command {
        OperatorCommand::Pause => {
            update_state(shared_state, |state| {
                state.paused = true;
                state.strategy_enabled = false;
                state.phase = RuntimePhase::Paused;
                state.push_event(
                    now_ms,
                    RuntimeEventLevel::Info,
                    "operator",
                    "strategy paused",
                );
            })?;
            Ok(command_outcome("pause", "accepted", "strategy paused", 0))
        }
        OperatorCommand::ResumePaper => {
            if settings.mode != BotMode::Paper || paper_router.is_none() {
                return Err(BotError::Execution(
                    "resume_paper_requires_operational_paper_runtime".to_string(),
                ));
            }
            update_state(shared_state, |state| {
                state.paused = false;
                state.strategy_enabled = true;
                state.phase = if state.market_feed.status == ConnectionStatus::Connected
                    && state.catalog_ready
                {
                    RuntimePhase::Running
                } else {
                    RuntimePhase::Degraded
                };
                state.push_event(
                    now_ms,
                    RuntimeEventLevel::Info,
                    "operator",
                    "paper strategy resumed",
                );
            })?;
            Ok(command_outcome(
                "resume_paper",
                "accepted",
                "paper strategy resumed; all risk gates remain active",
                0,
            ))
        }
        OperatorCommand::ResumeLive => {
            if settings.mode != BotMode::Live || live_runtime.is_none() {
                return Err(BotError::Readiness(
                    "resume_live_requires_fully_verified_live_runtime".to_string(),
                ));
            }
            if live_runtime
                .as_ref()
                .is_some_and(|runtime| runtime.safety_latched)
            {
                return Err(BotError::Readiness(
                    "live_safety_latch_requires_restart_reconciliation".to_string(),
                ));
            }
            let live_enabled = read_state(shared_state, |state| state.live_submission_enabled)?;
            if !live_enabled {
                return Err(BotError::Readiness(
                    "live_submission_not_enabled".to_string(),
                ));
            }
            update_state(shared_state, |state| {
                state.paused = false;
                state.strategy_enabled = true;
                state.phase = if state.market_feed.status == ConnectionStatus::Connected
                    && state.user_feed.status == ConnectionStatus::Connected
                    && state.catalog_ready
                {
                    RuntimePhase::Running
                } else {
                    RuntimePhase::Degraded
                };
                state.push_event(
                    now_ms,
                    RuntimeEventLevel::Warning,
                    "operator",
                    "live strategy explicitly resumed",
                );
            })?;
            Ok(command_outcome(
                "resume_live",
                "accepted",
                "live strategy resumed; every order still requires fresh account and market proofs",
                0,
            ))
        }
        OperatorCommand::CancelStale => {
            acquire_cancel(cancel_limiter, now_ms)?;
            let ids = if let Some(router) = paper_router.as_mut() {
                let mut staged = router.adapter().engine().clone();
                let ids = staged.cancel_expired(now_ms);
                if !ids.is_empty() {
                    commit_paper_transition(
                        journal,
                        router.adapter_mut().engine_mut(),
                        staged,
                        &[],
                        &ids,
                        "operator_cancel_stale",
                        now_ms,
                    )?;
                }
                ids
            } else if let Some(runtime) = live_runtime.as_mut() {
                let ids = tracked_stale_order_ids(runtime, now_ms, settings.order_ttl_ms);
                match cancel_tracked_live_orders(runtime, journal, ids, "operator_cancel_stale")
                    .await
                {
                    Ok(ids) => ids,
                    Err(error) => {
                        latch_cancel_uncertainty(
                            runtime,
                            journal,
                            shared_state,
                            now_ms,
                            "operator_cancel_stale",
                            &error,
                        )
                        .await?;
                        return Err(error);
                    }
                }
            } else {
                return Err(BotError::Execution(
                    "cancel_runtime_unavailable".to_string(),
                ));
            };
            Ok(command_outcome(
                "cancel_stale",
                "executed",
                "expired orders cancelled",
                ids.len(),
            ))
        }
        OperatorCommand::CancelAll | OperatorCommand::ReduceRisk => {
            acquire_cancel(cancel_limiter, now_ms)?;
            let reason = if command == OperatorCommand::ReduceRisk {
                "operator_reduce_risk"
            } else {
                "operator_cancel_all"
            };
            let ids = if let Some(router) = paper_router.as_mut() {
                let ids = router.adapter().engine().open_order_ids();
                if !ids.is_empty() {
                    let mut staged = router.adapter().engine().clone();
                    staged.cancel_all(now_ms, reason);
                    commit_paper_transition(
                        journal,
                        router.adapter_mut().engine_mut(),
                        staged,
                        &[],
                        &ids,
                        reason,
                        now_ms,
                    )?;
                }
                ids
            } else if let Some(runtime) = live_runtime.as_mut() {
                let ids = runtime.order_clients.keys().cloned().collect::<Vec<_>>();
                match cancel_tracked_live_orders(runtime, journal, ids, reason).await {
                    Ok(ids) => ids,
                    Err(error) => {
                        latch_cancel_uncertainty(
                            runtime,
                            journal,
                            shared_state,
                            now_ms,
                            reason,
                            &error,
                        )
                        .await?;
                        return Err(error);
                    }
                }
            } else {
                return Err(BotError::Execution(
                    "cancel_runtime_unavailable".to_string(),
                ));
            };
            update_state(shared_state, |state| {
                state.push_event(
                    now_ms,
                    RuntimeEventLevel::Info,
                    "operator",
                    format!("{reason}:{} order(s)", ids.len()),
                );
            })?;
            Ok(command_outcome(
                if command == OperatorCommand::ReduceRisk {
                    "reduce_risk"
                } else {
                    "cancel_all"
                },
                "executed",
                "open orders cancelled",
                ids.len(),
            ))
        }
        OperatorCommand::FlattenPaper => {
            acquire_cancel(cancel_limiter, now_ms)?;
            let router = paper_router.as_mut().ok_or_else(|| {
                BotError::Execution("paper_flatten_runtime_unavailable".to_string())
            })?;
            let cancelled = router.adapter().engine().open_order_ids();
            let (staged, fills) = router.adapter().engine().staged_flatten(
                books,
                now_ms,
                settings.max_book_age_ms,
                settings.max_event_lag_ms,
                settings.max_slippage_ticks,
            )?;
            let residual_positions = staged.nonzero_position_count();
            commit_paper_transition(
                journal,
                router.adapter_mut().engine_mut(),
                staged,
                &fills,
                &cancelled,
                "operator_flatten",
                now_ms,
            )?;
            let fill_count = fills.len();
            publish_paper_fills(shared_state, &fills)?;
            Ok(command_outcome(
                "flatten_paper",
                "executed",
                if residual_positions == 0 {
                    "paper positions flattened against verified visible bid liquidity".to_string()
                } else {
                    format!(
                        "paper flatten consumed verified visible bid liquidity; {residual_positions} residual position(s) remain"
                    )
                },
                cancelled.len().saturating_add(fill_count),
            ))
        }
        OperatorCommand::Shutdown => Ok(command_outcome(
            "shutdown",
            "accepted",
            "graceful shutdown requested",
            0,
        )),
    }
}

fn maintain_runtime(
    settings: &Settings,
    shared_state: &SharedRuntimeState,
    books: &mut BTreeMap<AssetId, BookState>,
    paper_router: &mut Option<ExecutionRouter<PaperExecution>>,
    journal: &mut Journal,
    latency: &mut LatencyRecorder,
) -> Result<()> {
    let now_ms = system_now_ms();
    if let Some(router) = paper_router {
        let mut staged = router.adapter().engine().clone();
        let expired = staged.cancel_expired(now_ms);
        if !expired.is_empty() {
            commit_paper_transition(
                journal,
                router.adapter_mut().engine_mut(),
                staged,
                &[],
                &expired,
                "paper_order_ttl_expired",
                now_ms,
            )?;
        }
        let snapshot = router.adapter().engine().snapshot(books)?;
        update_state(shared_state, |state| state.portfolio = Some(snapshot))?;
    }
    update_state(shared_state, |state| {
        state.latency = latency.summaries();
        state.journal_next_sequence = journal.next_sequence();
        state.journal_last_hash = journal.last_hash().to_string();
        if state.market_feed.status == ConnectionStatus::Connected
            && state
                .market_feed
                .last_message_at_ms
                .is_none_or(|last| now_ms.saturating_sub(last) > settings.max_book_age_ms * 4)
        {
            state.phase = RuntimePhase::Degraded;
        }
        state.touch(now_ms);
    })?;
    Ok(())
}

async fn maintain_live_order_ttls(
    settings: &Settings,
    shared_state: &SharedRuntimeState,
    live_runtime: &mut Option<LiveRuntime>,
    journal: &mut Journal,
    cancel_limiter: &mut SlidingWindowRateLimiter,
) -> Result<()> {
    let Some(runtime) = live_runtime.as_mut() else {
        return Ok(());
    };
    let now_ms = system_now_ms();
    let ids = tracked_stale_order_ids(runtime, now_ms, settings.order_ttl_ms);
    if ids.is_empty() {
        return Ok(());
    }
    if !cancel_limiter.try_acquire(now_ms) {
        update_state(shared_state, |state| {
            state.push_event(
                now_ms,
                RuntimeEventLevel::Warning,
                "cancel_rate_limit",
                format!(
                    "stale live orders awaiting cancellation; retry in {}ms",
                    cancel_limiter.retry_after_ms(now_ms)
                ),
            );
        })?;
        return Ok(());
    }
    match cancel_tracked_live_orders(runtime, journal, ids, "live_order_ttl_expired").await {
        Ok(canceled) => update_state(shared_state, |state| {
            state.push_event(
                now_ms,
                RuntimeEventLevel::Info,
                "live_ttl",
                format!("cancelled {} expired live order(s)", canceled.len()),
            );
        })?,
        Err(error) => {
            latch_cancel_uncertainty(
                runtime,
                journal,
                shared_state,
                now_ms,
                "live_order_ttl_expired",
                &error,
            )
            .await?;
        }
    }
    Ok(())
}

async fn maintain_live_account_reconciliation(
    settings: &Settings,
    shared_state: &SharedRuntimeState,
    live_runtime: &mut Option<LiveRuntime>,
    journal: &mut Journal,
    readiness: &ReadinessState,
    compliance: &ComplianceState,
    heartbeat: &HeartbeatState,
) -> Result<()> {
    let Some(runtime) = live_runtime.as_mut() else {
        return Ok(());
    };
    let audit_started_at_ms = system_now_ms();
    let audit_due = runtime.pending_account_reconciliation.is_some()
        || audit_started_at_ms.saturating_sub(runtime.last_trade_audit_at_ms)
            >= LIVE_TRADE_AUDIT_INTERVAL_MS;
    if !audit_due {
        return Ok(());
    }
    runtime.last_trade_audit_at_ms = audit_started_at_ms;
    let observed_revision = runtime.account_revision.load(Ordering::Acquire);
    if observed_revision != runtime.processed_account_revision {
        return Ok(());
    }
    let Some(asset_id) = settings.asset_ids.first() else {
        return Err(BotError::Config("missing_asset_ids".to_string()));
    };
    let adapter = runtime.router.adapter();
    let (snapshot_result, remote_result) = tokio::join!(
        adapter.risk_snapshot(
            &settings.data_api_host,
            &settings.funder_address,
            asset_id,
            runtime.baseline.value.baseline_equity_usdc,
            compliance.prohibited_conduct_flag || compliance.confidential_info_flag,
            system_now_ms(),
        ),
        adapter.remote_snapshot(),
    );
    let (mut snapshot, remote) = match (snapshot_result, remote_result) {
        (Ok(snapshot), Ok(remote)) if remote.trades_loaded => (snapshot, remote),
        (snapshot, remote) => {
            let error = snapshot.err().or_else(|| remote.err()).map_or_else(
                || "authoritative_trade_snapshot_incomplete".to_string(),
                |error| error.to_string(),
            );
            update_state(shared_state, |state| {
                state.live_submission_enabled = false;
                state.push_event(
                    system_now_ms(),
                    RuntimeEventLevel::Warning,
                    "live_account",
                    format!("periodic authoritative trade audit failed: {error}"),
                );
            })?;
            return Ok(());
        }
    };
    if runtime.account_revision.load(Ordering::Acquire) != observed_revision {
        return Ok(());
    }
    let now_ms = system_now_ms();
    let recovered_trades = match ingest_authoritative_remote_trades(runtime, journal, &remote) {
        Ok(count) => count,
        Err(error) => {
            fail_user_reconnect(
                runtime,
                journal,
                shared_state,
                now_ms,
                observed_revision,
                error,
            )
            .await?;
            return Ok(());
        }
    };
    if runtime.safety_latched {
        fail_user_reconnect(
            runtime,
            journal,
            shared_state,
            now_ms,
            observed_revision,
            BotError::Readiness("authoritative_trade_failed_or_unknown".to_string()),
        )
        .await?;
        return Ok(());
    }
    if apply_equity_high_water(&mut runtime.baseline, &mut snapshot, now_ms)? {
        update_state(shared_state, |state| {
            state.push_event(
                now_ms,
                RuntimeEventLevel::Info,
                "daily_risk",
                "UTC date advanced; retained the observed equity high-water for conservative loss control",
            );
        })?;
    }
    let Some(pending) = runtime.pending_account_reconciliation.as_ref() else {
        runtime.last_account_snapshot = snapshot.clone();
        let can_enable = !runtime.safety_latched
            && runtime.own_orders.certain
            && check_compliance(settings, compliance, now_ms).is_ok()
            && heartbeat.require_healthy().is_ok()
            && require_live_ready(settings, readiness).is_ok();
        return update_state(shared_state, |state| {
            state.live_account = Some(snapshot);
            state.live_submission_enabled = can_enable;
            if recovered_trades > 0 {
                state.push_event(
                    now_ms,
                    RuntimeEventLevel::Info,
                    "live_account",
                    format!(
                        "periodic authoritative trade audit durably refreshed {recovered_trades} terminal trade revision(s)"
                    ),
                );
            }
        });
    };
    let authoritative_trade_proof = match authoritative_pending_trade_proof(pending, &remote) {
        Ok(proof) => proof,
        Err(error) => {
            fail_user_reconnect(
                runtime,
                journal,
                shared_state,
                now_ms,
                observed_revision,
                error,
            )
            .await?;
            return Ok(());
        }
    };
    if !pending_account_reconciled(pending, &snapshot, &authoritative_trade_proof)? {
        return Ok(());
    }

    runtime.last_account_snapshot = snapshot.clone();
    commit_account_reconciliation(runtime, journal, now_ms)?;
    let can_enable = !runtime.safety_latched
        && runtime.own_orders.certain
        && check_compliance(settings, compliance, now_ms).is_ok()
        && heartbeat.require_healthy().is_ok()
        && require_live_ready(settings, readiness).is_ok();
    update_state(shared_state, |state| {
        state.live_account = Some(snapshot);
        state.live_submission_enabled = can_enable;
        state.push_event(
            now_ms,
            RuntimeEventLevel::Info,
            "live_account",
            format!(
                "all pending user-stream trades reconciled against exact asset deltas and authoritative trade history; recovered {recovered_trades} missed trade(s)"
            ),
        );
    })
}

fn install_catalog(
    catalog: &MarketCatalog,
    books: &mut BTreeMap<AssetId, BookState>,
    shared_state: &SharedRuntimeState,
) -> Result<()> {
    let now_ms = system_now_ms();
    for market in catalog.markets() {
        let asset = market.configured_asset_id.clone();
        let book = books.entry(asset.clone()).or_insert_with(|| {
            BookState::empty(
                asset.clone(),
                market.meta.tick_size,
                market.meta.min_order_size,
            )
        });
        if book.tick_size != market.meta.tick_size
            || book.min_order_size != market.meta.min_order_size
        {
            book.tick_size = market.meta.tick_size;
            book.min_order_size = market.meta.min_order_size;
            book.tradeable = false;
        }
    }
    update_state(shared_state, |state| {
        state.markets.clear();
        for market in catalog.markets() {
            if let Some(book) = books.get(&market.configured_asset_id) {
                state.markets.insert(
                    market.configured_asset_id.clone(),
                    MarketRuntimeView {
                        condition_id: market.meta.condition_id.clone(),
                        question: market.question.clone(),
                        slug: market.slug.clone(),
                        book: book.clone(),
                    },
                );
            }
        }
        state.catalog_ready = !catalog.is_empty();
        if state.paused {
            state.phase = RuntimePhase::Paused;
        }
        state.push_event(
            now_ms,
            RuntimeEventLevel::Info,
            "discovery",
            format!("verified {} configured market(s)", state.markets.len()),
        );
    })
}

fn append_paper_transition(
    journal: &mut Journal,
    engine: &PaperEngine,
    fills: &[PaperFill],
    cancelled_order_ids: &[String],
    reason: &str,
    now_ms: u64,
) -> Result<(String, String)> {
    engine.validate_state()?;
    let transition = PaperTransition {
        schema_version: PAPER_TRANSITION_SCHEMA_VERSION,
        reason: reason.to_string(),
        committed_at_ms: now_ms,
        engine: engine.clone(),
        fills: fills.to_vec(),
        cancelled_order_ids: cancelled_order_ids.to_vec(),
    };
    let payload = serde_json::to_string(&transition)
        .map_err(|error| BotError::Journal(format!("paper_transition_serialize:{error}")))?;
    let transition_id = format!("paper-transition-{}", Uuid::new_v4());
    journal.append_lifecycle(
        JournalEventKind::PaperTransition,
        transition_id.clone(),
        transition_id.clone(),
        None,
        "paper",
        "committed",
        payload.clone(),
    )?;
    Ok((transition_id, payload))
}

fn commit_paper_transition(
    journal: &mut Journal,
    current: &mut PaperEngine,
    mut staged: PaperEngine,
    fills: &[PaperFill],
    cancelled_order_ids: &[String],
    reason: &str,
    now_ms: u64,
) -> Result<()> {
    staged.prune_terminal_orders();
    let (transition_id, payload) =
        append_paper_transition(journal, &staged, fills, cancelled_order_ids, reason, now_ms)?;
    *current = staged;
    let should_compact = journal
        .should_compact(PAPER_JOURNAL_COMPACT_RECORDS, PAPER_JOURNAL_COMPACT_BYTES)
        .unwrap_or(false);
    if should_compact {
        let compact_result = journal.records_locked().and_then(|records| {
            if records
                .iter()
                .any(|record| record.event_kind != JournalEventKind::PaperTransition)
            {
                return Err(BotError::Journal(
                    "paper_compaction_requires_a_dedicated_journal".to_string(),
                ));
            }
            journal.compact_paper_history(transition_id.clone(), transition_id, payload)
        });
        if let Err(error) = compact_result {
            journal.note_maintenance_warning(format!("paper_compaction_deferred:{error}"));
        }
    }
    Ok(())
}

fn recover_paper_engine(settings: &Settings, journal: &mut Journal) -> Result<PaperEngine> {
    let records = journal.records_locked()?;
    if records
        .iter()
        .any(|record| record.event_kind != JournalEventKind::PaperTransition)
    {
        return Err(BotError::Journal(
            "paper_mode_requires_a_dedicated_journal".to_string(),
        ));
    }
    let Some(record) = records
        .iter()
        .rev()
        .find(|record| record.event_kind == JournalEventKind::PaperTransition)
    else {
        return PaperEngine::new(
            settings.paper_starting_cash_usdc,
            settings.paper_fee_bps,
            settings.paper_fill_participation_bps,
        );
    };
    let transition: PaperTransition = serde_json::from_str(&record.payload)
        .map_err(|error| BotError::Journal(format!("paper_transition_parse:{error}")))?;
    if transition.schema_version != PAPER_TRANSITION_SCHEMA_VERSION
        || !transition.engine.configuration_matches(
            settings.paper_starting_cash_usdc,
            settings.paper_fee_bps,
            settings.paper_fill_participation_bps,
        )
        || !transition.engine.uses_only_assets(&settings.asset_ids)
    {
        return Err(BotError::Journal(
            "paper_transition_configuration_mismatch".to_string(),
        ));
    }
    transition.engine.validate_state()?;
    Ok(transition.engine)
}

fn journal_owned_order_ids(records: &[JournalRecord]) -> BTreeSet<String> {
    records
        .iter()
        .filter_map(|record| record.exchange_order_id.clone())
        .collect()
}

fn recover_account_ledger(records: &[JournalRecord]) -> Result<RecoveredAccountLedger> {
    let mut trade_ledger = BTreeMap::<String, TradeLedgerEntry>::new();
    let mut trade_baselines = BTreeMap::<String, LiveRiskSnapshot>::new();
    let mut order_baselines = BTreeMap::<String, LiveRiskSnapshot>::new();
    let mut known_trade_order_ids = BTreeSet::new();
    let mut reconciled_trade_ids = BTreeSet::new();

    for record in records {
        match record.event_kind {
            JournalEventKind::AccountMutation => {
                let mutation: AccountMutationJournal = serde_json::from_str(&record.payload)
                    .map_err(|error| {
                        BotError::Journal(format!("account_mutation_parse:{error}"))
                    })?;
                if mutation.schema_version != ACCOUNT_MUTATION_SCHEMA_VERSION {
                    return Err(BotError::Journal(
                        "account_mutation_schema_unsupported".to_string(),
                    ));
                }
                match mutation.event {
                    UserEvent::Trade {
                        trade_id,
                        asset_id,
                        side,
                        price,
                        size,
                        status,
                        order_ids,
                        ..
                    } => {
                        let order_ids = order_ids.into_iter().collect::<BTreeSet<_>>();
                        let trade = PendingTradeMutation {
                            asset_id,
                            side,
                            price,
                            size,
                            order_ids: order_ids.clone(),
                        };
                        if trade_ledger
                            .get(&trade_id)
                            .is_some_and(|entry| entry.mutation != trade)
                        {
                            return Err(BotError::Journal(format!(
                                "account_trade_identity_conflict:{trade_id}"
                            )));
                        }
                        let existing = trade_ledger.get(&trade_id);
                        let latest_status = existing
                            .map(|entry| select_latest_trade_status(&entry.latest_status, &status))
                            .unwrap_or(status);
                        let reconciled = existing.is_some_and(|entry| entry.reconciled)
                            || reconciled_trade_ids.contains(&trade_id);
                        trade_ledger.insert(
                            trade_id.clone(),
                            TradeLedgerEntry {
                                mutation: trade,
                                latest_status,
                                reconciled,
                            },
                        );
                        trade_baselines.entry(trade_id).or_insert(mutation.baseline);
                        known_trade_order_ids.extend(order_ids);
                    }
                    UserEvent::Order { order_id, .. } => {
                        order_baselines.entry(order_id).or_insert(mutation.baseline);
                    }
                    UserEvent::Disconnect => {
                        return Err(BotError::Journal(
                            "disconnect_is_not_an_account_mutation".to_string(),
                        ));
                    }
                }
            }
            JournalEventKind::Reconciled => {
                let reconciliation: AccountReconciliationJournal =
                    serde_json::from_str(&record.payload).map_err(|error| {
                        BotError::Journal(format!("account_reconciliation_parse:{error}"))
                    })?;
                if reconciliation.schema_version != ACCOUNT_MUTATION_SCHEMA_VERSION {
                    return Err(BotError::Journal(
                        "account_reconciliation_schema_unsupported".to_string(),
                    ));
                }
                for trade_id in reconciliation.trade_ids {
                    reconciled_trade_ids.insert(trade_id.clone());
                    if let Some(entry) = trade_ledger.get_mut(&trade_id) {
                        entry.reconciled = true;
                    }
                }
            }
            _ => {}
        }
    }

    if reconciled_trade_ids
        .iter()
        .any(|trade_id| !trade_ledger.contains_key(trade_id))
    {
        return Err(BotError::Journal(
            "reconciliation_references_unknown_trade".to_string(),
        ));
    }
    let safety_latched = trade_ledger.values().any(|entry| {
        matches!(
            classify_trade_status(&entry.latest_status),
            TradeStatusClass::Failed | TradeStatusClass::Unknown
        )
    });
    let mut pending_trades = BTreeMap::new();
    let mut baseline: Option<LiveRiskSnapshot> = None;
    for (trade_id, entry) in &trade_ledger {
        if entry.reconciled
            || classify_trade_status(&entry.latest_status) != TradeStatusClass::Active
        {
            continue;
        }
        let trade_baseline = trade_baselines.get(trade_id).ok_or_else(|| {
            BotError::Journal(format!("account_trade_baseline_missing:{trade_id}"))
        })?;
        merge_account_baseline(&mut baseline, trade_baseline)?;
        pending_trades.insert(trade_id.clone(), entry.mutation.clone());
    }
    let unresolved_order_ids = order_baselines
        .keys()
        .filter(|order_id| !known_trade_order_ids.contains(*order_id))
        .cloned()
        .collect::<BTreeSet<_>>();
    for order_id in &unresolved_order_ids {
        let order_baseline = order_baselines.get(order_id).ok_or_else(|| {
            BotError::Journal(format!("account_order_baseline_missing:{order_id}"))
        })?;
        merge_account_baseline(&mut baseline, order_baseline)?;
    }
    let pending = if pending_trades.is_empty() && unresolved_order_ids.is_empty() {
        None
    } else {
        Some(PendingAccountReconciliation {
            baseline: baseline
                .ok_or_else(|| BotError::Journal("pending_account_baseline_missing".to_string()))?,
            trades: pending_trades,
            authoritative_order_ids: known_trade_order_ids.clone(),
            unresolved_order_ids,
        })
    };
    Ok(RecoveredAccountLedger {
        trade_ledger,
        known_trade_order_ids,
        pending,
        safety_latched,
    })
}

fn merge_account_baseline(
    target: &mut Option<LiveRiskSnapshot>,
    candidate: &LiveRiskSnapshot,
) -> Result<()> {
    if target
        .as_ref()
        .is_some_and(|existing| !account_baselines_match(existing, candidate))
    {
        return Err(BotError::Journal(
            "pending_account_baseline_conflict".to_string(),
        ));
    }
    target.get_or_insert_with(|| candidate.clone());
    Ok(())
}

fn account_baselines_match(left: &LiveRiskSnapshot, right: &LiveRiskSnapshot) -> bool {
    left.collateral_balance_usdc == right.collateral_balance_usdc
        && left.positions.len() == right.positions.len()
        && left.positions.iter().zip(&right.positions).all(|(a, b)| {
            a.asset_id == b.asset_id && a.size == b.size && a.average_price == b.average_price
        })
}

fn publish_paper_fills(shared_state: &SharedRuntimeState, fills: &[PaperFill]) -> Result<()> {
    for fill in fills {
        update_state(shared_state, |state| {
            state.push_fill(RuntimeFill::from_paper(fill));
            state.push_event(
                fill.filled_at_ms,
                RuntimeEventLevel::Info,
                "paper_fill",
                format!("{} {} @ {}", fill.side_label(), fill.size, fill.price),
            );
        })?;
    }
    Ok(())
}

trait PaperFillLabel {
    fn side_label(&self) -> &'static str;
}

impl PaperFillLabel for PaperFill {
    fn side_label(&self) -> &'static str {
        match self.side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }
}

fn wallet_copy_intent(
    trade: &WalletTradeObservation,
    book: &BookState,
    size: crate::fixed::Fixed,
    now_ms: u64,
    ttl_ms: u64,
) -> Option<OrderIntent> {
    let size = size.floor_to_decimals(2).ok()?;
    if size <= crate::fixed::Fixed::ZERO {
        return None;
    }
    let price = match trade.side {
        Side::Buy => {
            let price = book.best_bid?.checked_add(book.tick_size).ok()?;
            (price < book.best_ask?).then_some(price)?
        }
        Side::Sell => {
            let price = book.best_ask?.checked_sub(book.tick_size).ok()?;
            (price > book.best_bid?).then_some(price)?
        }
    };
    Some(OrderIntent {
        asset_id: AssetId::from(trade.asset_id.clone()),
        side: trade.side,
        limit_price: price,
        size,
        time_in_force: TimeInForce::Gtc,
        post_only: true,
        local_expires_at_ms: now_ms.checked_add(ttl_ms)?,
        wire_expiration_s: None,
        reason: "public_wallet_observation".to_string(),
        strategy_id: format!("wallet-shadow:{}", trade.wallet),
        feature_snapshot_id: format!("wallet-tx:{}", trade.transaction_hash),
    })
}

fn midpoint(book: &BookState) -> Option<crate::fixed::Fixed> {
    book.best_bid?
        .checked_add(book.best_ask?)
        .ok()?
        .checked_div_int(2)
        .ok()
}

fn external_gate_ready(settings: &Settings, cache: &ExternalFeatureCache, now_ms: u64) -> bool {
    !settings.enable_external_signal
        || settings.external_symbols.iter().all(|symbol| {
            cache
                .fresh_quote(symbol, now_ms, settings.external_stale_ms)
                .is_some()
        })
}

fn event_asset_ids(event: &MarketEvent) -> Vec<AssetId> {
    match event {
        MarketEvent::Book { asset_id, .. }
        | MarketEvent::TickSizeChange { asset_id, .. }
        | MarketEvent::LastTradePrice { asset_id, .. }
        | MarketEvent::BestBidAsk { asset_id, .. } => vec![asset_id.clone()],
        MarketEvent::PriceChange { price_changes, .. } => {
            let mut assets = Vec::new();
            for change in price_changes {
                if !assets.contains(&change.asset_id) {
                    assets.push(change.asset_id.clone());
                }
            }
            assets
        }
        MarketEvent::MarketResolved { asset_ids, .. } => asset_ids.clone(),
        MarketEvent::NewMarket { .. } => Vec::new(),
    }
}

fn update_compliance_state(
    shared_state: &SharedRuntimeState,
    compliance: &ComplianceState,
    settings: &Settings,
) -> Result<()> {
    let now_ms = system_now_ms();
    let status = match compliance.eligibility_status_at(now_ms, settings.max_geoblock_age_ms) {
        EligibilityStatus::Unverified => "unverified",
        EligibilityStatus::InvalidFuture => "invalid_future",
        EligibilityStatus::Stale => "stale",
        EligibilityStatus::Blocked => "blocked",
        EligibilityStatus::VenueReview => "venue_review",
        EligibilityStatus::Eligible => "eligible",
    };
    update_state(shared_state, |state| {
        state.compliance_status = status.to_string();
        state.compliance_location = compliance.location_label();
        state.push_event(
            now_ms,
            RuntimeEventLevel::Info,
            "compliance",
            format!("geographic eligibility: {status}"),
        );
    })
}

fn spawn_market_task(
    settings: &Settings,
    sender: mpsc::Sender<MarketFeedMessage>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    let endpoint = settings.market_ws_endpoint.clone();
    let assets = settings.asset_ids.clone();
    let reconnect_min = settings.reconnect_min_ms;
    let reconnect_max = settings.reconnect_max_ms;
    tokio::spawn(async move {
        let error_sender = sender.clone();
        if let Err(error) = run_market_feed(
            endpoint,
            assets,
            reconnect_min,
            reconnect_max,
            sender,
            shutdown,
        )
        .await
        {
            let _ = error_sender
                .send(MarketFeedMessage::Disconnected(error.to_string()))
                .await;
        }
    })
}

fn spawn_user_task(
    settings: &Settings,
    signer_address: String,
    account_revision: Arc<AtomicU64>,
    sender: mpsc::Sender<UserFeedMessage>,
    shutdown: CancellationToken,
) -> Result<JoinHandle<()>> {
    let auth = UserWsAuth::new(
        &required_environment("POLYMARKET_API_KEY")?,
        required_environment("POLYMARKET_API_SECRET")?,
        required_environment("POLYMARKET_API_PASSPHRASE")?,
        &signer_address,
    )?;
    let endpoint = settings.user_ws_endpoint.clone();
    let conditions = settings.condition_ids.clone();
    let settings_reconnect_min = settings.reconnect_min_ms;
    let settings_reconnect_max = settings.reconnect_max_ms;
    Ok(tokio::spawn(async move {
        let error_sender = sender.clone();
        let error_revision = Arc::clone(&account_revision);
        if let Err(error) = run_user_feed(
            endpoint,
            conditions,
            auth,
            settings_reconnect_min,
            settings_reconnect_max,
            account_revision,
            sender,
            shutdown,
        )
        .await
        {
            let revision = error_revision
                .fetch_add(1, Ordering::AcqRel)
                .saturating_add(1);
            let _ = error_sender
                .send(UserFeedMessage::Disconnected {
                    revision,
                    error: error.to_string(),
                })
                .await;
        }
    }))
}

fn spawn_external_task(
    settings: &Settings,
    sender: mpsc::Sender<ExternalFeedMessage>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    let endpoint = settings.coinbase_ws_endpoint.clone();
    let symbols = settings.external_symbols.clone();
    let reconnect_min = settings.reconnect_min_ms;
    let reconnect_max = settings.reconnect_max_ms;
    tokio::spawn(async move {
        let error_sender = sender.clone();
        if let Err(error) = run_coinbase_feed(
            endpoint,
            symbols,
            reconnect_min,
            reconnect_max,
            sender,
            shutdown,
        )
        .await
        {
            let _ = error_sender
                .send(ExternalFeedMessage::Disconnected(error.to_string()))
                .await;
        }
    })
}

fn spawn_wallet_task(
    settings: &Settings,
    sender: mpsc::Sender<WalletWatchMessage>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    let host = settings.data_api_host.clone();
    let wallets = settings.watched_wallets.clone();
    let poll_ms = settings.wallet_poll_interval_ms;
    tokio::spawn(async move {
        let error_sender = sender.clone();
        if let Err(error) = run_wallet_watch(host, wallets, poll_ms, sender, shutdown).await {
            let _ = error_sender
                .send(WalletWatchMessage::Disconnected(error.to_string()))
                .await;
        }
    })
}

fn spawn_compliance_task(
    url: String,
    max_age_ms: u64,
    sender: mpsc::Sender<Result<ComplianceState>>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let refresh_ms = (max_age_ms / 2).clamp(250, 30_000);
        let mut ticker = interval(Duration::from_millis(refresh_ms));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = ticker.tick() => {
                    let result = fetch_geoblock_state(&url, system_now_ms()).await;
                    if sender.send(result).await.is_err() {
                        return;
                    }
                }
            }
        }
    })
}

fn spawn_catalog_task(
    clob_host: String,
    configured: Vec<(AssetId, crate::types::ConditionId)>,
    sender: mpsc::Sender<Result<MarketCatalog>>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(60));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = ticker.tick() => {
                    let result = discover_configured_markets(&clob_host, configured.clone()).await;
                    if sender.send(result).await.is_err() {
                        return;
                    }
                }
            }
        }
    })
}

fn acquire_cancel(limiter: &mut SlidingWindowRateLimiter, now_ms: u64) -> Result<()> {
    if limiter.try_acquire(now_ms) {
        Ok(())
    } else {
        Err(BotError::Execution(format!(
            "cancel_rate_limited_retry_after_ms:{}",
            limiter.retry_after_ms(now_ms)
        )))
    }
}

fn tracked_stale_order_ids(runtime: &LiveRuntime, now_ms: u64, ttl_ms: u64) -> Vec<String> {
    runtime
        .order_clients
        .iter()
        .filter(|(_, tracked)| now_ms.saturating_sub(tracked.submitted_at_ms) >= ttl_ms)
        .map(|(exchange_id, _)| exchange_id.clone())
        .collect()
}

fn validate_reconnect_snapshot(
    tracked: &BTreeMap<String, RecoveredOpenOrder>,
    remote: &RemoteSnapshot,
) -> Result<()> {
    if !remote.open_orders_loaded || !remote.trades_loaded || !remote.unresolved_gaps.is_empty() {
        return Err(BotError::Readiness(
            "user_ws_resnapshot_incomplete".to_string(),
        ));
    }
    if remote
        .open_order_ids
        .iter()
        .any(|order_id| !tracked.contains_key(order_id))
    {
        return Err(BotError::Readiness(
            "user_ws_resnapshot_unmapped_remote_order".to_string(),
        ));
    }
    if tracked.keys().any(|order_id| {
        !remote.open_order_ids.contains(order_id) && !remote.trade_order_ids.contains(order_id)
    }) {
        return Err(BotError::Readiness(
            "user_ws_resnapshot_tracked_order_unresolved".to_string(),
        ));
    }
    Ok(())
}

fn journal_authoritative_terminal_orders(
    journal: &mut Journal,
    attempts: &[UnresolvedOrderAttempt],
    statuses: &BTreeMap<String, crate::types::OrderStatus>,
    remote: &RemoteSnapshot,
    reconciled_at_ms: u64,
) -> Result<usize> {
    for attempt in attempts {
        let status = statuses.get(&attempt.exchange_order_id).ok_or_else(|| {
            BotError::Readiness(format!(
                "authoritative_order_status_missing:{}",
                attempt.exchange_order_id
            ))
        })?;
        if *status == crate::types::OrderStatus::Unknown {
            return Err(BotError::Readiness(format!(
                "authoritative_order_status_unknown:{}",
                attempt.exchange_order_id
            )));
        }
    }
    let mut committed = 0usize;
    for attempt in attempts {
        let status = statuses
            .get(&attempt.exchange_order_id)
            .ok_or_else(|| BotError::Readiness("order_status_preflight_lost".to_string()))?;
        let event_kind = match status {
            crate::types::OrderStatus::Filled
                if remote.trade_order_ids.contains(&attempt.exchange_order_id) =>
            {
                Some(JournalEventKind::Filled)
            }
            crate::types::OrderStatus::Cancelled => Some(JournalEventKind::Cancelled),
            crate::types::OrderStatus::Rejected => Some(JournalEventKind::Rejected),
            _ => None,
        };
        let Some(event_kind) = event_kind else {
            continue;
        };
        journal.append_lifecycle(
            event_kind,
            attempt.decision_id.clone(),
            attempt.client_order_id.clone(),
            Some(attempt.exchange_order_id.clone()),
            attempt.strategy_id.clone(),
            "authoritative_startup_reconciliation",
            format!("order_lookup_status={status:?};reconciled_at_ms={reconciled_at_ms}"),
        )?;
        committed = committed.saturating_add(1);
    }
    Ok(committed)
}

async fn latch_cancel_uncertainty(
    runtime: &mut LiveRuntime,
    journal: &mut Journal,
    shared_state: &SharedRuntimeState,
    now_ms: u64,
    reason: &str,
    error: &BotError,
) -> Result<()> {
    runtime.safety_latched = true;
    runtime.own_orders.certain = false;
    update_state(shared_state, |state| {
        state.live_submission_enabled = false;
        state.paused = true;
        state.strategy_enabled = false;
        state.phase = RuntimePhase::Degraded;
        state.push_event(
            now_ms,
            RuntimeEventLevel::Error,
            "live_cancel",
            format!(
                "cancellation state uncertain after {reason}; restart reconciliation required: {error}"
            ),
        );
    })?;
    match emergency_cancel_all_account(runtime, journal, "cancel_state_uncertain").await {
        Ok(canceled) => update_state(shared_state, |state| {
            state.push_event(
                now_ms,
                RuntimeEventLevel::Warning,
                "live_cancel",
                format!(
                    "account-wide safety cancel attempted after uncertainty; {} order(s) confirmed cancelled",
                    canceled.len()
                ),
            );
        })?,
        Err(cancel_all_error) => update_state(shared_state, |state| {
            state.push_event(
                now_ms,
                RuntimeEventLevel::Error,
                "live_cancel",
                format!("account-wide safety cancel also failed: {cancel_all_error}"),
            );
        })?,
    }
    Ok(())
}

async fn cancel_tracked_live_orders(
    runtime: &mut LiveRuntime,
    journal: &mut Journal,
    exchange_ids: Vec<String>,
    reason: &str,
) -> Result<Vec<String>> {
    if exchange_ids.is_empty() {
        return Ok(Vec::new());
    }
    let requested = exchange_ids.iter().cloned().collect::<BTreeSet<_>>();
    let result = runtime
        .router
        .adapter()
        .cancel_orders_tracked(&exchange_ids)
        .await?;
    let canceled = result.canceled.iter().cloned().collect::<BTreeSet<_>>();
    let not_canceled = result.not_canceled.keys().cloned().collect::<BTreeSet<_>>();
    let covered = canceled
        .union(&not_canceled)
        .cloned()
        .collect::<BTreeSet<_>>();
    if covered != requested || !canceled.is_disjoint(&not_canceled) {
        return Err(BotError::Protocol(
            "tracked_cancel_response_identity_mismatch".to_string(),
        ));
    }
    journal_live_cancellations(journal, runtime, &result.canceled, reason)?;
    if !result.not_canceled.is_empty() {
        return Err(BotError::Execution(format!(
            "tracked_cancel_partial_failure:{}",
            result.not_canceled.len()
        )));
    }
    Ok(result.canceled)
}

async fn emergency_cancel_all_account(
    runtime: &mut LiveRuntime,
    journal: &mut Journal,
    reason: &str,
) -> Result<Vec<String>> {
    let canceled = runtime.router.adapter().cancel_all().await?;
    journal_live_cancellations(journal, runtime, &canceled, reason)?;
    Ok(canceled)
}

fn journal_live_cancellations(
    journal: &mut Journal,
    runtime: &mut LiveRuntime,
    exchange_ids: &[String],
    reason: &str,
) -> Result<()> {
    for exchange_id in exchange_ids {
        let client_order_id = runtime
            .order_clients
            .get(exchange_id)
            .map(|tracked| tracked.client_order_id.clone())
            .unwrap_or_else(|| format!("reconciled-{exchange_id}"));
        journal.append_lifecycle(
            JournalEventKind::Cancelled,
            format!("decision-{client_order_id}"),
            client_order_id,
            Some(exchange_id.clone()),
            "live",
            "cancelled",
            reason,
        )?;
        runtime.order_clients.remove(exchange_id);
    }
    Ok(())
}

fn journal_user_event(
    journal: &mut Journal,
    runtime: &LiveRuntime,
    event: &UserEvent,
) -> Result<()> {
    if user_event_requires_holdings_reconciliation(event) {
        let baseline = runtime.pending_account_reconciliation.as_ref().map_or_else(
            || runtime.last_account_snapshot.clone(),
            |pending| pending.baseline.clone(),
        );
        append_account_mutation(journal, event, &baseline)?;
    }
    match event {
        UserEvent::Order {
            order_id, status, ..
        } => {
            let Some(tracked) = runtime.order_clients.get(order_id) else {
                return Ok(());
            };
            let client_order_id = &tracked.client_order_id;
            let lower = status.to_ascii_lowercase();
            let kind = if lower == "matched" {
                JournalEventKind::Filled
            } else if order_status_is_terminal(status) {
                JournalEventKind::Cancelled
            } else if lower.contains("partial") || lower.contains("update") {
                JournalEventKind::PartiallyFilled
            } else {
                JournalEventKind::Acknowledged
            };
            journal.append_lifecycle(
                kind,
                format!("decision-{client_order_id}"),
                client_order_id,
                Some(order_id.clone()),
                "live",
                status,
                "authenticated_user_ws_order",
            )?;
        }
        UserEvent::Trade {
            order_ids, status, ..
        } => {
            for order_id in order_ids {
                let Some(tracked) = runtime.order_clients.get(order_id) else {
                    continue;
                };
                let client_order_id = &tracked.client_order_id;
                journal.append_lifecycle(
                    JournalEventKind::PartiallyFilled,
                    format!("decision-{client_order_id}"),
                    client_order_id,
                    Some(order_id.clone()),
                    "live",
                    status,
                    "authenticated_user_ws_trade",
                )?;
            }
        }
        UserEvent::Disconnect => {}
    }
    Ok(())
}

fn append_account_mutation(
    journal: &mut Journal,
    event: &UserEvent,
    baseline: &LiveRiskSnapshot,
) -> Result<()> {
    if !user_event_requires_holdings_reconciliation(event) {
        return Err(BotError::Journal(
            "non_mutating_event_cannot_be_account_mutation".to_string(),
        ));
    }
    let (identity, status) = match event {
        UserEvent::Trade {
            trade_id, status, ..
        } => (format!("trade-{trade_id}"), status.as_str()),
        UserEvent::Order {
            order_id, status, ..
        } => (format!("order-{order_id}"), status.as_str()),
        UserEvent::Disconnect => {
            return Err(BotError::Journal(
                "disconnect_is_not_an_account_mutation".to_string(),
            ));
        }
    };
    let mutation = AccountMutationJournal {
        schema_version: ACCOUNT_MUTATION_SCHEMA_VERSION,
        event: event.clone(),
        baseline: baseline.clone(),
    };
    journal.append_lifecycle(
        JournalEventKind::AccountMutation,
        format!("account-{identity}"),
        format!("account-{identity}"),
        None,
        "live_account",
        status,
        serde_json::to_string(&mutation)
            .map_err(|error| BotError::Journal(format!("account_mutation_serialize:{error}")))?,
    )?;
    Ok(())
}

fn short_identifier(value: &str) -> String {
    let character_count = value.chars().count();
    if character_count <= 16 {
        value.to_string()
    } else {
        let leading = value.chars().take(8).collect::<String>();
        let trailing = value
            .chars()
            .skip(character_count.saturating_sub(6))
            .collect::<String>();
        format!("{leading}…{trailing}")
    }
}

fn daily_loss(
    baseline: crate::fixed::Fixed,
    current: crate::fixed::Fixed,
) -> Result<crate::fixed::Fixed> {
    if current < baseline {
        baseline.checked_sub(current)
    } else {
        Ok(crate::fixed::Fixed::ZERO)
    }
}

fn apply_equity_high_water(
    baseline: &mut DailyRiskBaselineGuard,
    account: &mut LiveRiskSnapshot,
    now_ms: u64,
) -> Result<bool> {
    let date_changed = baseline.observe_and_rollover(account.current_equity_usdc, now_ms)?;
    account.risk_state.daily_loss_usdc = daily_loss(
        baseline.value.baseline_equity_usdc,
        account.current_equity_usdc,
    )?;
    Ok(date_changed)
}

fn pending_account_reconciled(
    pending: &PendingAccountReconciliation,
    current: &LiveRiskSnapshot,
    authoritative_trade_proof: &AuthoritativePendingTradeProof,
) -> Result<bool> {
    if pending.trades.is_empty() || !pending.unresolved_order_ids.is_empty() {
        return Ok(false);
    }
    let mut position_deltas = BTreeMap::<AssetId, crate::fixed::Fixed>::new();
    let mut collateral_delta = crate::fixed::Fixed::ZERO;
    for trade in pending.trades.values() {
        let signed_size = match trade.side {
            Side::Buy => trade.size,
            Side::Sell => crate::fixed::Fixed::ZERO.checked_sub(trade.size)?,
        };
        let entry = position_deltas
            .entry(trade.asset_id.clone())
            .or_insert(crate::fixed::Fixed::ZERO);
        *entry = entry.checked_add(signed_size)?;

        let notional = trade.price.checked_mul_ceil(trade.size)?;
        collateral_delta = match trade.side {
            Side::Buy => collateral_delta.checked_sub(notional)?,
            Side::Sell => collateral_delta.checked_add(notional)?,
        };
    }
    let mut expected_positions = account_position_sizes(&pending.baseline)?;
    for (asset_id, delta) in position_deltas {
        let baseline_size = expected_positions
            .get(&asset_id)
            .copied()
            .unwrap_or(crate::fixed::Fixed::ZERO);
        let expected_size = baseline_size.checked_add(delta)?;
        if expected_size < crate::fixed::Fixed::ZERO {
            return Ok(false);
        }
        expected_positions.insert(asset_id, expected_size);
    }
    expected_positions.retain(|_, size| *size != crate::fixed::Fixed::ZERO);
    if account_position_sizes(current)? != expected_positions {
        return Ok(false);
    }

    let baseline_cash = pending.baseline.collateral_balance_usdc;
    let current_cash = current.collateral_balance_usdc;
    let no_fee_expected = baseline_cash.checked_add(collateral_delta)?;
    let all_trades_authoritative = pending
        .trades
        .keys()
        .all(|trade_id| authoritative_trade_proof.trade_ids.contains(trade_id));
    if !all_trades_authoritative
        || authoritative_trade_proof.cash_fee_lower_bound_usdc
            > authoritative_trade_proof.cash_fee_upper_bound_usdc
    {
        return Ok(false);
    }

    let minimum_cash =
        no_fee_expected.checked_sub(authoritative_trade_proof.cash_fee_upper_bound_usdc)?;
    let maximum_cash =
        no_fee_expected.checked_sub(authoritative_trade_proof.cash_fee_lower_bound_usdc)?;
    Ok(current_cash >= minimum_cash && current_cash <= maximum_cash)
}

fn account_position_sizes(
    snapshot: &LiveRiskSnapshot,
) -> Result<BTreeMap<AssetId, crate::fixed::Fixed>> {
    let mut sizes = BTreeMap::new();
    for position in &snapshot.positions {
        if position.size < crate::fixed::Fixed::ZERO {
            return Err(BotError::Protocol(
                "negative_position_in_account_snapshot".to_string(),
            ));
        }
        let total = sizes
            .get(&position.asset_id)
            .copied()
            .unwrap_or(crate::fixed::Fixed::ZERO)
            .checked_add(position.size)?;
        sizes.insert(position.asset_id.clone(), total);
    }
    sizes.retain(|_, size| *size != crate::fixed::Fixed::ZERO);
    Ok(sizes)
}

fn open_daily_risk_baseline(
    path: impl AsRef<Path>,
    current_equity: crate::fixed::Fixed,
    now_ms: u64,
) -> Result<DailyRiskBaselineGuard> {
    if current_equity <= crate::fixed::Fixed::ZERO {
        return Err(BotError::Readiness(
            "live_equity_must_be_positive".to_string(),
        ));
    }
    let path = path.as_ref();
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    if path.exists() {
        harden_permissions(path)?;
    }
    let lock_path = path.with_extension(format!(
        "{}.lock",
        path.extension()
            .and_then(|value| value.to_str())
            .unwrap_or("baseline")
    ));
    let lock = secure_open(&lock_path)?;
    lock.try_lock_exclusive().map_err(|error| {
        BotError::Readiness(format!("live_risk_baseline_already_locked:{error}"))
    })?;

    let utc_date = utc_date_for_ms(now_ms)?;
    let value = if path.exists() {
        let raw = std::fs::read_to_string(path)?;
        let existing: DailyRiskBaseline = serde_json::from_str(&raw)
            .map_err(|error| BotError::Readiness(format!("risk_baseline_parse:{error}")))?;
        if existing.schema_version != 1
            || existing.baseline_equity_usdc <= crate::fixed::Fixed::ZERO
            || existing.written_at_ms > now_ms
        {
            return Err(BotError::Readiness("risk_baseline_invalid".to_string()));
        }
        if existing.utc_date == utc_date && existing.baseline_equity_usdc >= current_equity {
            existing
        } else {
            let fresh = DailyRiskBaseline {
                schema_version: 1,
                utc_date,
                baseline_equity_usdc: std::cmp::max(existing.baseline_equity_usdc, current_equity),
                written_at_ms: now_ms,
            };
            write_baseline_atomic(path, &fresh)?;
            fresh
        }
    } else {
        let fresh = DailyRiskBaseline {
            schema_version: 1,
            utc_date,
            baseline_equity_usdc: current_equity,
            written_at_ms: now_ms,
        };
        write_baseline_atomic(path, &fresh)?;
        fresh
    };
    Ok(DailyRiskBaselineGuard {
        _lock: lock,
        path: path.to_path_buf(),
        value,
    })
}

fn write_baseline_atomic(path: &Path, value: &DailyRiskBaseline) -> Result<()> {
    let temporary = path.with_extension(format!(
        "{}.tmp-{}",
        path.extension()
            .and_then(|value| value.to_str())
            .unwrap_or("baseline"),
        uuid::Uuid::new_v4()
    ));
    {
        let mut file = secure_create_new(&temporary)?;
        serde_json::to_writer(&mut file, value)
            .map_err(|error| BotError::Io(format!("risk_baseline_serialize:{error}")))?;
        writeln!(file)?;
        file.sync_all()?;
    }
    std::fs::rename(&temporary, path)?;
    sync_parent(path)?;
    Ok(())
}

fn utc_date_for_ms(now_ms: u64) -> Result<String> {
    let timestamp = i64::try_from(now_ms)
        .ok()
        .and_then(chrono::DateTime::<Utc>::from_timestamp_millis)
        .ok_or_else(|| BotError::Readiness("utc_baseline_timestamp_invalid".to_string()))?;
    Ok(timestamp.date_naive().to_string())
}

fn secure_open(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    harden_permissions(path)?;
    Ok(file)
}

fn secure_create_new(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create_new(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    options.open(path).map_err(Into::into)
}

fn harden_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn required_environment(name: &str) -> Result<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| BotError::Config(format!("missing_secret:{name}")))
}

fn command_outcome(
    action: &str,
    status: &str,
    message: impl Into<String>,
    affected_orders: usize,
) -> CommandOutcome {
    CommandOutcome {
        action: action.to_string(),
        status: status.to_string(),
        message: message.into(),
        affected_orders,
    }
}

fn readiness_flags(readiness: &ReadinessState) -> BTreeMap<String, bool> {
    [
        ("Protocol config", readiness.protocol_verified),
        ("Wallet path", readiness.wallet_path_verified),
        ("Signer authorized", readiness.signer_authorized),
        ("Funder verified", readiness.funder_verified),
        ("Balance", readiness.balance_verified),
        ("Allowance", readiness.allowance_verified),
        ("API credentials", readiness.api_credentials_verified),
        ("Market parameters", readiness.market_parameters_verified),
        ("Clock", readiness.clock_synced),
        ("Journal", readiness.journal_verified),
        ("Heartbeat", readiness.heartbeat_ready),
        ("Startup reconciliation", readiness.reconciled_after_startup),
    ]
    .into_iter()
    .map(|(name, passed)| (name.to_string(), passed))
    .collect()
}

fn update_state(
    shared_state: &SharedRuntimeState,
    update: impl FnOnce(&mut RuntimeState),
) -> Result<()> {
    let mut state = shared_state
        .write()
        .map_err(|_| BotError::Execution("runtime_state_lock_poisoned".to_string()))?;
    update(&mut state);
    Ok(())
}

fn read_state<T>(
    shared_state: &SharedRuntimeState,
    read: impl FnOnce(&RuntimeState) -> T,
) -> Result<T> {
    let state = shared_state
        .read()
        .map_err(|_| BotError::Execution("runtime_state_lock_poisoned".to_string()))?;
    Ok(read(&state))
}

fn hash_file(path: &str) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn hash_current_binary() -> Result<String> {
    let path = std::env::current_exe()?;
    hash_file(
        path.to_str()
            .ok_or_else(|| BotError::Io("binary_path_not_utf8".to_string()))?,
    )
}

fn elapsed_us(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

pub fn system_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixed::Fixed;
    use crate::types::Level;
    use std::sync::atomic::AtomicUsize;

    struct FailingSnapshotAdapter {
        cancel_all_calls: Arc<AtomicUsize>,
        cancel_tracked_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ExecutionAdapter for FailingSnapshotAdapter {
        async fn submit(
            &mut self,
            _intent: &OrderIntent,
            _client_order_id: &str,
            _now_ms: u64,
        ) -> Result<crate::execution::ExecutionResult> {
            Err(BotError::Execution("unexpected_submit".to_string()))
        }
    }

    #[async_trait]
    impl LiveAdapter for FailingSnapshotAdapter {
        async fn post_heartbeat(&mut self) -> Result<()> {
            Ok(())
        }

        async fn cancel_all(&self) -> Result<Vec<String>> {
            self.cancel_all_calls.fetch_add(1, Ordering::AcqRel);
            Ok(Vec::new())
        }

        async fn cancel_orders_tracked(&self, order_ids: &[String]) -> Result<LiveCancelResult> {
            self.cancel_tracked_calls.fetch_add(1, Ordering::AcqRel);
            Ok(LiveCancelResult {
                canceled: order_ids.to_vec(),
                not_canceled: BTreeMap::new(),
            })
        }

        async fn remote_snapshot(&self) -> Result<RemoteSnapshot> {
            Err(BotError::Protocol("injected_snapshot_timeout".to_string()))
        }

        async fn risk_snapshot(
            &self,
            _data_api_host: &str,
            _funder_address: &str,
            _market_asset: &AssetId,
            _daily_baseline_equity_usdc: crate::fixed::Fixed,
            _prohibited_conduct_flag: bool,
            _now_ms: u64,
        ) -> Result<LiveRiskSnapshot> {
            Err(BotError::Protocol("unexpected_risk_snapshot".to_string()))
        }
    }

    struct AuditingSnapshotAdapter {
        snapshot: LiveRiskSnapshot,
        remote: RemoteSnapshot,
    }

    #[async_trait]
    impl ExecutionAdapter for AuditingSnapshotAdapter {
        async fn submit(
            &mut self,
            _intent: &OrderIntent,
            _client_order_id: &str,
            _now_ms: u64,
        ) -> Result<crate::execution::ExecutionResult> {
            Err(BotError::Execution("unexpected_submit".to_string()))
        }
    }

    #[async_trait]
    impl LiveAdapter for AuditingSnapshotAdapter {
        async fn post_heartbeat(&mut self) -> Result<()> {
            Ok(())
        }

        async fn cancel_all(&self) -> Result<Vec<String>> {
            Ok(Vec::new())
        }

        async fn cancel_orders_tracked(&self, _order_ids: &[String]) -> Result<LiveCancelResult> {
            Ok(LiveCancelResult {
                canceled: Vec::new(),
                not_canceled: BTreeMap::new(),
            })
        }

        async fn remote_snapshot(&self) -> Result<RemoteSnapshot> {
            Ok(self.remote.clone())
        }

        async fn risk_snapshot(
            &self,
            _data_api_host: &str,
            _funder_address: &str,
            _market_asset: &AssetId,
            _daily_baseline_equity_usdc: crate::fixed::Fixed,
            _prohibited_conduct_flag: bool,
            _now_ms: u64,
        ) -> Result<LiveRiskSnapshot> {
            Ok(self.snapshot.clone())
        }
    }

    #[test]
    fn wallet_copy_intent_remains_post_only() {
        let mut book = BookState::empty("1", "0.01".parse().unwrap(), "1".parse().unwrap());
        book.best_bid = Some("0.45".parse().unwrap());
        book.best_ask = Some("0.55".parse().unwrap());
        let trade = WalletTradeObservation {
            wallet: "0x0000000000000000000000000000000000000001".to_string(),
            transaction_hash: "0x1".to_string(),
            asset_id: "1".to_string(),
            condition_id: "0x1".to_string(),
            side: Side::Buy,
            size: "2".parse().unwrap(),
            price: "0.46".parse().unwrap(),
            timestamp_ms: 1,
            title: "q".to_string(),
            slug: "q".to_string(),
            outcome: "Yes".to_string(),
        };
        let intent = wallet_copy_intent(&trade, &book, "1".parse().unwrap(), 10, 100).unwrap();
        assert!(intent.post_only);
        assert_eq!(intent.limit_price.to_string(), "0.46");
    }

    #[test]
    fn external_gate_requires_every_configured_quote_to_be_fresh() {
        let settings = Settings {
            enable_external_signal: true,
            external_symbols: vec!["BTC-USD".to_string()],
            ..Settings::default()
        };
        assert!(!external_gate_ready(
            &settings,
            &ExternalFeatureCache::default(),
            100
        ));
    }

    #[test]
    fn midpoint_uses_checked_fixed_point_math() {
        let mut book = BookState::empty("1", "0.01".parse().unwrap(), "1".parse().unwrap());
        book.bids.push(Level {
            price: "0.4".parse().unwrap(),
            size: "1".parse().unwrap(),
        });
        book.best_bid = Some("0.4".parse().unwrap());
        book.best_ask = Some("0.6".parse().unwrap());
        assert_eq!(midpoint(&book).unwrap().to_string(), "0.5");
    }

    #[test]
    fn reconnect_snapshot_rejects_unmapped_remote_orders() {
        let remote = RemoteSnapshot {
            open_orders_loaded: true,
            trades_loaded: true,
            balances_loaded: true,
            allowances_loaded: true,
            open_order_ids: ["foreign".to_string()].into_iter().collect(),
            ..RemoteSnapshot::default()
        };

        let error = validate_reconnect_snapshot(&BTreeMap::new(), &remote).unwrap_err();

        assert!(error.to_string().contains("unmapped_remote_order"));
    }

    #[test]
    fn reconnect_snapshot_requires_every_tracked_order_to_resolve() {
        let tracked = [(
            "ours".to_string(),
            RecoveredOpenOrder {
                client_order_id: "client-1".to_string(),
                submitted_at_ms: 1,
                asset_id: AssetId::from("1"),
            },
        )]
        .into_iter()
        .collect();
        let mut remote = RemoteSnapshot {
            open_orders_loaded: true,
            trades_loaded: true,
            balances_loaded: true,
            allowances_loaded: true,
            ..RemoteSnapshot::default()
        };
        assert!(validate_reconnect_snapshot(&tracked, &remote).is_err());

        remote.trade_order_ids.insert("ours".to_string());
        assert!(validate_reconnect_snapshot(&tracked, &remote).is_ok());
    }

    #[test]
    fn authoritative_order_lookup_repairs_cancelled_before_journal_crash_window() {
        let path = std::env::temp_dir().join(format!(
            "polymarket-cancel-recovery-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        journal
            .append_write_ahead(
                "decision-1",
                "client-1",
                "strategy",
                "accepted",
                "intent_v2|asset_id_hex=31",
            )
            .unwrap();
        journal
            .append_lifecycle(
                JournalEventKind::Acknowledged,
                "decision-1",
                "client-1",
                Some("exchange-1".to_string()),
                "strategy",
                "acknowledged",
                "live",
            )
            .unwrap();
        let attempts = unresolved_order_attempts(&journal.records_locked().unwrap());
        let statuses = [(
            "exchange-1".to_string(),
            crate::types::OrderStatus::Cancelled,
        )]
        .into_iter()
        .collect();

        assert_eq!(
            journal_authoritative_terminal_orders(
                &mut journal,
                &attempts,
                &statuses,
                &RemoteSnapshot::default(),
                10,
            )
            .unwrap(),
            1
        );
        let remote = RemoteSnapshot {
            open_orders_loaded: true,
            trades_loaded: true,
            balances_loaded: true,
            allowances_loaded: true,
            ..RemoteSnapshot::default()
        };
        assert!(
            recover_after_crash_records(&journal.records_locked().unwrap(), &remote)
                .unwrap()
                .live_unlock_allowed
        );
        drop(journal);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("jsonl.checkpoint"));
    }

    #[test]
    fn authoritative_repair_resumes_between_multi_order_terminal_writes() {
        let path = std::env::temp_dir().join(format!(
            "polymarket-multi-cancel-recovery-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        for index in 1..=2 {
            journal
                .append_write_ahead(
                    format!("decision-{index}"),
                    format!("client-{index}"),
                    "strategy",
                    "accepted",
                    "intent_v2|asset_id_hex=31",
                )
                .unwrap();
            journal
                .append_lifecycle(
                    JournalEventKind::Acknowledged,
                    format!("decision-{index}"),
                    format!("client-{index}"),
                    Some(format!("exchange-{index}")),
                    "strategy",
                    "acknowledged",
                    "live",
                )
                .unwrap();
        }
        let statuses = [
            (
                "exchange-1".to_string(),
                crate::types::OrderStatus::Cancelled,
            ),
            (
                "exchange-2".to_string(),
                crate::types::OrderStatus::Cancelled,
            ),
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>();
        let attempts = unresolved_order_attempts(&journal.records_locked().unwrap());
        journal.fail_append_after_for_test(1);
        assert!(journal_authoritative_terminal_orders(
            &mut journal,
            &attempts,
            &statuses,
            &RemoteSnapshot::default(),
            10,
        )
        .is_err());
        drop(journal);

        let mut reopened = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        let remaining = unresolved_order_attempts(&reopened.records_locked().unwrap());
        assert_eq!(remaining.len(), 1);
        assert_eq!(
            journal_authoritative_terminal_orders(
                &mut reopened,
                &remaining,
                &statuses,
                &RemoteSnapshot::default(),
                11,
            )
            .unwrap(),
            1
        );
        let remote = RemoteSnapshot {
            open_orders_loaded: true,
            trades_loaded: true,
            balances_loaded: true,
            allowances_loaded: true,
            ..RemoteSnapshot::default()
        };
        assert!(
            recover_after_crash_records(&reopened.records_locked().unwrap(), &remote)
                .unwrap()
                .live_unlock_allowed
        );
        drop(reopened);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("jsonl.checkpoint"));
    }

    #[tokio::test]
    async fn reconnect_snapshot_failure_latches_and_cancels_the_account() {
        let journal_path = std::env::temp_dir().join(format!(
            "polymarket-reconnect-journal-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let baseline_path = std::env::temp_dir().join(format!(
            "polymarket-reconnect-baseline-{}.json",
            uuid::Uuid::new_v4()
        ));
        let now_ms = system_now_ms();
        let account = LiveRiskSnapshot {
            updated_at_ms: now_ms,
            collateral_balance_usdc: "100".parse().unwrap(),
            current_equity_usdc: "100".parse().unwrap(),
            positions: Vec::new(),
            open_order_count: 0,
            risk_state: crate::risk::RiskState::default(),
        };
        let cancel_all_calls = Arc::new(AtomicUsize::new(0));
        let cancel_tracked_calls = Arc::new(AtomicUsize::new(0));
        let revision = Arc::new(AtomicU64::new(1));
        let runtime = LiveRuntime {
            router: ExecutionRouter::new(Box::new(FailingSnapshotAdapter {
                cancel_all_calls: Arc::clone(&cancel_all_calls),
                cancel_tracked_calls,
            }) as Box<dyn LiveAdapter>),
            baseline: open_daily_risk_baseline(&baseline_path, account.current_equity_usdc, now_ms)
                .unwrap(),
            own_orders: OwnOrderCacheState::default(),
            signer_address: "0x0000000000000000000000000000000000000001".to_string(),
            order_clients: BTreeMap::new(),
            safety_latched: false,
            account_revision: revision,
            processed_account_revision: 0,
            last_account_snapshot: account,
            pending_account_reconciliation: None,
            trade_ledger: BTreeMap::new(),
            known_trade_order_ids: BTreeSet::new(),
            owned_order_ids: BTreeSet::new(),
            last_trade_audit_at_ms: now_ms,
        };
        let mut live_runtime = Some(runtime);
        let shared_state = crate::state::shared_runtime_state(RuntimeState::new(
            BotMode::Live,
            now_ms,
            false,
            false,
        ));
        let mut journal = Journal::open(&journal_path, JournalStartup::new("c", "b")).unwrap();
        let settings = Settings::default();
        let compliance = ComplianceState::default();
        let heartbeat = HeartbeatState::default();
        let readiness = ReadinessState::default();

        handle_user_message(
            UserFeedMessage::Connected { revision: 1 },
            &shared_state,
            &mut live_runtime,
            &mut journal,
            LiveSafetyContext {
                settings: &settings,
                compliance: &compliance,
                heartbeat: &heartbeat,
                readiness: &readiness,
            },
        )
        .await
        .unwrap();

        let runtime = live_runtime.as_ref().unwrap();
        assert!(runtime.safety_latched);
        assert!(!runtime.own_orders.certain);
        assert_eq!(cancel_all_calls.load(Ordering::Acquire), 1);
        assert!(!read_state(&shared_state, |state| state.live_submission_enabled).unwrap());
        drop(live_runtime);
        drop(journal);
        let _ = std::fs::remove_file(journal_path);
        let _ = std::fs::remove_file(&baseline_path);
        let _ = std::fs::remove_file(baseline_path.with_extension("json.lock"));
    }

    #[tokio::test]
    async fn ambiguous_submission_latches_preserves_identity_and_cancels_account() {
        let journal_path = std::env::temp_dir().join(format!(
            "polymarket-ambiguous-submit-journal-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let baseline_path = std::env::temp_dir().join(format!(
            "polymarket-ambiguous-submit-baseline-{}.json",
            uuid::Uuid::new_v4()
        ));
        let now_ms = system_now_ms();
        let account = LiveRiskSnapshot {
            updated_at_ms: now_ms,
            collateral_balance_usdc: "100".parse().unwrap(),
            current_equity_usdc: "100".parse().unwrap(),
            positions: Vec::new(),
            open_order_count: 0,
            risk_state: crate::risk::RiskState::default(),
        };
        let cancel_all_calls = Arc::new(AtomicUsize::new(0));
        let cancel_tracked_calls = Arc::new(AtomicUsize::new(0));
        let mut runtime = LiveRuntime {
            router: ExecutionRouter::new(Box::new(FailingSnapshotAdapter {
                cancel_all_calls: Arc::clone(&cancel_all_calls),
                cancel_tracked_calls,
            }) as Box<dyn LiveAdapter>),
            baseline: open_daily_risk_baseline(&baseline_path, account.current_equity_usdc, now_ms)
                .unwrap(),
            own_orders: OwnOrderCacheState::default(),
            signer_address: "0x0000000000000000000000000000000000000001".to_string(),
            order_clients: BTreeMap::new(),
            safety_latched: false,
            account_revision: Arc::new(AtomicU64::new(0)),
            processed_account_revision: 0,
            last_account_snapshot: account,
            pending_account_reconciliation: None,
            trade_ledger: BTreeMap::new(),
            known_trade_order_ids: BTreeSet::new(),
            owned_order_ids: BTreeSet::new(),
            last_trade_audit_at_ms: now_ms,
        };
        let shared_state = crate::state::shared_runtime_state(RuntimeState::new(
            BotMode::Live,
            now_ms,
            false,
            false,
        ));
        update_state(&shared_state, |state| {
            state.live_submission_enabled = true;
            state.paused = false;
            state.strategy_enabled = true;
            state.phase = RuntimePhase::Running;
        })
        .unwrap();
        let mut journal = Journal::open(&journal_path, JournalStartup::new("c", "b")).unwrap();

        handle_ambiguous_live_submission(
            &mut runtime,
            &mut journal,
            &shared_state,
            Some(ExecutionResult {
                client_order_id: "client-7".to_string(),
                exchange_order_id: Some("exchange-7".to_string()),
                status: crate::types::OrderStatus::Acknowledged,
                message: "response could not be committed".to_string(),
            }),
            AssetId::from("1"),
            "journal_commit_uncertain".to_string(),
            now_ms,
        )
        .await
        .unwrap();

        assert!(runtime.safety_latched);
        assert_eq!(cancel_all_calls.load(Ordering::Acquire), 1);
        assert_eq!(
            runtime
                .order_clients
                .get("exchange-7")
                .map(|tracked| tracked.client_order_id.as_str()),
            Some("client-7")
        );
        read_state(&shared_state, |state| {
            assert!(!state.live_submission_enabled);
            assert!(state.paused);
            assert!(!state.strategy_enabled);
            assert_eq!(state.phase, RuntimePhase::Degraded);
            assert_eq!(state.submitted_orders, 1);
        })
        .unwrap();

        drop(runtime);
        drop(journal);
        let _ = std::fs::remove_file(journal_path);
        let _ = std::fs::remove_file(&baseline_path);
        let _ = std::fs::remove_file(baseline_path.with_extension("json.lock"));
    }

    #[tokio::test]
    async fn cancellation_uncertainty_cannot_be_reenabled_by_later_compliance_success() {
        let journal_path = std::env::temp_dir().join(format!(
            "polymarket-cancel-latch-journal-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let baseline_path = std::env::temp_dir().join(format!(
            "polymarket-cancel-latch-baseline-{}.json",
            uuid::Uuid::new_v4()
        ));
        let now_ms = system_now_ms();
        let account = LiveRiskSnapshot {
            updated_at_ms: now_ms,
            collateral_balance_usdc: "100".parse().unwrap(),
            current_equity_usdc: "100".parse().unwrap(),
            positions: Vec::new(),
            open_order_count: 0,
            risk_state: crate::risk::RiskState::default(),
        };
        let cancel_all_calls = Arc::new(AtomicUsize::new(0));
        let mut own_orders = OwnOrderCacheState::default();
        own_orders.apply_authoritative_snapshot(Vec::<String>::new());
        let runtime = LiveRuntime {
            router: ExecutionRouter::new(Box::new(FailingSnapshotAdapter {
                cancel_all_calls: Arc::clone(&cancel_all_calls),
                cancel_tracked_calls: Arc::new(AtomicUsize::new(0)),
            }) as Box<dyn LiveAdapter>),
            baseline: open_daily_risk_baseline(&baseline_path, account.current_equity_usdc, now_ms)
                .unwrap(),
            own_orders,
            signer_address: "0x0000000000000000000000000000000000000001".to_string(),
            order_clients: BTreeMap::new(),
            safety_latched: false,
            account_revision: Arc::new(AtomicU64::new(0)),
            processed_account_revision: 0,
            last_account_snapshot: account,
            pending_account_reconciliation: None,
            trade_ledger: BTreeMap::new(),
            known_trade_order_ids: BTreeSet::new(),
            owned_order_ids: BTreeSet::new(),
            last_trade_audit_at_ms: now_ms,
        };
        let mut live_runtime = Some(runtime);
        let shared_state = crate::state::shared_runtime_state(RuntimeState::new(
            BotMode::Live,
            now_ms,
            false,
            false,
        ));
        update_state(&shared_state, |state| state.live_submission_enabled = true).unwrap();
        let mut journal = Journal::open(&journal_path, JournalStartup::new("c", "b")).unwrap();
        let injected = BotError::Execution("tracked_cancel_timeout".to_string());

        latch_cancel_uncertainty(
            live_runtime.as_mut().unwrap(),
            &mut journal,
            &shared_state,
            now_ms,
            "test_cancel",
            &injected,
        )
        .await
        .unwrap();

        let settings = Settings {
            mode: BotMode::Live,
            ..Settings::default()
        };
        let compliance = ComplianceState::from_geoblock_result(false, "TR", "34", true, now_ms);
        let heartbeat = HeartbeatState {
            live_enabled: true,
            degraded: false,
            ..HeartbeatState::default()
        };
        enforce_live_compliance(
            &settings,
            &compliance,
            &shared_state,
            &mut live_runtime,
            &heartbeat,
            &mut journal,
        )
        .await
        .unwrap();

        let runtime = live_runtime.as_ref().unwrap();
        assert!(runtime.safety_latched);
        assert!(!runtime.own_orders.certain);
        assert_eq!(cancel_all_calls.load(Ordering::Acquire), 1);
        assert!(!read_state(&shared_state, |state| state.live_submission_enabled).unwrap());
        drop(live_runtime);
        drop(journal);
        let _ = std::fs::remove_file(journal_path);
        let _ = std::fs::remove_file(&baseline_path);
        let _ = std::fs::remove_file(baseline_path.with_extension("json.lock"));
    }

    #[tokio::test]
    async fn invalid_book_cancels_affected_live_orders_and_requests_resubscription() {
        let journal_path = std::env::temp_dir().join(format!(
            "polymarket-book-repair-journal-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let baseline_path = std::env::temp_dir().join(format!(
            "polymarket-book-repair-baseline-{}.json",
            uuid::Uuid::new_v4()
        ));
        let now_ms = system_now_ms();
        let account = LiveRiskSnapshot {
            updated_at_ms: now_ms,
            collateral_balance_usdc: "100".parse().unwrap(),
            current_equity_usdc: "100".parse().unwrap(),
            positions: Vec::new(),
            open_order_count: 1,
            risk_state: crate::risk::RiskState::default(),
        };
        let cancel_tracked_calls = Arc::new(AtomicUsize::new(0));
        let runtime = LiveRuntime {
            router: ExecutionRouter::new(Box::new(FailingSnapshotAdapter {
                cancel_all_calls: Arc::new(AtomicUsize::new(0)),
                cancel_tracked_calls: Arc::clone(&cancel_tracked_calls),
            }) as Box<dyn LiveAdapter>),
            baseline: open_daily_risk_baseline(&baseline_path, account.current_equity_usdc, now_ms)
                .unwrap(),
            own_orders: OwnOrderCacheState::default(),
            signer_address: "0x0000000000000000000000000000000000000001".to_string(),
            order_clients: [(
                "exchange-1".to_string(),
                RecoveredOpenOrder {
                    client_order_id: "client-1".to_string(),
                    submitted_at_ms: now_ms,
                    asset_id: AssetId::from("1"),
                },
            )]
            .into_iter()
            .collect(),
            safety_latched: false,
            account_revision: Arc::new(AtomicU64::new(0)),
            processed_account_revision: 0,
            last_account_snapshot: account,
            pending_account_reconciliation: None,
            trade_ledger: BTreeMap::new(),
            known_trade_order_ids: BTreeSet::new(),
            owned_order_ids: ["exchange-1".to_string()].into_iter().collect(),
            last_trade_audit_at_ms: now_ms,
        };
        let mut live_runtime = Some(runtime);
        let mut book = BookState::empty("1", "0.01".parse().unwrap(), "1".parse().unwrap());
        book.bids.push(Level {
            price: "0.4".parse().unwrap(),
            size: "10".parse().unwrap(),
        });
        book.asks.push(Level {
            price: "0.6".parse().unwrap(),
            size: "10".parse().unwrap(),
        });
        book.best_bid = Some("0.4".parse().unwrap());
        book.best_ask = Some("0.6".parse().unwrap());
        book.book_hash = Some("authoritative".to_string());
        book.exchange_timestamp_ms = now_ms.saturating_sub(1);
        book.local_received_at_ms = now_ms.saturating_sub(1);
        book.tradeable = true;
        let mut books = [(AssetId::from("1"), book)].into_iter().collect();
        let event = MarketEvent::PriceChange {
            market: "condition".to_string(),
            timestamp_ms: now_ms,
            price_changes: vec![crate::market_ws::PriceChange {
                asset_id: AssetId::from("1"),
                price: "0.4".parse().unwrap(),
                size: "9".parse().unwrap(),
                side: Side::Buy,
                best_bid: Some("0.4".parse().unwrap()),
                best_ask: Some("0.6".parse().unwrap()),
                hash: None,
            }],
        };
        let (processed_tx, processed_rx) = tokio::sync::oneshot::channel();
        let mut repairing = BTreeSet::new();
        let shared_state = crate::state::shared_runtime_state(RuntimeState::new(
            BotMode::Live,
            now_ms,
            false,
            false,
        ));
        let mut journal = Journal::open(&journal_path, JournalStartup::new("c", "b")).unwrap();
        let settings = Settings {
            mode: BotMode::Live,
            ..Settings::default()
        };
        let mut paper_router = None;
        let mut limiter = SlidingWindowRateLimiter::per_second(10);
        let mut latency = LatencyRecorder::default();

        handle_market_message(
            MarketFeedMessage::Events {
                events: vec![event],
                processed: processed_tx,
            },
            &settings,
            &shared_state,
            &mut repairing,
            &mut books,
            None,
            &mut paper_router,
            &mut live_runtime,
            &mut journal,
            &ReadinessState::default(),
            &ComplianceState::default(),
            &HeartbeatState::default(),
            &RecoveryReport {
                live_unlock_allowed: false,
                reason: "test".to_string(),
                replayed_records: 0,
                in_flight_attempts: 0,
            },
            &MatchingEngineState::normal_after_verified_startup(),
            &ExternalFeatureCache::default(),
            &mut limiter,
            &mut latency,
        )
        .await
        .unwrap();

        assert_eq!(cancel_tracked_calls.load(Ordering::Acquire), 1);
        assert!(live_runtime.as_ref().unwrap().order_clients.is_empty());
        assert!(!books[&AssetId::from("1")].tradeable);
        assert_eq!(processed_rx.await.unwrap(), vec![AssetId::from("1")]);
        assert!(repairing.contains(&AssetId::from("1")));
        drop(live_runtime);
        drop(journal);
        let _ = std::fs::remove_file(journal_path);
        let _ = std::fs::remove_file(&baseline_path);
        let _ = std::fs::remove_file(baseline_path.with_extension("json.lock"));
    }

    #[tokio::test]
    async fn market_resolution_commits_paper_order_cancellation_without_repair() {
        let journal_path = std::env::temp_dir().join(format!(
            "polymarket-resolution-journal-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let now_ms = system_now_ms();
        let mut engine = PaperEngine::new("100".parse().unwrap(), 0, 10_000).unwrap();
        engine
            .submit(
                OrderIntent {
                    asset_id: AssetId::from("1"),
                    side: Side::Buy,
                    limit_price: "0.4".parse().unwrap(),
                    size: "1".parse().unwrap(),
                    time_in_force: TimeInForce::Gtc,
                    post_only: true,
                    local_expires_at_ms: now_ms.saturating_add(10_000),
                    wire_expiration_s: None,
                    reason: "test".to_string(),
                    strategy_id: "test".to_string(),
                    feature_snapshot_id: "book".to_string(),
                },
                "paper-open".to_string(),
                now_ms,
            )
            .unwrap();
        let mut paper_router = Some(ExecutionRouter::new(PaperExecution::new(engine)));
        let mut book = BookState::empty("1", "0.01".parse().unwrap(), "1".parse().unwrap());
        book.tradeable = true;
        book.book_hash = Some("authoritative".to_string());
        let mut books = [(AssetId::from("1"), book)].into_iter().collect();
        let (processed_tx, processed_rx) = tokio::sync::oneshot::channel();
        let shared_state = crate::state::shared_runtime_state(RuntimeState::new(
            BotMode::Paper,
            now_ms,
            false,
            false,
        ));
        let mut journal = Journal::open(&journal_path, JournalStartup::new("c", "b")).unwrap();
        let mut repairing = BTreeSet::new();
        let mut live_runtime = None;
        let mut limiter = SlidingWindowRateLimiter::per_second(10);
        let mut latency = LatencyRecorder::default();

        handle_market_message(
            MarketFeedMessage::Events {
                events: vec![MarketEvent::MarketResolved {
                    condition_id: crate::types::ConditionId::from("condition"),
                    asset_ids: vec![AssetId::from("1")],
                    winning_asset_id: AssetId::from("1"),
                }],
                processed: processed_tx,
            },
            &Settings::default(),
            &shared_state,
            &mut repairing,
            &mut books,
            None,
            &mut paper_router,
            &mut live_runtime,
            &mut journal,
            &ReadinessState::default(),
            &ComplianceState::default(),
            &HeartbeatState::default(),
            &RecoveryReport {
                live_unlock_allowed: false,
                reason: "test".to_string(),
                replayed_records: 0,
                in_flight_attempts: 0,
            },
            &MatchingEngineState::normal_after_verified_startup(),
            &ExternalFeatureCache::default(),
            &mut limiter,
            &mut latency,
        )
        .await
        .unwrap();

        assert!(processed_rx.await.unwrap().is_empty());
        assert!(repairing.is_empty());
        assert!(!books[&AssetId::from("1")].tradeable);
        assert_eq!(
            paper_router
                .as_ref()
                .unwrap()
                .adapter()
                .engine()
                .orders()
                .count(),
            0
        );
        let records = journal.records_locked().unwrap();
        let record = records
            .iter()
            .rev()
            .find(|record| record.event_kind == JournalEventKind::PaperTransition)
            .unwrap();
        let transition: PaperTransition = serde_json::from_str(&record.payload).unwrap();
        assert_eq!(transition.reason, "market_resolved");
        assert_eq!(
            transition.cancelled_order_ids,
            vec!["paper-open".to_string()]
        );
        drop(journal);
        let _ = std::fs::remove_file(journal_path);
    }

    #[tokio::test]
    async fn authoritative_book_in_same_wire_batch_clears_repair_before_acknowledgement() {
        let journal_path = std::env::temp_dir().join(format!(
            "polymarket-batch-repair-journal-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let now_ms = system_now_ms();
        let mut books = [(
            AssetId::from("1"),
            BookState::empty("1", "0.01".parse().unwrap(), "1".parse().unwrap()),
        )]
        .into_iter()
        .collect();
        let invalid_delta = MarketEvent::PriceChange {
            market: "condition".to_string(),
            timestamp_ms: now_ms,
            price_changes: vec![crate::market_ws::PriceChange {
                asset_id: AssetId::from("1"),
                price: "0.4".parse().unwrap(),
                size: "9".parse().unwrap(),
                side: Side::Buy,
                best_bid: Some("0.4".parse().unwrap()),
                best_ask: Some("0.6".parse().unwrap()),
                hash: Some("delta".to_string()),
            }],
        };
        let authoritative_book = MarketEvent::Book {
            asset_id: AssetId::from("1"),
            market: "condition".to_string(),
            timestamp_ms: now_ms,
            hash: Some("authoritative".to_string()),
            bids: vec![Level {
                price: "0.4".parse().unwrap(),
                size: "10".parse().unwrap(),
            }],
            asks: vec![Level {
                price: "0.6".parse().unwrap(),
                size: "10".parse().unwrap(),
            }],
        };
        let (processed_tx, processed_rx) = tokio::sync::oneshot::channel();
        let shared_state = crate::state::shared_runtime_state(RuntimeState::new(
            BotMode::Paper,
            now_ms,
            false,
            false,
        ));
        let mut journal = Journal::open(&journal_path, JournalStartup::new("c", "b")).unwrap();
        let mut repairing = BTreeSet::new();
        let mut paper_router = None;
        let mut live_runtime = None;
        let mut limiter = SlidingWindowRateLimiter::per_second(10);
        let mut latency = LatencyRecorder::default();

        handle_market_message(
            MarketFeedMessage::Events {
                events: vec![invalid_delta, authoritative_book],
                processed: processed_tx,
            },
            &Settings::default(),
            &shared_state,
            &mut repairing,
            &mut books,
            None,
            &mut paper_router,
            &mut live_runtime,
            &mut journal,
            &ReadinessState::default(),
            &ComplianceState::default(),
            &HeartbeatState::default(),
            &RecoveryReport {
                live_unlock_allowed: false,
                reason: "test".to_string(),
                replayed_records: 0,
                in_flight_attempts: 0,
            },
            &MatchingEngineState::normal_after_verified_startup(),
            &ExternalFeatureCache::default(),
            &mut limiter,
            &mut latency,
        )
        .await
        .unwrap();

        assert!(processed_rx.await.unwrap().is_empty());
        assert!(repairing.is_empty());
        assert!(books[&AssetId::from("1")].tradeable);
        assert_eq!(
            books[&AssetId::from("1")].book_hash.as_deref(),
            Some("authoritative")
        );
        drop(journal);
        let _ = std::fs::remove_file(journal_path);
    }

    #[test]
    fn fill_reconciliation_requires_the_specific_asset_and_cash_delta() {
        let baseline = LiveRiskSnapshot {
            updated_at_ms: 1,
            collateral_balance_usdc: "10".parse().unwrap(),
            current_equity_usdc: "11".parse().unwrap(),
            positions: vec![crate::execution::LivePositionSnapshot {
                asset_id: AssetId::from("1"),
                size: "2".parse().unwrap(),
                average_price: "0.4".parse().unwrap(),
                current_value_usdc: "1".parse().unwrap(),
                cash_pnl_usdc: "0.2".parse().unwrap(),
            }],
            open_order_count: 0,
            risk_state: crate::risk::RiskState::default(),
        };
        let mut pending = PendingAccountReconciliation {
            baseline: baseline.clone(),
            trades: [(
                "trade-1".to_string(),
                PendingTradeMutation {
                    asset_id: AssetId::from("1"),
                    side: Side::Buy,
                    price: "0.4".parse().unwrap(),
                    size: "0.5".parse().unwrap(),
                    order_ids: BTreeSet::new(),
                },
            )]
            .into_iter()
            .collect(),
            authoritative_order_ids: BTreeSet::new(),
            unresolved_order_ids: BTreeSet::new(),
        };
        let mut current = baseline.clone();
        current.collateral_balance_usdc = "9.8".parse().unwrap();
        current
            .positions
            .push(crate::execution::LivePositionSnapshot {
                asset_id: AssetId::from("unrelated"),
                size: "1".parse().unwrap(),
                average_price: "0.1".parse().unwrap(),
                current_value_usdc: "0.1".parse().unwrap(),
                cash_pnl_usdc: crate::fixed::Fixed::ZERO,
            });
        assert!(!pending_account_reconciled(
            &pending,
            &current,
            &AuthoritativePendingTradeProof::default()
        )
        .unwrap());

        current.positions[0].size = "2.5".parse().unwrap();
        pending.unresolved_order_ids.insert("order-1".to_string());
        assert!(!pending_account_reconciled(
            &pending,
            &current,
            &AuthoritativePendingTradeProof::default()
        )
        .unwrap());
        pending.unresolved_order_ids.clear();
        let fee_free_proof = AuthoritativePendingTradeProof {
            trade_ids: ["trade-1".to_string()].into_iter().collect(),
            cash_fee_lower_bound_usdc: Fixed::ZERO,
            cash_fee_upper_bound_usdc: Fixed::ZERO,
        };
        assert!(!pending_account_reconciled(&pending, &current, &fee_free_proof).unwrap());

        current
            .positions
            .retain(|position| position.asset_id != AssetId::from("unrelated"));
        assert!(!pending_account_reconciled(
            &pending,
            &current,
            &AuthoritativePendingTradeProof::default()
        )
        .unwrap());
        assert!(pending_account_reconciled(&pending, &current, &fee_free_proof).unwrap());

        current.collateral_balance_usdc = "9.799999".parse().unwrap();
        assert!(!pending_account_reconciled(&pending, &current, &fee_free_proof).unwrap());
    }

    #[test]
    fn zero_net_fee_free_trades_require_exact_authoritative_trade_proof() {
        let baseline = LiveRiskSnapshot {
            updated_at_ms: 1,
            collateral_balance_usdc: "10".parse().unwrap(),
            current_equity_usdc: "10.4".parse().unwrap(),
            positions: vec![crate::execution::LivePositionSnapshot {
                asset_id: AssetId::from("1"),
                size: "1".parse().unwrap(),
                average_price: "0.4".parse().unwrap(),
                current_value_usdc: "0.4".parse().unwrap(),
                cash_pnl_usdc: Fixed::ZERO,
            }],
            open_order_count: 0,
            risk_state: crate::risk::RiskState::default(),
        };
        let trade = |side, order_id: &str| PendingTradeMutation {
            asset_id: AssetId::from("1"),
            side,
            price: "0.4".parse().unwrap(),
            size: "1".parse().unwrap(),
            order_ids: [order_id.to_string()].into_iter().collect(),
        };
        let pending = PendingAccountReconciliation {
            baseline: baseline.clone(),
            trades: [
                ("buy".to_string(), trade(Side::Buy, "buy-order")),
                ("sell".to_string(), trade(Side::Sell, "sell-order")),
            ]
            .into_iter()
            .collect(),
            authoritative_order_ids: ["buy-order".to_string(), "sell-order".to_string()]
                .into_iter()
                .collect(),
            unresolved_order_ids: BTreeSet::new(),
        };

        assert!(!pending_account_reconciled(
            &pending,
            &baseline,
            &AuthoritativePendingTradeProof::default()
        )
        .unwrap());
        assert!(!pending_account_reconciled(
            &pending,
            &baseline,
            &AuthoritativePendingTradeProof {
                trade_ids: ["buy".to_string()].into_iter().collect(),
                cash_fee_lower_bound_usdc: Fixed::ZERO,
                cash_fee_upper_bound_usdc: Fixed::ZERO,
            }
        )
        .unwrap());
        assert!(pending_account_reconciled(
            &pending,
            &baseline,
            &AuthoritativePendingTradeProof {
                trade_ids: ["buy".to_string(), "sell".to_string()]
                    .into_iter()
                    .collect(),
                cash_fee_lower_bound_usdc: Fixed::ZERO,
                cash_fee_upper_bound_usdc: Fixed::ZERO,
            }
        )
        .unwrap());

        let fee_bearing_proof = AuthoritativePendingTradeProof {
            trade_ids: ["buy".to_string(), "sell".to_string()]
                .into_iter()
                .collect(),
            cash_fee_lower_bound_usdc: "0.01".parse().unwrap(),
            cash_fee_upper_bound_usdc: "0.01".parse().unwrap(),
        };
        assert!(!pending_account_reconciled(&pending, &baseline, &fee_bearing_proof).unwrap());
        let mut after_fees = baseline.clone();
        after_fees.collateral_balance_usdc = "9.99".parse().unwrap();
        assert!(pending_account_reconciled(&pending, &after_fees, &fee_bearing_proof).unwrap());
        after_fees.collateral_balance_usdc = "9.989999".parse().unwrap();
        assert!(!pending_account_reconciled(&pending, &after_fees, &fee_bearing_proof).unwrap());
    }

    #[test]
    fn authoritative_trade_fee_terms_produce_a_tight_cash_envelope() {
        let mutation = PendingTradeMutation {
            asset_id: AssetId::from("1"),
            side: Side::Buy,
            price: "0.4".parse().unwrap(),
            size: "100".parse().unwrap(),
            order_ids: ["order-1".to_string()].into_iter().collect(),
        };
        let pending = PendingAccountReconciliation {
            baseline: LiveRiskSnapshot {
                updated_at_ms: 1,
                collateral_balance_usdc: "100".parse().unwrap(),
                current_equity_usdc: "100".parse().unwrap(),
                positions: Vec::new(),
                open_order_count: 0,
                risk_state: crate::risk::RiskState::default(),
            },
            trades: [("trade-1".to_string(), mutation.clone())]
                .into_iter()
                .collect(),
            authoritative_order_ids: ["order-1".to_string()].into_iter().collect(),
            unresolved_order_ids: BTreeSet::new(),
        };
        let trade = AuthoritativeTrade {
            trade_id: "trade-1".to_string(),
            condition_id: crate::types::ConditionId::from("condition"),
            asset_id: mutation.asset_id,
            side: mutation.side,
            price: mutation.price,
            size: mutation.size,
            fee_rate_bps: "700".parse().unwrap(),
            status: "Matched".to_string(),
            order_ids: mutation.order_ids,
            timestamp_ms: Some(2),
        };
        let remote = RemoteSnapshot {
            trades_loaded: true,
            trades: [("trade-1".to_string(), trade)].into_iter().collect(),
            ..RemoteSnapshot::default()
        };

        let proof = authoritative_pending_trade_proof(&pending, &remote).unwrap();

        assert_eq!(proof.cash_fee_lower_bound_usdc.to_string(), "1.68");
        assert_eq!(proof.cash_fee_upper_bound_usdc.to_string(), "1.68");
    }

    #[test]
    fn authoritative_rest_trade_recovers_a_lost_user_trade_across_restart() {
        let path = std::env::temp_dir().join(format!(
            "polymarket-lost-trade-recovery-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        let baseline = LiveRiskSnapshot {
            updated_at_ms: 1,
            collateral_balance_usdc: "10".parse().unwrap(),
            current_equity_usdc: "10".parse().unwrap(),
            positions: Vec::new(),
            open_order_count: 0,
            risk_state: crate::risk::RiskState::default(),
        };
        append_account_mutation(
            &mut journal,
            &UserEvent::Order {
                condition_id: crate::types::ConditionId::from("condition"),
                order_id: "order-1".to_string(),
                asset_id: AssetId::from("1"),
                side: Side::Buy,
                price: "0.4".parse().unwrap(),
                original_size: Some("1".parse().unwrap()),
                size_matched: Some("1".parse().unwrap()),
                status: "Matched".to_string(),
                timestamp_ms: Some(2),
            },
            &baseline,
        )
        .unwrap();
        let before = recover_account_ledger(&journal.records_locked().unwrap()).unwrap();
        let pending = before.pending.as_ref().unwrap();
        assert_eq!(pending.unresolved_order_ids.len(), 1);
        assert!(pending.trades.is_empty());

        let trade = AuthoritativeTrade {
            trade_id: "trade-1".to_string(),
            condition_id: crate::types::ConditionId::from("condition"),
            asset_id: AssetId::from("1"),
            side: Side::Buy,
            price: "0.4".parse().unwrap(),
            size: "1".parse().unwrap(),
            fee_rate_bps: Fixed::ZERO,
            status: "Matched".to_string(),
            order_ids: ["order-1".to_string()].into_iter().collect(),
            timestamp_ms: Some(2),
        };
        let remote = RemoteSnapshot {
            trades_loaded: true,
            trade_order_ids: ["order-1".to_string()].into_iter().collect(),
            trades: [("trade-1".to_string(), trade)].into_iter().collect(),
            ..RemoteSnapshot::default()
        };
        assert_eq!(
            append_startup_authoritative_trades(
                &mut journal,
                &pending.baseline,
                &pending.unresolved_order_ids,
                &before.trade_ledger,
                &remote,
            )
            .unwrap(),
            vec!["trade-1".to_string()]
        );

        let recovered = recover_account_ledger(&journal.records_locked().unwrap()).unwrap();
        let pending = recovered.pending.as_ref().unwrap();
        assert!(pending.unresolved_order_ids.is_empty());
        assert!(pending.trades.contains_key("trade-1"));
        let mut current = baseline;
        current.collateral_balance_usdc = "9.6".parse().unwrap();
        current
            .positions
            .push(crate::execution::LivePositionSnapshot {
                asset_id: AssetId::from("1"),
                size: "1".parse().unwrap(),
                average_price: "0.4".parse().unwrap(),
                current_value_usdc: "0.4".parse().unwrap(),
                cash_pnl_usdc: Fixed::ZERO,
            });
        let authoritative = authoritative_pending_trade_proof(pending, &remote).unwrap();
        assert!(pending_account_reconciled(pending, &current, &authoritative).unwrap());
        drop(journal);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn authoritative_trade_audit_recovers_when_all_user_events_are_missed() {
        let journal_path = std::env::temp_dir().join(format!(
            "polymarket-completely-missed-trade-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let baseline_path = std::env::temp_dir().join(format!(
            "polymarket-completely-missed-trade-baseline-{}.json",
            uuid::Uuid::new_v4()
        ));
        let baseline = LiveRiskSnapshot {
            updated_at_ms: 1,
            collateral_balance_usdc: "10".parse().unwrap(),
            current_equity_usdc: "10".parse().unwrap(),
            positions: Vec::new(),
            open_order_count: 1,
            risk_state: crate::risk::RiskState::default(),
        };
        let mut runtime = LiveRuntime {
            router: ExecutionRouter::new(Box::new(FailingSnapshotAdapter {
                cancel_all_calls: Arc::new(AtomicUsize::new(0)),
                cancel_tracked_calls: Arc::new(AtomicUsize::new(0)),
            }) as Box<dyn LiveAdapter>),
            baseline: open_daily_risk_baseline(&baseline_path, baseline.current_equity_usdc, 1)
                .unwrap(),
            own_orders: OwnOrderCacheState::default(),
            signer_address: "0x0000000000000000000000000000000000000001".to_string(),
            order_clients: BTreeMap::new(),
            safety_latched: false,
            account_revision: Arc::new(AtomicU64::new(0)),
            processed_account_revision: 0,
            last_account_snapshot: baseline.clone(),
            pending_account_reconciliation: None,
            trade_ledger: BTreeMap::new(),
            known_trade_order_ids: BTreeSet::new(),
            owned_order_ids: ["order-1".to_string()].into_iter().collect(),
            last_trade_audit_at_ms: 0,
        };
        let trade = AuthoritativeTrade {
            trade_id: "trade-1".to_string(),
            condition_id: crate::types::ConditionId::from("condition"),
            asset_id: AssetId::from("1"),
            side: Side::Buy,
            price: "0.4".parse().unwrap(),
            size: "1".parse().unwrap(),
            fee_rate_bps: Fixed::ZERO,
            status: "Matched".to_string(),
            order_ids: ["order-1".to_string()].into_iter().collect(),
            timestamp_ms: Some(2),
        };
        let remote = RemoteSnapshot {
            trades_loaded: true,
            trade_order_ids: ["order-1".to_string()].into_iter().collect(),
            trades: [("trade-1".to_string(), trade)].into_iter().collect(),
            ..RemoteSnapshot::default()
        };
        let mut journal = Journal::open(&journal_path, JournalStartup::new("c", "b")).unwrap();

        assert_eq!(
            ingest_authoritative_remote_trades(&mut runtime, &mut journal, &remote).unwrap(),
            1
        );
        let pending = runtime.pending_account_reconciliation.as_ref().unwrap();
        assert!(pending.unresolved_order_ids.is_empty());
        assert!(pending.trades.contains_key("trade-1"));

        let mut current = baseline;
        current.collateral_balance_usdc = "9.6".parse().unwrap();
        current
            .positions
            .push(crate::execution::LivePositionSnapshot {
                asset_id: AssetId::from("1"),
                size: "1".parse().unwrap(),
                average_price: "0.4".parse().unwrap(),
                current_value_usdc: "0.4".parse().unwrap(),
                cash_pnl_usdc: Fixed::ZERO,
            });
        let proof = authoritative_pending_trade_proof(pending, &remote).unwrap();
        assert!(pending_account_reconciled(pending, &current, &proof).unwrap());

        commit_account_reconciliation(&mut runtime, &mut journal, 3).unwrap();
        let recovered = recover_account_ledger(&journal.records_locked().unwrap()).unwrap();
        assert!(recovered.pending.is_none());
        assert!(recovered.trade_ledger["trade-1"].reconciled);

        drop(runtime);
        drop(journal);
        let _ = std::fs::remove_file(journal_path);
        let _ = std::fs::remove_file(&baseline_path);
        let _ = std::fs::remove_file(baseline_path.with_extension("json.lock"));
    }

    #[test]
    fn startup_audit_reconstructs_a_completely_missed_trade_and_keeps_live_locked() {
        let path = std::env::temp_dir().join(format!(
            "polymarket-startup-completely-missed-trade-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        journal
            .append_write_ahead("decision", "client-1", "live", "accepted", "intent")
            .unwrap();
        journal
            .append_lifecycle(
                JournalEventKind::Acknowledged,
                "decision",
                "client-1",
                Some("order-1".to_string()),
                "live",
                "acknowledged",
                "venue ack",
            )
            .unwrap();
        let records = journal.records_locked().unwrap();
        let owned_order_ids = journal_owned_order_ids(&records);
        let before = recover_account_ledger(&records).unwrap();
        assert!(before.pending.is_none());

        let current = LiveRiskSnapshot {
            updated_at_ms: 3,
            collateral_balance_usdc: "9.6".parse().unwrap(),
            current_equity_usdc: "10".parse().unwrap(),
            positions: vec![crate::execution::LivePositionSnapshot {
                asset_id: AssetId::from("1"),
                size: "1".parse().unwrap(),
                average_price: "0.4".parse().unwrap(),
                current_value_usdc: "0.4".parse().unwrap(),
                cash_pnl_usdc: Fixed::ZERO,
            }],
            open_order_count: 0,
            risk_state: crate::risk::RiskState::default(),
        };
        let trade = AuthoritativeTrade {
            trade_id: "trade-1".to_string(),
            condition_id: crate::types::ConditionId::from("condition"),
            asset_id: AssetId::from("1"),
            side: Side::Buy,
            price: "0.4".parse().unwrap(),
            size: "1".parse().unwrap(),
            fee_rate_bps: Fixed::ZERO,
            status: "Matched".to_string(),
            order_ids: ["order-1".to_string()].into_iter().collect(),
            timestamp_ms: Some(2),
        };
        let remote = RemoteSnapshot {
            trades_loaded: true,
            trade_order_ids: ["order-1".to_string()].into_iter().collect(),
            trades: [("trade-1".to_string(), trade)].into_iter().collect(),
            ..RemoteSnapshot::default()
        };
        assert_eq!(
            append_startup_authoritative_trades(
                &mut journal,
                &current,
                &owned_order_ids,
                &before.trade_ledger,
                &remote,
            )
            .unwrap(),
            vec!["trade-1".to_string()]
        );

        let recovered = recover_account_ledger(&journal.records_locked().unwrap()).unwrap();
        let pending = recovered.pending.as_ref().unwrap();
        let proof = authoritative_pending_trade_proof(pending, &remote).unwrap();
        assert!(!pending_account_reconciled(pending, &current, &proof).unwrap());

        drop(journal);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn periodic_trade_audit_recovers_without_preexisting_pending_state() {
        let journal_path = std::env::temp_dir().join(format!(
            "polymarket-periodic-missed-trade-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let baseline_path = std::env::temp_dir().join(format!(
            "polymarket-periodic-missed-trade-baseline-{}.json",
            uuid::Uuid::new_v4()
        ));
        let now_ms = system_now_ms();
        let baseline = LiveRiskSnapshot {
            updated_at_ms: now_ms.saturating_sub(10_000),
            collateral_balance_usdc: "10".parse().unwrap(),
            current_equity_usdc: "10".parse().unwrap(),
            positions: Vec::new(),
            open_order_count: 1,
            risk_state: crate::risk::RiskState::default(),
        };
        let mut current = baseline.clone();
        current.updated_at_ms = now_ms;
        current.collateral_balance_usdc = "9.6".parse().unwrap();
        current.open_order_count = 0;
        current
            .positions
            .push(crate::execution::LivePositionSnapshot {
                asset_id: AssetId::from("1"),
                size: "1".parse().unwrap(),
                average_price: "0.4".parse().unwrap(),
                current_value_usdc: "0.4".parse().unwrap(),
                cash_pnl_usdc: Fixed::ZERO,
            });
        let trade = AuthoritativeTrade {
            trade_id: "trade-1".to_string(),
            condition_id: crate::types::ConditionId::from("condition"),
            asset_id: AssetId::from("1"),
            side: Side::Buy,
            price: "0.4".parse().unwrap(),
            size: "1".parse().unwrap(),
            fee_rate_bps: Fixed::ZERO,
            status: "Matched".to_string(),
            order_ids: ["order-1".to_string()].into_iter().collect(),
            timestamp_ms: Some(now_ms.saturating_sub(1)),
        };
        let remote = RemoteSnapshot {
            trades_loaded: true,
            trade_order_ids: ["order-1".to_string()].into_iter().collect(),
            trades: [("trade-1".to_string(), trade)].into_iter().collect(),
            ..RemoteSnapshot::default()
        };
        let runtime = LiveRuntime {
            router: ExecutionRouter::new(Box::new(AuditingSnapshotAdapter {
                snapshot: current.clone(),
                remote,
            }) as Box<dyn LiveAdapter>),
            baseline: open_daily_risk_baseline(
                &baseline_path,
                baseline.current_equity_usdc,
                now_ms,
            )
            .unwrap(),
            own_orders: OwnOrderCacheState::default(),
            signer_address: "0x0000000000000000000000000000000000000001".to_string(),
            order_clients: BTreeMap::new(),
            safety_latched: false,
            account_revision: Arc::new(AtomicU64::new(0)),
            processed_account_revision: 0,
            last_account_snapshot: baseline,
            pending_account_reconciliation: None,
            trade_ledger: BTreeMap::new(),
            known_trade_order_ids: BTreeSet::new(),
            owned_order_ids: ["order-1".to_string()].into_iter().collect(),
            last_trade_audit_at_ms: 0,
        };
        let mut live_runtime = Some(runtime);
        let shared_state = crate::state::shared_runtime_state(RuntimeState::new(
            BotMode::Live,
            now_ms,
            false,
            false,
        ));
        let mut journal = Journal::open(&journal_path, JournalStartup::new("c", "b")).unwrap();
        let settings = Settings {
            asset_ids: vec![AssetId::from("1")],
            ..Settings::default()
        };

        maintain_live_account_reconciliation(
            &settings,
            &shared_state,
            &mut live_runtime,
            &mut journal,
            &ReadinessState::default(),
            &ComplianceState::default(),
            &HeartbeatState::default(),
        )
        .await
        .unwrap();

        let runtime = live_runtime.as_ref().unwrap();
        assert!(runtime.pending_account_reconciliation.is_none());
        assert!(runtime.trade_ledger["trade-1"].reconciled);
        assert_eq!(runtime.last_account_snapshot, current);
        let recovered = recover_account_ledger(&journal.records_locked().unwrap()).unwrap();
        assert!(recovered.pending.is_none());
        assert!(recovered.trade_ledger["trade-1"].reconciled);

        drop(live_runtime);
        drop(journal);
        let _ = std::fs::remove_file(journal_path);
        let _ = std::fs::remove_file(&baseline_path);
        let _ = std::fs::remove_file(baseline_path.with_extension("json.lock"));
    }

    #[test]
    fn durable_trade_ledger_deduplicates_later_status_revisions() {
        let path = std::env::temp_dir().join(format!(
            "polymarket-trade-ledger-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        let baseline = LiveRiskSnapshot {
            updated_at_ms: 1,
            collateral_balance_usdc: "10".parse().unwrap(),
            current_equity_usdc: "10".parse().unwrap(),
            positions: Vec::new(),
            open_order_count: 0,
            risk_state: crate::risk::RiskState::default(),
        };
        let event = |status: &str| UserEvent::Trade {
            condition_id: crate::types::ConditionId::from("c"),
            trade_id: "trade-1".to_string(),
            asset_id: AssetId::from("1"),
            side: Side::Buy,
            price: "0.4".parse().unwrap(),
            size: "1".parse().unwrap(),
            status: status.to_string(),
            order_ids: vec!["order-1".to_string()],
            timestamp_ms: Some(1),
        };
        for status in ["Matched", "Confirmed"] {
            let payload = AccountMutationJournal {
                schema_version: ACCOUNT_MUTATION_SCHEMA_VERSION,
                event: event(status),
                baseline: baseline.clone(),
            };
            journal
                .append_lifecycle(
                    JournalEventKind::AccountMutation,
                    format!("account-{status}"),
                    format!("account-{status}"),
                    None,
                    "live_account",
                    status,
                    serde_json::to_string(&payload).unwrap(),
                )
                .unwrap();
            if status == "Matched" {
                let reconciliation = AccountReconciliationJournal {
                    schema_version: ACCOUNT_MUTATION_SCHEMA_VERSION,
                    reconciled_at_ms: 2,
                    trade_ids: vec!["trade-1".to_string()],
                };
                journal
                    .append_lifecycle(
                        JournalEventKind::Reconciled,
                        "account-reconciled",
                        "account-reconciled",
                        None,
                        "live_account",
                        "reconciled",
                        serde_json::to_string(&reconciliation).unwrap(),
                    )
                    .unwrap();
            }
        }

        let recovered = recover_account_ledger(&journal.records_locked().unwrap()).unwrap();

        let entry = recovered.trade_ledger.get("trade-1").unwrap();
        assert!(entry.reconciled);
        assert_eq!(entry.latest_status, "Confirmed");
        assert!(recovered.pending.is_none());
        assert!(!recovered.safety_latched);

        let failed = AccountMutationJournal {
            schema_version: ACCOUNT_MUTATION_SCHEMA_VERSION,
            event: event("Failed"),
            baseline,
        };
        journal
            .append_lifecycle(
                JournalEventKind::AccountMutation,
                "account-failed",
                "account-failed",
                None,
                "live_account",
                "Failed",
                serde_json::to_string(&failed).unwrap(),
            )
            .unwrap();
        let failed_recovery = recover_account_ledger(&journal.records_locked().unwrap()).unwrap();
        assert!(failed_recovery.safety_latched);
        drop(journal);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn paper_transition_is_atomic_and_replays_after_restart() {
        let path = std::env::temp_dir().join(format!(
            "polymarket-paper-transition-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let settings = Settings {
            paper_starting_cash_usdc: "100".parse().unwrap(),
            paper_fee_bps: 0,
            paper_fill_participation_bps: 10_000,
            asset_ids: vec![AssetId::from("1")],
            ..Settings::default()
        };
        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        let mut current = PaperEngine::new("100".parse().unwrap(), 0, 10_000).unwrap();
        let mut staged = current.clone();
        staged
            .submit(
                OrderIntent {
                    asset_id: AssetId::from("1"),
                    side: Side::Buy,
                    limit_price: "0.4".parse().unwrap(),
                    size: "1".parse().unwrap(),
                    time_in_force: TimeInForce::Gtc,
                    post_only: true,
                    local_expires_at_ms: 2_000,
                    wire_expiration_s: None,
                    reason: "test".to_string(),
                    strategy_id: "test".to_string(),
                    feature_snapshot_id: "book".to_string(),
                },
                "client-0",
                1_000,
            )
            .unwrap();
        commit_paper_transition(
            &mut journal,
            &mut current,
            staged.clone(),
            &[],
            &[],
            "test_submit",
            1_000,
        )
        .unwrap();
        assert_eq!(current, staged);
        drop(journal);

        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        let recovered = recover_paper_engine(&settings, &mut journal).unwrap();
        assert_eq!(recovered, staged);

        let before_failure = recovered.clone();
        let mut cancelled = recovered.clone();
        cancelled.cancel_all(1_100, "test_cancel");
        journal.fail_append_after_for_test(0);
        let mut target = recovered;
        assert!(commit_paper_transition(
            &mut journal,
            &mut target,
            cancelled,
            &[],
            &["client-0".to_string()],
            "test_cancel",
            1_100,
        )
        .is_err());
        assert_eq!(target, before_failure);
        drop(journal);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn paper_state_prunes_terminal_orders_and_recovers_across_atomic_compaction() {
        let path = std::env::temp_dir().join(format!(
            "polymarket-paper-compaction-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let settings = Settings {
            paper_starting_cash_usdc: "100".parse().unwrap(),
            paper_fee_bps: 0,
            paper_fill_participation_bps: 10_000,
            asset_ids: vec![AssetId::from("1")],
            ..Settings::default()
        };
        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        let mut current = PaperEngine::new("100".parse().unwrap(), 0, 10_000).unwrap();
        let mut staged = current.clone();
        staged
            .submit(
                OrderIntent {
                    asset_id: AssetId::from("1"),
                    side: Side::Buy,
                    limit_price: "0.4".parse().unwrap(),
                    size: "1".parse().unwrap(),
                    time_in_force: TimeInForce::Gtc,
                    post_only: true,
                    local_expires_at_ms: 2_000,
                    wire_expiration_s: None,
                    reason: "test".to_string(),
                    strategy_id: "test".to_string(),
                    feature_snapshot_id: "book".to_string(),
                },
                "paper-client-1",
                1_000,
            )
            .unwrap();
        commit_paper_transition(
            &mut journal,
            &mut current,
            staged,
            &[],
            &[],
            "submit",
            1_000,
        )
        .unwrap();
        let mut staged = current.clone();
        staged
            .cancel("paper-client-1", 1_100, "test_cancel")
            .unwrap();
        commit_paper_transition(
            &mut journal,
            &mut current,
            staged,
            &[],
            &["paper-client-1".to_string()],
            "cancel",
            1_100,
        )
        .unwrap();
        assert_eq!(current.orders().count(), 0);

        let latest = journal
            .records_locked()
            .unwrap()
            .into_iter()
            .last()
            .unwrap();
        journal
            .compact_paper_history(
                latest.decision_id.clone(),
                latest.client_order_id,
                latest.payload,
            )
            .unwrap();
        let compacted = journal.records_locked().unwrap();
        assert_eq!(compacted.len(), 1);
        assert!(compacted[0]
            .source_event_refs
            .iter()
            .any(|reference| reference.starts_with("compacted_root:")));
        assert_eq!(journal.next_sequence(), 1);
        drop(journal);

        let mut reopened = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        assert_eq!(
            recover_paper_engine(&settings, &mut reopened).unwrap(),
            current
        );
        drop(reopened);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("jsonl.checkpoint"));
    }

    #[test]
    fn synced_paper_transition_commits_even_when_checkpoint_write_fails() {
        let path = std::env::temp_dir().join(format!(
            "polymarket-paper-checkpoint-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let settings = Settings {
            paper_starting_cash_usdc: "100".parse().unwrap(),
            paper_fee_bps: 0,
            paper_fill_participation_bps: 10_000,
            asset_ids: vec![AssetId::from("1")],
            ..Settings::default()
        };
        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        let mut current = PaperEngine::new("100".parse().unwrap(), 0, 10_000).unwrap();
        let mut staged = current.clone();
        staged
            .submit(
                OrderIntent {
                    asset_id: AssetId::from("1"),
                    side: Side::Buy,
                    limit_price: "0.4".parse().unwrap(),
                    size: "1".parse().unwrap(),
                    time_in_force: TimeInForce::Gtc,
                    post_only: true,
                    local_expires_at_ms: 2_000,
                    wire_expiration_s: None,
                    reason: "test".to_string(),
                    strategy_id: "test".to_string(),
                    feature_snapshot_id: "book".to_string(),
                },
                "client-0",
                1_000,
            )
            .unwrap();
        journal.fail_stage_for_test(crate::journal::JournalFailStage::AfterSyncBeforeCheckpoint);

        commit_paper_transition(
            &mut journal,
            &mut current,
            staged.clone(),
            &[],
            &[],
            "test_submit",
            1_000,
        )
        .unwrap();
        assert_eq!(current, staged);
        assert!(journal.checkpoint_warning().is_some());
        assert!(!journal.is_poisoned());
        drop(journal);

        let mut reopened = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        assert_eq!(
            recover_paper_engine(&settings, &mut reopened).unwrap(),
            staged
        );
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn paper_transition_write_failure_rolls_back_before_commit() {
        let path = std::env::temp_dir().join(format!(
            "polymarket-paper-rollback-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        let mut current = PaperEngine::new("100".parse().unwrap(), 0, 10_000).unwrap();
        let before = current.clone();
        let mut staged = current.clone();
        staged.cancel_all(1_000, "noop");
        journal.fail_stage_for_test(crate::journal::JournalFailStage::AfterWriteBeforeSync);

        assert!(commit_paper_transition(
            &mut journal,
            &mut current,
            staged,
            &[],
            &[],
            "test_failure",
            1_000,
        )
        .is_err());
        assert_eq!(current, before);
        drop(journal);
        assert!(crate::journal::replay_records(&path).unwrap().is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn equity_high_water_survives_restart_and_utc_rollover_without_erasing_loss() {
        let path = std::env::temp_dir().join(format!(
            "polymarket-daily-risk-baseline-{}.json",
            uuid::Uuid::new_v4()
        ));
        let now_ms = system_now_ms();
        let first = open_daily_risk_baseline(&path, "100".parse().unwrap(), now_ms).unwrap();
        assert_eq!(first.value.baseline_equity_usdc.to_string(), "100");
        drop(first);

        let mut reopened =
            open_daily_risk_baseline(&path, "80".parse().unwrap(), now_ms.saturating_add(1))
                .unwrap();
        assert_eq!(reopened.value.baseline_equity_usdc.to_string(), "100");
        assert_eq!(
            daily_loss(reopened.value.baseline_equity_usdc, "80".parse().unwrap())
                .unwrap()
                .to_string(),
            "20"
        );
        reopened.value.utc_date = "1900-01-01".to_string();
        assert!(reopened
            .observe_and_rollover("40".parse().unwrap(), now_ms.saturating_add(2))
            .unwrap());
        assert_eq!(reopened.value.baseline_equity_usdc.to_string(), "100");
        assert_eq!(
            daily_loss(reopened.value.baseline_equity_usdc, "40".parse().unwrap())
                .unwrap()
                .to_string(),
            "60"
        );
        let mut account = LiveRiskSnapshot {
            updated_at_ms: now_ms.saturating_add(3),
            collateral_balance_usdc: "40".parse().unwrap(),
            current_equity_usdc: "40".parse().unwrap(),
            positions: Vec::new(),
            open_order_count: 0,
            risk_state: crate::risk::RiskState::default(),
        };
        apply_equity_high_water(&mut reopened, &mut account, now_ms.saturating_add(3)).unwrap();
        assert_eq!(account.risk_state.daily_loss_usdc.to_string(), "60");

        account.current_equity_usdc = "150".parse().unwrap();
        apply_equity_high_water(&mut reopened, &mut account, now_ms.saturating_add(4)).unwrap();
        assert_eq!(reopened.value.baseline_equity_usdc.to_string(), "150");
        assert_eq!(account.risk_state.daily_loss_usdc.to_string(), "0");

        account.current_equity_usdc = "110".parse().unwrap();
        apply_equity_high_water(&mut reopened, &mut account, now_ms.saturating_add(5)).unwrap();
        assert_eq!(reopened.value.baseline_equity_usdc.to_string(), "150");
        assert_eq!(account.risk_state.daily_loss_usdc.to_string(), "40");
        drop(reopened);

        std::fs::remove_file(&path).unwrap();
        std::fs::remove_file(path.with_extension("json.lock")).unwrap();
    }
}
