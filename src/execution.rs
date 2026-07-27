use crate::compliance::{check_compliance, ComplianceState};
use crate::config::Settings;
use crate::error::{BotError, Result};
use crate::heartbeat::HeartbeatState;
use crate::journal::{Journal, JournalEventKind};
use crate::matching_engine::{MatchingEngineMode, MatchingEngineState};
use crate::paper::PaperEngine;
use crate::readiness::{require_live_ready, ReadinessState};
use crate::reconcile::{require_recovered, RecoveryReport};
use crate::reconcile::{AuthoritativeTrade, RemoteSnapshot};
use crate::risk::{check_order, RiskState};
use crate::types::{
    AssetId, BookState, BotMode, ConditionId, MarketMeta, OrderIntent, OrderStatus,
    OwnRestingOrder, Side, SignatureType as ConfigSignatureType, TimeInForce,
};
use alloy::signers::local::PrivateKeySigner;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::{stream, StreamExt as _};
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::{Credentials, LocalSigner, Normal, Signer as _};
use polymarket_client_sdk_v2::clob::types::request::{
    BalanceAllowanceRequest, OrdersRequest, TradesRequest,
};
use polymarket_client_sdk_v2::clob::types::response::TradeResponse;
use polymarket_client_sdk_v2::clob::types::{
    AssetType, OrderStatusType as ApiOrderStatus, OrderType as ApiOrderType, Side as ApiSide,
    SignatureType as ApiSignatureType, TraderSide as ApiTraderSide,
};
use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk_v2::data::types::request::PositionsRequest;
use polymarket_client_sdk_v2::data::Client as DataClient;
use polymarket_client_sdk_v2::types::{Address, Decimal, U256};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::str::FromStr as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const SDK_REQUEST_TIMEOUT: Duration = Duration::from_millis(1_500);
const SNAPSHOT_OPERATION_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionResult {
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub status: OrderStatus,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckedSubmission {
    Recorded(ExecutionResult),
    Ambiguous {
        result: Option<ExecutionResult>,
        reason: String,
    },
}

#[async_trait]
pub trait ExecutionAdapter: Send {
    async fn submit(
        &mut self,
        intent: &OrderIntent,
        client_order_id: &str,
        now_ms: u64,
    ) -> Result<ExecutionResult>;
}

#[async_trait]
impl<T> ExecutionAdapter for Box<T>
where
    T: ExecutionAdapter + ?Sized,
{
    async fn submit(
        &mut self,
        intent: &OrderIntent,
        client_order_id: &str,
        now_ms: u64,
    ) -> Result<ExecutionResult> {
        (**self).submit(intent, client_order_id, now_ms).await
    }
}

#[derive(Debug)]
pub struct PaperExecution {
    engine: PaperEngine,
}

impl PaperExecution {
    pub fn new(engine: PaperEngine) -> Self {
        Self { engine }
    }

    pub fn engine(&self) -> &PaperEngine {
        &self.engine
    }

    pub fn engine_mut(&mut self) -> &mut PaperEngine {
        &mut self.engine
    }
}

impl Default for PaperExecution {
    fn default() -> Self {
        Self {
            engine: PaperEngine::new(crate::fixed::Fixed::from_scaled(10_000_000_000), 0, 2_500)
                .expect("static paper engine defaults are valid"),
        }
    }
}

#[async_trait]
impl ExecutionAdapter for PaperExecution {
    async fn submit(
        &mut self,
        intent: &OrderIntent,
        client_order_id: &str,
        now_ms: u64,
    ) -> Result<ExecutionResult> {
        self.engine
            .submit(intent.clone(), client_order_id.to_string(), now_ms)
    }
}

pub struct LiveExecution {
    client: ClobClient<Authenticated<Normal>>,
    signer: PrivateKeySigner,
    api_key: Uuid,
    heartbeat_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveAccountProof {
    pub signer_address: String,
    pub balance_usdc: crate::fixed::Fixed,
    pub allowance_configured: bool,
    pub closed_only: bool,
    pub server_clock_drift_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveCancelResult {
    pub canceled: Vec<String>,
    pub not_canceled: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LivePositionSnapshot {
    pub asset_id: AssetId,
    pub size: crate::fixed::Fixed,
    pub average_price: crate::fixed::Fixed,
    pub current_value_usdc: crate::fixed::Fixed,
    pub cash_pnl_usdc: crate::fixed::Fixed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveRiskSnapshot {
    pub updated_at_ms: u64,
    pub collateral_balance_usdc: crate::fixed::Fixed,
    pub current_equity_usdc: crate::fixed::Fixed,
    pub positions: Vec<LivePositionSnapshot>,
    pub open_order_count: usize,
    pub risk_state: RiskState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveAccountComponents {
    collateral_balance_usdc: crate::fixed::Fixed,
    positions: Vec<LivePositionSnapshot>,
    open_orders: Vec<OwnRestingOrder>,
    open_order_ids: BTreeSet<String>,
    positions_complete: bool,
    orders_complete: bool,
}

impl LiveExecution {
    pub async fn from_environment(settings: &Settings) -> Result<Self> {
        if settings.mode != BotMode::Live || !settings.live_confirmation_valid() {
            return Err(BotError::Readiness(
                "live_execution_requires_explicit_confirmation".to_string(),
            ));
        }
        let private_key = required_env("PRIVATE_KEY")?;
        let api_key = Uuid::parse_str(&required_env("POLYMARKET_API_KEY")?)
            .map_err(|_| BotError::Config("polymarket_api_key_invalid".to_string()))?;
        let credentials = Credentials::new(
            api_key,
            required_env("POLYMARKET_API_SECRET")?,
            required_env("POLYMARKET_API_PASSPHRASE")?,
        );
        let signer = LocalSigner::from_str(&private_key)
            .map_err(|_| BotError::Config("private_key_invalid".to_string()))?
            .with_chain_id(Some(settings.chain_id));
        drop(private_key);

        if settings.signature_type == ConfigSignatureType::Eoa
            && !signer
                .address()
                .to_string()
                .eq_ignore_ascii_case(&settings.funder_address)
        {
            return Err(BotError::Readiness(
                "eoa_signer_does_not_match_configured_funder".to_string(),
            ));
        }

        if settings.signature_type == ConfigSignatureType::Poly1271 {
            let deposit = required_env("DEPOSIT_WALLET_ADDRESS")?;
            if !deposit.eq_ignore_ascii_case(&settings.funder_address) {
                return Err(BotError::Readiness(
                    "deposit_wallet_does_not_match_configured_funder".to_string(),
                ));
            }
        }
        let api_signature = map_signature_type(settings.signature_type);
        let base = ClobClient::new(
            &settings.clob_host,
            ClobConfig::builder().use_server_time(true).build(),
        )
        .map_err(|error| BotError::Protocol(format!("live_clob_client:{error}")))?;
        let builder = base
            .authentication_builder(&signer)
            .credentials(credentials)
            .signature_type(api_signature);
        let client = if settings.signature_type == ConfigSignatureType::Eoa {
            bounded_sdk_request("live_authentication", builder.authenticate()).await
        } else {
            let funder = Address::from_str(&settings.funder_address)
                .map_err(|_| BotError::Config("funder_address_invalid".to_string()))?;
            bounded_sdk_request("live_authentication", builder.funder(funder).authenticate()).await
        }?;
        Ok(Self {
            client,
            signer,
            api_key,
            heartbeat_id: None,
        })
    }

    pub async fn verify_account(&mut self, now_ms: u64) -> Result<LiveAccountProof> {
        let server_time = bounded_sdk_request("server_time", self.client.server_time()).await?;
        let server_ms = u64::try_from(server_time)
            .ok()
            .and_then(|value| value.checked_mul(1_000))
            .ok_or_else(|| BotError::Protocol("server_time_invalid".to_string()))?;
        let server_clock_drift_ms = now_ms.abs_diff(server_ms);
        let balance = bounded_sdk_request(
            "balance_allowance",
            self.client.balance_allowance(
                BalanceAllowanceRequest::builder()
                    .asset_type(AssetType::Collateral)
                    .build(),
            ),
        )
        .await?;
        let closed_only = bounded_sdk_request("closed_only_mode", self.client.closed_only_mode())
            .await?
            .closed_only;
        self.post_heartbeat().await?;
        Ok(LiveAccountProof {
            signer_address: self.signer.address().to_string(),
            balance_usdc: balance.balance.to_string().parse()?,
            allowance_configured: allowances_configured(
                balance.allowances.values().map(String::as_str),
            ),
            closed_only,
            server_clock_drift_ms,
        })
    }

    pub async fn post_heartbeat(&mut self) -> Result<()> {
        let response = bounded_sdk_request(
            "heartbeat_post",
            self.client.post_heartbeat(self.heartbeat_id),
        )
        .await
        .map_err(|error| BotError::Readiness(error.to_string()))?;
        if let Some(error) = response.error.filter(|error| !error.trim().is_empty()) {
            return Err(BotError::Readiness(format!("heartbeat_rejected:{error}")));
        }
        self.heartbeat_id = Some(response.heartbeat_id);
        Ok(())
    }

    pub async fn cancel_order(&self, order_id: &str) -> Result<bool> {
        let response = bounded_sdk_request("cancel_order", self.client.cancel_order(order_id))
            .await
            .map_err(|error| BotError::Execution(error.to_string()))?;
        if let Some(reason) = response.not_canceled.get(order_id) {
            return Err(BotError::Execution(format!(
                "cancel_order_rejected:{reason}"
            )));
        }
        Ok(response.canceled.iter().any(|value| value == order_id))
    }

    pub async fn cancel_all(&self) -> Result<Vec<String>> {
        let response = bounded_sdk_request("cancel_all", self.client.cancel_all_orders())
            .await
            .map_err(|error| BotError::Execution(error.to_string()))?;
        if !response.not_canceled.is_empty() {
            return Err(BotError::Execution(format!(
                "cancel_all_partial_failure:{}",
                response.not_canceled.len()
            )));
        }
        Ok(response.canceled)
    }

    pub async fn cancel_orders_tracked(&self, order_ids: &[String]) -> Result<LiveCancelResult> {
        if order_ids.is_empty() {
            return Ok(LiveCancelResult {
                canceled: Vec::new(),
                not_canceled: BTreeMap::new(),
            });
        }
        let references = order_ids.iter().map(String::as_str).collect::<Vec<_>>();
        let response =
            bounded_sdk_request("cancel_tracked", self.client.cancel_orders(&references))
                .await
                .map_err(|error| BotError::Execution(error.to_string()))?;
        Ok(LiveCancelResult {
            canceled: response.canceled,
            not_canceled: response.not_canceled.into_iter().collect(),
        })
    }

    pub async fn remote_snapshot(&self) -> Result<RemoteSnapshot> {
        bounded_operation(
            "remote_snapshot_operation",
            SNAPSHOT_OPERATION_TIMEOUT,
            self.remote_snapshot_inner(),
        )
        .await
    }

    pub async fn order_statuses(
        &self,
        order_ids: &[String],
    ) -> Result<BTreeMap<String, OrderStatus>> {
        let order_ids = order_ids.to_vec();
        bounded_operation(
            "order_status_recovery_operation",
            SNAPSHOT_OPERATION_TIMEOUT,
            async {
                let results = stream::iter(order_ids.into_iter().map(|order_id| async move {
                    let response =
                        bounded_sdk_request("recover_order_status", self.client.order(&order_id))
                            .await?;
                    validate_order_status_identity(&order_id, &response.id)?;
                    let status = map_api_order_status(response.status);
                    Ok((order_id, status))
                }))
                .buffer_unordered(8)
                .collect::<Vec<Result<(String, OrderStatus)>>>()
                .await;
                let mut statuses = BTreeMap::new();
                for result in results {
                    let (order_id, status) = result?;
                    statuses.insert(order_id, status);
                }
                Ok(statuses)
            },
        )
        .await
    }

    async fn remote_snapshot_inner(&self) -> Result<RemoteSnapshot> {
        const TERMINAL_CURSOR: &str = "LTE=";
        const MAX_PAGES: usize = 100;
        let mut open_order_ids = BTreeSet::new();
        let mut cursor = None;
        let orders_request = OrdersRequest::builder().build();
        let mut orders_complete = false;
        for _ in 0..MAX_PAGES {
            let page = bounded_sdk_request(
                "reconcile_orders",
                self.client.orders(&orders_request, cursor),
            )
            .await?;
            open_order_ids.extend(page.data.into_iter().map(|order| order.id));
            if page.next_cursor == TERMINAL_CURSOR {
                orders_complete = true;
                break;
            }
            cursor = Some(page.next_cursor);
        }

        let mut trade_order_ids = BTreeSet::new();
        let mut trades = BTreeMap::new();
        let mut cursor = None;
        let trades_request = TradesRequest::builder().build();
        let mut trades_complete = false;
        for _ in 0..MAX_PAGES {
            let page = bounded_sdk_request(
                "reconcile_trades",
                self.client.trades(&trades_request, cursor),
            )
            .await?;
            for trade in page.data {
                let authoritative = authoritative_trade_from_response(&trade, self.api_key)?;
                trade_order_ids.extend(authoritative.order_ids.iter().cloned());
                if let Some(existing) =
                    trades.insert(authoritative.trade_id.clone(), authoritative.clone())
                {
                    if existing != authoritative {
                        return Err(BotError::Protocol(format!(
                            "conflicting_trade_id_across_pages:{}",
                            trade.id
                        )));
                    }
                }
            }
            if page.next_cursor == TERMINAL_CURSOR {
                trades_complete = true;
                break;
            }
            cursor = Some(page.next_cursor);
        }
        let mut unresolved_gaps = Vec::new();
        if !orders_complete {
            unresolved_gaps.push("open_order_pagination_limit".to_string());
        }
        if !trades_complete {
            unresolved_gaps.push("trade_pagination_limit".to_string());
        }
        Ok(RemoteSnapshot {
            open_orders_loaded: orders_complete,
            trades_loaded: trades_complete,
            balances_loaded: true,
            allowances_loaded: true,
            open_order_ids,
            trade_order_ids,
            trades,
            unresolved_gaps,
        })
    }

    async fn account_components(
        &self,
        data_api_host: &str,
        funder_address: &str,
    ) -> Result<LiveAccountComponents> {
        const TERMINAL_CURSOR: &str = "LTE=";
        const MAX_ORDER_PAGES: usize = 100;
        const POSITION_PAGE_SIZE: i32 = 500;
        const MAX_POSITION_OFFSET: i32 = 10_000;

        let balance = bounded_sdk_request(
            "risk_balance",
            self.client.balance_allowance(
                BalanceAllowanceRequest::builder()
                    .asset_type(AssetType::Collateral)
                    .build(),
            ),
        )
        .await?;
        let collateral_balance_usdc: crate::fixed::Fixed = balance.balance.to_string().parse()?;
        if collateral_balance_usdc < crate::fixed::Fixed::ZERO {
            return Err(BotError::Protocol(
                "negative_collateral_balance".to_string(),
            ));
        }

        let data = DataClient::new(data_api_host)
            .map_err(|error| BotError::Protocol(format!("risk_data_client:{error}")))?;
        let address = Address::from_str(funder_address)
            .map_err(|_| BotError::Config("risk_funder_address_invalid".to_string()))?;
        let mut offset = 0i32;
        let mut positions = Vec::new();
        let mut positions_complete = false;
        while offset <= MAX_POSITION_OFFSET {
            let request = PositionsRequest::builder()
                .user(address)
                .size_threshold(Decimal::ZERO)
                .limit(POSITION_PAGE_SIZE)
                .map_err(|error| BotError::Config(format!("position_limit:{error}")))?
                .offset(offset)
                .map_err(|error| BotError::Config(format!("position_offset:{error}")))?
                .build();
            let page = bounded_sdk_request("risk_positions", data.positions(&request)).await?;
            let count = page.len();
            for position in page {
                let size: crate::fixed::Fixed = position.size.to_string().parse()?;
                let current_value_usdc: crate::fixed::Fixed =
                    position.current_value.to_string().parse()?;
                if size < crate::fixed::Fixed::ZERO
                    || current_value_usdc < crate::fixed::Fixed::ZERO
                {
                    return Err(BotError::Protocol(
                        "negative_position_value_from_data_api".to_string(),
                    ));
                }
                positions.push(LivePositionSnapshot {
                    asset_id: AssetId::from(position.asset.to_string()),
                    size,
                    average_price: position.avg_price.to_string().parse()?,
                    current_value_usdc,
                    cash_pnl_usdc: position.cash_pnl.to_string().parse()?,
                });
            }
            if count < usize::try_from(POSITION_PAGE_SIZE).unwrap_or(usize::MAX) {
                positions_complete = true;
                break;
            }
            offset = offset
                .checked_add(POSITION_PAGE_SIZE)
                .ok_or_else(|| BotError::Protocol("position_offset_overflow".to_string()))?;
        }
        positions.sort_by(|left, right| {
            left.asset_id
                .as_ref()
                .cmp(right.asset_id.as_ref())
                .then_with(|| left.size.cmp(&right.size))
                .then_with(|| left.average_price.cmp(&right.average_price))
        });

        let mut open_orders = Vec::new();
        let mut open_order_ids = BTreeSet::new();
        let mut cursor = None;
        let request = OrdersRequest::builder().build();
        let mut orders_complete = false;
        for _ in 0..MAX_ORDER_PAGES {
            let page =
                bounded_sdk_request("risk_open_orders", self.client.orders(&request, cursor))
                    .await?;
            for order in page.data {
                if !open_order_ids.insert(order.id.clone()) {
                    return Err(BotError::Protocol(
                        "duplicate_open_order_id_across_pages".to_string(),
                    ));
                }
                let original: crate::fixed::Fixed = order.original_size.to_string().parse()?;
                let matched: crate::fixed::Fixed = order.size_matched.to_string().parse()?;
                let remaining = original.checked_sub(matched)?;
                if remaining < crate::fixed::Fixed::ZERO {
                    return Err(BotError::Protocol(
                        "open_order_matched_size_exceeds_original".to_string(),
                    ));
                }
                let side = match order.side {
                    ApiSide::Buy => Side::Buy,
                    ApiSide::Sell => Side::Sell,
                    _ => {
                        return Err(BotError::Protocol(
                            "unsupported_open_order_side".to_string(),
                        ));
                    }
                };
                open_orders.push(OwnRestingOrder {
                    asset_id: AssetId::from(order.asset_id.to_string()),
                    side,
                    price: order.price.to_string().parse()?,
                    size: remaining,
                });
            }
            if page.next_cursor == TERMINAL_CURSOR {
                orders_complete = true;
                break;
            }
            cursor = Some(page.next_cursor);
        }
        open_orders.sort_by(|left, right| {
            left.asset_id
                .as_ref()
                .cmp(right.asset_id.as_ref())
                .then_with(|| side_rank(left.side).cmp(&side_rank(right.side)))
                .then_with(|| left.price.cmp(&right.price))
                .then_with(|| left.size.cmp(&right.size))
        });

        Ok(LiveAccountComponents {
            collateral_balance_usdc,
            positions,
            open_orders,
            open_order_ids,
            positions_complete,
            orders_complete,
        })
    }

    pub async fn risk_snapshot(
        &self,
        data_api_host: &str,
        funder_address: &str,
        market_asset: &AssetId,
        daily_baseline_equity_usdc: crate::fixed::Fixed,
        prohibited_conduct_flag: bool,
        now_ms: u64,
    ) -> Result<LiveRiskSnapshot> {
        bounded_operation(
            "live_account_snapshot_operation",
            SNAPSHOT_OPERATION_TIMEOUT,
            self.risk_snapshot_inner(
                data_api_host,
                funder_address,
                market_asset,
                daily_baseline_equity_usdc,
                prohibited_conduct_flag,
                now_ms,
            ),
        )
        .await
    }

    async fn risk_snapshot_inner(
        &self,
        data_api_host: &str,
        funder_address: &str,
        market_asset: &AssetId,
        daily_baseline_equity_usdc: crate::fixed::Fixed,
        prohibited_conduct_flag: bool,
        now_ms: u64,
    ) -> Result<LiveRiskSnapshot> {
        let first = self
            .account_components(data_api_host, funder_address)
            .await?;
        let second = self
            .account_components(data_api_host, funder_address)
            .await?;
        if !account_components_consistent(&first, &second) {
            return Err(BotError::Readiness(
                "live_account_changed_during_snapshot".to_string(),
            ));
        }
        let LiveAccountComponents {
            collateral_balance_usdc: collateral_balance,
            positions,
            open_orders: own_resting_orders,
            open_order_ids: _,
            positions_complete,
            orders_complete,
        } = second;
        if !positions_complete || !orders_complete {
            return Err(BotError::Readiness(
                "live_account_snapshot_pagination_incomplete".to_string(),
            ));
        }

        let mut pending_buy_total = crate::fixed::Fixed::ZERO;
        let mut pending_buy_market = crate::fixed::Fixed::ZERO;
        let mut pending_sell_market = crate::fixed::Fixed::ZERO;
        for order in &own_resting_orders {
            if order.side == Side::Buy {
                let notional = order.price.checked_mul_ceil(order.size)?;
                pending_buy_total = pending_buy_total.checked_add(notional)?;
                if &order.asset_id == market_asset {
                    pending_buy_market = pending_buy_market.checked_add(notional)?;
                }
            } else if &order.asset_id == market_asset {
                pending_sell_market = pending_sell_market.checked_add(order.size)?;
            }
        }

        let position_total = positions
            .iter()
            .try_fold(crate::fixed::Fixed::ZERO, |total, position| {
                total.checked_add(position.current_value_usdc)
            })?;
        let position_market = positions
            .iter()
            .filter(|position| &position.asset_id == market_asset)
            .try_fold(crate::fixed::Fixed::ZERO, |total, position| {
                total.checked_add(position.current_value_usdc)
            })?;
        let held_market_size = positions
            .iter()
            .filter(|position| &position.asset_id == market_asset)
            .try_fold(crate::fixed::Fixed::ZERO, |total, position| {
                total.checked_add(position.size)
            })?;
        if pending_sell_market > held_market_size {
            return Err(BotError::Protocol(
                "pending_sell_exceeds_position".to_string(),
            ));
        }
        let available_position = held_market_size.checked_sub(pending_sell_market)?;
        let current_equity = collateral_balance.checked_add(position_total)?;
        let daily_loss = if current_equity < daily_baseline_equity_usdc {
            daily_baseline_equity_usdc.checked_sub(current_equity)?
        } else {
            crate::fixed::Fixed::ZERO
        };
        let available_cash = if pending_buy_total >= collateral_balance {
            crate::fixed::Fixed::ZERO
        } else {
            collateral_balance.checked_sub(pending_buy_total)?
        };
        let risk_state = RiskState {
            now_ms,
            market_exposure_usdc: position_market.checked_add(pending_buy_market)?,
            total_exposure_usdc: position_total.checked_add(pending_buy_total)?,
            daily_loss_usdc: daily_loss,
            available_cash_usdc: Some(available_cash),
            available_position_size: Some(available_position),
            own_resting_orders,
            own_order_cache_certain: true,
            prohibited_conduct_flag,
            behavioral_pressure: None,
        };
        Ok(LiveRiskSnapshot {
            updated_at_ms: now_ms,
            collateral_balance_usdc: collateral_balance,
            current_equity_usdc: current_equity,
            positions,
            open_order_count: risk_state.own_resting_orders.len(),
            risk_state,
        })
    }
}

fn authoritative_trade_from_response(
    trade: &TradeResponse,
    api_key: Uuid,
) -> Result<AuthoritativeTrade> {
    if trade.id.trim().is_empty() {
        return Err(BotError::Protocol(
            "empty_authoritative_trade_id".to_string(),
        ));
    }
    let (asset_id, side, price, size, fee_rate_bps, order_ids) = match &trade.trader_side {
        ApiTraderSide::Taker => {
            if trade.owner != api_key {
                return Err(BotError::Protocol(format!(
                    "authoritative_taker_owner_mismatch:{}",
                    trade.id
                )));
            }
            if trade.taker_order_id.trim().is_empty() {
                return Err(BotError::Protocol(format!(
                    "authoritative_taker_order_id_missing:{}",
                    trade.id
                )));
            }
            (
                AssetId::from(trade.asset_id.to_string()),
                convert_api_side(trade.side)?,
                trade.price.to_string().parse()?,
                trade.size.to_string().parse()?,
                trade.fee_rate_bps.to_string().parse()?,
                [trade.taker_order_id.clone()].into_iter().collect(),
            )
        }
        ApiTraderSide::Maker => {
            let own_orders = trade
                .maker_orders
                .iter()
                .filter(|order| order.owner == api_key)
                .collect::<Vec<_>>();
            let [maker] = own_orders.as_slice() else {
                return Err(BotError::Protocol(format!(
                    "authoritative_maker_order_cardinality:{}:{}",
                    trade.id,
                    own_orders.len()
                )));
            };
            if maker.order_id.trim().is_empty() {
                return Err(BotError::Protocol(format!(
                    "authoritative_maker_order_id_missing:{}",
                    trade.id
                )));
            }
            (
                AssetId::from(maker.asset_id.to_string()),
                convert_api_side(maker.side)?,
                maker.price.to_string().parse()?,
                maker.matched_amount.to_string().parse()?,
                maker.fee_rate_bps.to_string().parse()?,
                [maker.order_id.clone()].into_iter().collect(),
            )
        }
        ApiTraderSide::Unknown(value) => {
            return Err(BotError::Protocol(format!(
                "unsupported_authoritative_trader_side:{}:{value}",
                trade.id
            )));
        }
        _ => {
            return Err(BotError::Protocol(format!(
                "unsupported_authoritative_trader_side:{}",
                trade.id
            )));
        }
    };
    if asset_id.as_ref().is_empty() || size <= crate::fixed::Fixed::ZERO {
        return Err(BotError::Protocol(format!(
            "authoritative_trade_terms_invalid:{}",
            trade.id
        )));
    }
    if price <= crate::fixed::Fixed::ZERO || price >= crate::fixed::Fixed::ONE {
        return Err(BotError::Protocol(format!(
            "authoritative_trade_price_invalid:{}",
            trade.id
        )));
    }
    if fee_rate_bps < crate::fixed::Fixed::ZERO || fee_rate_bps > "10000".parse()? {
        return Err(BotError::Protocol(format!(
            "authoritative_trade_fee_rate_invalid:{}",
            trade.id
        )));
    }
    let timestamp_ms = u64::try_from(trade.match_time.timestamp_millis()).map_err(|_| {
        BotError::Protocol(format!(
            "authoritative_trade_timestamp_invalid:{}",
            trade.id
        ))
    })?;
    Ok(AuthoritativeTrade {
        trade_id: trade.id.clone(),
        condition_id: ConditionId::from(trade.market.to_string()),
        asset_id,
        side,
        price,
        size,
        fee_rate_bps,
        status: trade.status.to_string(),
        order_ids,
        timestamp_ms: Some(timestamp_ms),
    })
}

fn convert_api_side(side: ApiSide) -> Result<Side> {
    match side {
        ApiSide::Buy => Ok(Side::Buy),
        ApiSide::Sell => Ok(Side::Sell),
        _ => Err(BotError::Protocol(
            "unsupported_authoritative_trade_side".to_string(),
        )),
    }
}

fn validate_order_status_identity(expected: &str, actual: &str) -> Result<()> {
    if expected == actual {
        Ok(())
    } else {
        Err(BotError::Protocol(
            "order_status_identity_mismatch".to_string(),
        ))
    }
}

fn map_api_order_status(status: ApiOrderStatus) -> OrderStatus {
    match status {
        ApiOrderStatus::Live | ApiOrderStatus::Delayed | ApiOrderStatus::Unmatched => {
            OrderStatus::Acknowledged
        }
        ApiOrderStatus::Matched => OrderStatus::Filled,
        ApiOrderStatus::Canceled => OrderStatus::Cancelled,
        ApiOrderStatus::Unknown(_) => OrderStatus::Unknown,
        _ => OrderStatus::Unknown,
    }
}

#[async_trait]
impl ExecutionAdapter for LiveExecution {
    async fn submit(
        &mut self,
        intent: &OrderIntent,
        client_order_id: &str,
        _now_ms: u64,
    ) -> Result<ExecutionResult> {
        let token_id = U256::from_str(intent.asset_id.as_ref())
            .map_err(|_| BotError::Execution("live_token_id_invalid".to_string()))?;
        let price = Decimal::from_str(&intent.limit_price.to_string())
            .map_err(|error| BotError::Execution(format!("live_price:{error}")))?;
        let size = Decimal::from_str(&intent.size.to_string())
            .map_err(|error| BotError::Execution(format!("live_size:{error}")))?;
        let order_type = map_order_type(intent.time_in_force);
        let mut builder = self
            .client
            .limit_order()
            .token_id(token_id)
            .side(map_side(intent.side))
            .price(price)
            .size(size)
            .order_type(order_type)
            .post_only(intent.post_only);
        if intent.time_in_force == TimeInForce::Gtd {
            let expiration = intent
                .wire_expiration_s
                .and_then(|timestamp| i64::try_from(timestamp).ok())
                .and_then(|timestamp| DateTime::<Utc>::from_timestamp(timestamp, 0))
                .ok_or_else(|| BotError::Execution("live_gtd_expiration_invalid".to_string()))?;
            builder = builder.expiration(expiration);
        }
        let response =
            bounded_sdk_request("live_order_post", builder.build_sign_and_post(&self.signer))
                .await
                .map_err(|error| BotError::Execution(error.to_string()))?;
        let response_has_error = response
            .error_msg
            .as_deref()
            .is_some_and(|message| !message.trim().is_empty());
        let status = if !response.success {
            if response.order_id.is_empty() {
                OrderStatus::Rejected
            } else {
                OrderStatus::Unknown
            }
        } else if response.order_id.is_empty() || response_has_error {
            OrderStatus::Unknown
        } else {
            match response.status {
                ApiOrderStatus::Live | ApiOrderStatus::Unmatched => OrderStatus::Acknowledged,
                ApiOrderStatus::Matched => OrderStatus::Filled,
                ApiOrderStatus::Canceled => OrderStatus::Cancelled,
                ApiOrderStatus::Delayed => OrderStatus::Submitted,
                ApiOrderStatus::Unknown(_) => OrderStatus::Unknown,
                _ => OrderStatus::Unknown,
            }
        };
        Ok(ExecutionResult {
            client_order_id: client_order_id.to_string(),
            exchange_order_id: (!response.order_id.is_empty()).then_some(response.order_id),
            status,
            message: response
                .error_msg
                .unwrap_or_else(|| response.status.to_string()),
        })
    }
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| BotError::Config(format!("missing_secret:{name}")))
}

async fn bounded_sdk_request<T, E, F>(label: &str, future: F) -> Result<T>
where
    E: std::fmt::Display,
    F: Future<Output = std::result::Result<T, E>>,
{
    match tokio::time::timeout(SDK_REQUEST_TIMEOUT, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(BotError::Protocol(format!("{label}:{error}"))),
        Err(_) => Err(BotError::Protocol(format!("{label}:timeout"))),
    }
}

async fn bounded_operation<T, F>(label: &str, deadline: Duration, future: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    match tokio::time::timeout(deadline, future).await {
        Ok(result) => result,
        Err(_) => Err(BotError::Protocol(format!("{label}:timeout"))),
    }
}

fn map_signature_type(value: ConfigSignatureType) -> ApiSignatureType {
    match value {
        ConfigSignatureType::Poly1271 => ApiSignatureType::Poly1271,
        ConfigSignatureType::GnosisSafe => ApiSignatureType::GnosisSafe,
        ConfigSignatureType::Proxy => ApiSignatureType::Proxy,
        ConfigSignatureType::Eoa => ApiSignatureType::Eoa,
    }
}

fn map_order_type(value: TimeInForce) -> ApiOrderType {
    match value {
        TimeInForce::Gtc => ApiOrderType::GTC,
        TimeInForce::Gtd => ApiOrderType::GTD,
        TimeInForce::Fok => ApiOrderType::FOK,
        TimeInForce::Fak => ApiOrderType::FAK,
    }
}

fn map_side(value: Side) -> ApiSide {
    match value {
        Side::Buy => ApiSide::Buy,
        Side::Sell => ApiSide::Sell,
    }
}

const fn side_rank(value: Side) -> u8 {
    match value {
        Side::Buy => 0,
        Side::Sell => 1,
    }
}

fn account_components_consistent(
    first: &LiveAccountComponents,
    second: &LiveAccountComponents,
) -> bool {
    first.collateral_balance_usdc == second.collateral_balance_usdc
        && first.positions_complete
        && second.positions_complete
        && first.orders_complete
        && second.orders_complete
        && first.open_order_ids == second.open_order_ids
        && first.open_orders == second.open_orders
        && first.positions.len() == second.positions.len()
        && first
            .positions
            .iter()
            .zip(&second.positions)
            .all(|(left, right)| {
                left.asset_id == right.asset_id
                    && left.size == right.size
                    && left.average_price == right.average_price
            })
}

fn nonzero_integer(value: &str) -> bool {
    let value = value.trim_start_matches('0');
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn allowances_configured<'a>(values: impl IntoIterator<Item = &'a str>) -> bool {
    let mut values = values.into_iter();
    let Some(first) = values.next() else {
        return false;
    };
    nonzero_integer(first) && values.all(nonzero_integer)
}

pub trait Clock {
    fn now_ms(&self) -> u64;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }
}

pub struct ExecutionRouter<A, C = SystemClock> {
    adapter: A,
    clock: C,
}

#[derive(Debug)]
pub struct ExecutionContext<'a> {
    pub settings: &'a Settings,
    pub readiness: &'a ReadinessState,
    pub compliance: &'a ComplianceState,
    pub heartbeat: &'a HeartbeatState,
    pub recovery: &'a RecoveryReport,
    pub matching_engine: &'a MatchingEngineState,
    pub market: &'a MarketMeta,
    pub book: Option<&'a BookState>,
    pub risk_state: &'a RiskState,
    pub account_revision: Option<(&'a AtomicU64, u64)>,
}

impl<A> ExecutionRouter<A, SystemClock> {
    pub fn new(adapter: A) -> Self {
        Self::with_clock(adapter, SystemClock)
    }
}

impl<A, C> ExecutionRouter<A, C> {
    pub fn with_clock(adapter: A, clock: C) -> Self {
        Self { adapter, clock }
    }

    pub fn adapter(&self) -> &A {
        &self.adapter
    }

    pub fn adapter_mut(&mut self) -> &mut A {
        &mut self.adapter
    }
}

impl<A: ExecutionAdapter, C: Clock> ExecutionRouter<A, C> {
    pub(crate) async fn submit_live_checked(
        &mut self,
        journal: &mut Journal,
        intent: &OrderIntent,
        context: &ExecutionContext<'_>,
    ) -> Result<CheckedSubmission> {
        if context.settings.mode != BotMode::Live {
            return Err(BotError::Execution(
                "submit_live_checked_requires_live_mode".to_string(),
            ));
        }
        self.submit_checked_inner(journal, intent, context).await
    }

    #[cfg(test)]
    async fn submit_checked(
        &mut self,
        journal: &mut Journal,
        intent: &OrderIntent,
        context: &ExecutionContext<'_>,
    ) -> Result<CheckedSubmission> {
        self.submit_checked_inner(journal, intent, context).await
    }

    async fn submit_checked_inner(
        &mut self,
        journal: &mut Journal,
        intent: &OrderIntent,
        context: &ExecutionContext<'_>,
    ) -> Result<CheckedSubmission> {
        let initial_checked_at_ms = self.clock.now_ms();
        self.check_gates(intent, context, initial_checked_at_ms)?;
        let client_order_id = format!("client-{}", Uuid::new_v4());
        let book = context.book;
        let source_event_refs = book
            .and_then(|value| value.book_hash.clone())
            .into_iter()
            .collect();
        journal.append_write_ahead_with_sources(
            format!("decision-{client_order_id}"),
            client_order_id.clone(),
            intent.strategy_id.clone(),
            "accepted",
            format!(
                "intent_v2|asset_id_hex={}|side={:?}|limit_price_raw={}|size_raw={}|time_in_force={:?}|post_only={}|local_expires_at_ms={}|wire_expiration_s={}|reason_hex={}|strategy_id_hex={}|feature_snapshot_id_hex={}|market_condition_id_hex={}|market_tick_size_raw={}|market_min_order_size_raw={}|book_hash_hex={}|book_exchange_timestamp_ms={}|book_local_received_at_ms={}|initial_checked_at_ms={}|settings_debug_hex={}|readiness_debug_hex={}|compliance_debug_hex={}|heartbeat_debug_hex={}|recovery_debug_hex={}|matching_engine_debug_hex={}|market_state_debug_hex={}|book_state_debug_hex={}|risk_state_debug_hex={}",
                hex_bytes(intent.asset_id.as_ref().as_bytes()),
                intent.side,
                intent.limit_price.raw(),
                intent.size.raw(),
                intent.time_in_force,
                intent.post_only,
                intent.local_expires_at_ms,
                intent
                    .wire_expiration_s
                    .map_or_else(|| "NONE".to_string(), |value| value.to_string()),
                hex_bytes(intent.reason.as_bytes()),
                hex_bytes(intent.strategy_id.as_bytes()),
                hex_bytes(intent.feature_snapshot_id.as_bytes()),
                hex_bytes(context.market.condition_id.as_ref().as_bytes()),
                context.market.tick_size.raw(),
                context.market.min_order_size.raw(),
                book.and_then(|value| value.book_hash.as_deref())
                    .map_or_else(|| "NONE".to_string(), |value| hex_bytes(value.as_bytes())),
                book.map_or(0, |value| value.exchange_timestamp_ms),
                book.map_or(0, |value| value.local_received_at_ms),
                initial_checked_at_ms,
                debug_hex(context.settings),
                debug_hex(context.readiness),
                debug_hex(context.compliance),
                debug_hex(context.heartbeat),
                debug_hex(context.recovery),
                debug_hex(context.matching_engine),
                debug_hex(context.market),
                book.map_or_else(|| "NONE".to_string(), debug_hex),
                debug_hex(context.risk_state),
            ),
            source_event_refs,
        )?;
        if let Err(error) = self.check_gates(intent, context, self.clock.now_ms()) {
            journal.append_lifecycle(
                JournalEventKind::Rejected,
                format!("decision-{client_order_id}"),
                client_order_id,
                None,
                intent.strategy_id.clone(),
                "rejected_before_submit",
                error.to_string(),
            )?;
            return Err(error);
        }
        let submit_started_at_ms = self.clock.now_ms();
        match self
            .adapter
            .submit(intent, &client_order_id, submit_started_at_ms)
            .await
        {
            Ok(result) => {
                let account_changed = !account_revision_matches(context);
                let journal_result = journal.append_lifecycle(
                    journal_kind_for_status(result.status),
                    format!("decision-{client_order_id}"),
                    client_order_id.clone(),
                    result.exchange_order_id.clone(),
                    intent.strategy_id.clone(),
                    format!("{:?}", result.status),
                    result.message.clone(),
                );
                match (account_changed, journal_result) {
                    (false, Ok(_)) => Ok(CheckedSubmission::Recorded(result)),
                    (true, Ok(_)) => Ok(CheckedSubmission::Ambiguous {
                        result: Some(result),
                        reason: "account_revision_changed_during_submission".to_string(),
                    }),
                    (account_changed, Err(error)) => Ok(CheckedSubmission::Ambiguous {
                        result: Some(result),
                        reason: if account_changed {
                            format!(
                                "account_revision_changed_during_submission;post_submit_journal_failure:{error}"
                            )
                        } else {
                            format!("post_submit_journal_failure:{error}")
                        },
                    }),
                }
            }
            Err(error) => {
                let live_submission = context.settings.mode == BotMode::Live;
                let journal_error = journal
                    .append_lifecycle(
                        if live_submission {
                            JournalEventKind::Submitted
                        } else {
                            JournalEventKind::Rejected
                        },
                        format!("decision-{client_order_id}"),
                        client_order_id,
                        None,
                        intent.strategy_id.clone(),
                        if live_submission {
                            "submission_state_unknown"
                        } else {
                            "adapter_rejected"
                        },
                        error.to_string(),
                    )
                    .err();
                if live_submission {
                    Ok(CheckedSubmission::Ambiguous {
                        result: None,
                        reason: journal_error.map_or_else(
                            || format!("live_submission_state_unknown:{error}"),
                            |journal_error| {
                                format!(
                                    "live_submission_state_unknown:{error};journal:{journal_error}"
                                )
                            },
                        ),
                    })
                } else if let Some(journal_error) = journal_error {
                    Err(journal_error)
                } else {
                    Err(error)
                }
            }
        }
    }

    fn check_gates(
        &self,
        intent: &OrderIntent,
        context: &ExecutionContext<'_>,
        now_ms: u64,
    ) -> Result<()> {
        if !account_revision_matches(context) {
            return Err(BotError::Readiness(
                "account_revision_changed_before_submit".to_string(),
            ));
        }
        let mut current_risk_state = context.risk_state.clone();
        current_risk_state.now_ms = now_ms;
        check_order(
            context.settings,
            context.market,
            context.book,
            intent,
            &current_risk_state,
        )
        .into_result()?;
        require_live_ready(context.settings, context.readiness)?;
        check_compliance(context.settings, context.compliance, now_ms)?;

        if context.settings.mode == BotMode::Live {
            context.heartbeat.require_healthy()?;
            require_recovered(context.recovery)?;
        }

        match context.matching_engine.mode {
            MatchingEngineMode::Normal => Ok(()),
            MatchingEngineMode::PostOnly | MatchingEngineMode::PostOnlyRecovery
                if intent.post_only =>
            {
                Ok(())
            }
            MatchingEngineMode::PostOnly | MatchingEngineMode::PostOnlyRecovery => {
                Err(BotError::Execution(
                    "matching_engine_post_only_requires_post_only_intent".to_string(),
                ))
            }
            MatchingEngineMode::Restarting => Err(BotError::Execution(
                "matching_engine_restarting".to_string(),
            )),
            MatchingEngineMode::CancelOnly => Err(BotError::Execution(
                "matching_engine_cancel_only".to_string(),
            )),
            MatchingEngineMode::ReadOnly => {
                Err(BotError::Execution("matching_engine_read_only".to_string()))
            }
        }
    }
}

impl<C: Clock> ExecutionRouter<PaperExecution, C> {
    pub fn stage_paper_submission(
        &self,
        _journal: &Journal,
        intent: &OrderIntent,
        context: &ExecutionContext<'_>,
    ) -> Result<(PaperEngine, ExecutionResult)> {
        let now_ms = self.clock.now_ms();
        self.check_gates(intent, context, now_ms)?;
        let client_order_id = format!("client-{}", Uuid::new_v4());
        let mut staged = self.adapter.engine().clone();
        let result = staged.submit(intent.clone(), client_order_id, now_ms)?;
        Ok((staged, result))
    }
}

fn journal_kind_for_status(status: OrderStatus) -> JournalEventKind {
    match status {
        OrderStatus::Submitted | OrderStatus::Unknown => JournalEventKind::Submitted,
        OrderStatus::Acknowledged => JournalEventKind::Acknowledged,
        OrderStatus::PartiallyFilled => JournalEventKind::PartiallyFilled,
        OrderStatus::Filled => JournalEventKind::Filled,
        OrderStatus::CancelRequested => JournalEventKind::CancelRequested,
        OrderStatus::Cancelled => JournalEventKind::Cancelled,
        OrderStatus::Rejected | OrderStatus::Intent | OrderStatus::WriteAhead => {
            JournalEventKind::Rejected
        }
    }
}

fn account_revision_matches(context: &ExecutionContext<'_>) -> bool {
    context
        .account_revision
        .is_none_or(|(revision, expected)| revision.load(Ordering::Acquire) == expected)
}

fn hex_bytes(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn debug_hex(value: &impl std::fmt::Debug) -> String {
    hex_bytes(format!("{value:?}").as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compliance::ComplianceState;
    use crate::fixed::Fixed;
    use crate::journal::{replay_records, JournalStartup};
    use crate::readiness::ReadinessState;
    use crate::reconcile::RecoveryReport;
    use crate::types::{AssetId, ConditionId, Level, Side, TimeInForce};
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone, Copy)]
    struct FixedClock(u64);

    impl Clock for FixedClock {
        fn now_ms(&self) -> u64 {
            self.0
        }
    }

    #[derive(Debug)]
    struct SequenceClock {
        times: Mutex<VecDeque<u64>>,
    }

    impl SequenceClock {
        fn new(times: impl IntoIterator<Item = u64>) -> Self {
            Self {
                times: Mutex::new(times.into_iter().collect()),
            }
        }
    }

    impl Clock for SequenceClock {
        fn now_ms(&self) -> u64 {
            self.times
                .lock()
                .expect("sequence clock lock")
                .pop_front()
                .expect("sequence clock exhausted")
        }
    }

    fn intent() -> OrderIntent {
        OrderIntent {
            asset_id: AssetId::from("a"),
            side: Side::Buy,
            limit_price: "0.5".parse().unwrap(),
            size: "1".parse().unwrap(),
            time_in_force: TimeInForce::Gtc,
            post_only: true,
            local_expires_at_ms: 800,
            wire_expiration_s: None,
            reason: "test".to_string(),
            strategy_id: "s".to_string(),
            feature_snapshot_id: "f".to_string(),
        }
    }

    struct CheckedContextInputs<'a> {
        settings: &'a Settings,
        readiness: &'a ReadinessState,
        compliance: &'a ComplianceState,
        heartbeat: &'a HeartbeatState,
        recovery: &'a RecoveryReport,
        market: &'a MarketMeta,
        book: &'a BookState,
        risk_state: &'a RiskState,
    }

    fn checked_context(inputs: CheckedContextInputs<'_>) -> ExecutionContext<'_> {
        let matching = MatchingEngineState::normal_after_verified_startup();
        ExecutionContext {
            settings: inputs.settings,
            readiness: inputs.readiness,
            compliance: inputs.compliance,
            heartbeat: inputs.heartbeat,
            recovery: inputs.recovery,
            matching_engine: Box::leak(Box::new(matching)),
            market: inputs.market,
            book: Some(inputs.book),
            risk_state: inputs.risk_state,
            account_revision: None,
        }
    }

    fn market() -> MarketMeta {
        MarketMeta {
            condition_id: ConditionId::from("c"),
            asset_id_yes: AssetId::from("a"),
            asset_id_no: AssetId::from("b"),
            tick_size: "0.001".parse().unwrap(),
            min_order_size: Fixed::ONE,
            neg_risk: false,
            active: true,
            accepting_orders: true,
            resolved: false,
            paused: false,
            taker_delay_enabled: false,
            fees_enabled: false,
        }
    }

    fn book() -> BookState {
        let mut book = BookState::empty("a", "0.001".parse().unwrap(), Fixed::ONE);
        book.bids = vec![Level {
            price: "0.499".parse().unwrap(),
            size: "10".parse().unwrap(),
        }];
        book.asks = vec![Level {
            price: "0.501".parse().unwrap(),
            size: "10".parse().unwrap(),
        }];
        book.best_bid = Some("0.499".parse().unwrap());
        book.best_ask = Some("0.501".parse().unwrap());
        book.book_hash = Some("book-hash".to_string());
        book.exchange_timestamp_ms = 90;
        book.local_received_at_ms = 100;
        book.tradeable = true;
        book
    }

    fn risk_state() -> RiskState {
        RiskState {
            now_ms: 0,
            own_order_cache_certain: true,
            ..RiskState::default()
        }
    }

    fn temp_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "polymarket_rs_execution_test_{}.log",
            uuid::Uuid::new_v4()
        ))
    }

    struct AmbiguousLiveAdapter;

    #[async_trait]
    impl ExecutionAdapter for AmbiguousLiveAdapter {
        async fn submit(
            &mut self,
            _intent: &OrderIntent,
            _client_order_id: &str,
            _now_ms: u64,
        ) -> Result<ExecutionResult> {
            Err(BotError::Execution(
                "transport_closed_after_write".to_string(),
            ))
        }
    }

    struct RevisionChangingAdapter {
        revision: Arc<AtomicU64>,
    }

    #[async_trait]
    impl ExecutionAdapter for RevisionChangingAdapter {
        async fn submit(
            &mut self,
            _intent: &OrderIntent,
            client_order_id: &str,
            _now_ms: u64,
        ) -> Result<ExecutionResult> {
            self.revision.fetch_add(1, Ordering::AcqRel);
            Ok(ExecutionResult {
                client_order_id: client_order_id.to_string(),
                exchange_order_id: Some("exchange-1".to_string()),
                status: OrderStatus::Acknowledged,
                message: "accepted".to_string(),
            })
        }
    }

    #[tokio::test]
    async fn paper_execution_runs_after_write_ahead() {
        let path = temp_path();
        let _ = std::fs::remove_file(&path);
        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        let mut router = ExecutionRouter::with_clock(PaperExecution::default(), FixedClock(110));
        let settings = Settings::default();
        let readiness = ReadinessState::default();
        let compliance = ComplianceState::default();
        let heartbeat = HeartbeatState::default();
        let recovery = RecoveryReport {
            live_unlock_allowed: false,
            reason: "paper".to_string(),
            replayed_records: 0,
            in_flight_attempts: 0,
        };
        let market = market();
        let book = book();
        let risk_state = risk_state();
        let context = checked_context(CheckedContextInputs {
            settings: &settings,
            readiness: &readiness,
            compliance: &compliance,
            heartbeat: &heartbeat,
            recovery: &recovery,
            market: &market,
            book: &book,
            risk_state: &risk_state,
        });
        let outcome = router
            .submit_checked(&mut journal, &intent(), &context)
            .await
            .unwrap();
        let CheckedSubmission::Recorded(result) = outcome else {
            panic!("expected a durably recorded paper result");
        };
        assert_eq!(result.status, OrderStatus::Acknowledged);
        let client_order_id = result.client_order_id.clone();
        drop(journal);
        let journal_text = std::fs::read_to_string(&path).unwrap();
        assert!(journal_text.contains(&client_order_id));
        let records = replay_records(&path).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].source_event_refs, ["book-hash"]);
        let payload = &records[0].payload;
        assert!(payload.contains("time_in_force=Gtc"));
        assert!(payload.contains("post_only=true"));
        assert!(payload.contains("local_expires_at_ms=800"));
        assert!(payload.contains("wire_expiration_s=NONE"));
        assert!(payload.contains("feature_snapshot_id_hex=66"));
        assert!(payload.contains("book_hash_hex=626f6f6b2d68617368"));
        assert!(payload.contains(&format!("book_state_debug_hex={}", debug_hex(&book))));
        assert!(payload.contains(&format!("risk_state_debug_hex={}", debug_hex(&risk_state))));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn post_submit_journal_failure_preserves_execution_identity() {
        let path = temp_path();
        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        journal.fail_append_after_for_test(1);
        let mut router = ExecutionRouter::with_clock(PaperExecution::default(), FixedClock(110));
        let settings = Settings::default();
        let readiness = ReadinessState::default();
        let compliance = ComplianceState::default();
        let heartbeat = HeartbeatState::default();
        let recovery = RecoveryReport {
            live_unlock_allowed: false,
            reason: "paper".to_string(),
            replayed_records: 0,
            in_flight_attempts: 0,
        };
        let market = market();
        let book = book();
        let risk_state = risk_state();
        let context = checked_context(CheckedContextInputs {
            settings: &settings,
            readiness: &readiness,
            compliance: &compliance,
            heartbeat: &heartbeat,
            recovery: &recovery,
            market: &market,
            book: &book,
            risk_state: &risk_state,
        });

        let outcome = router
            .submit_checked(&mut journal, &intent(), &context)
            .await
            .unwrap();

        let CheckedSubmission::Ambiguous { result, reason } = outcome else {
            panic!("expected an undurable post-submit outcome");
        };
        let result = result.expect("the adapter result must be preserved");
        assert!(result.client_order_id.starts_with("client-"));
        assert!(result.exchange_order_id.is_some());
        assert!(reason.contains("post_submit_journal_failure"));
        assert!(journal.is_poisoned());
        assert_eq!(router.adapter().engine().orders().count(), 1);
        drop(journal);
        assert_eq!(replay_records(&path).unwrap().len(), 1);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn account_revision_change_during_submit_is_ambiguous() {
        let path = temp_path();
        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        let revision = Arc::new(AtomicU64::new(0));
        let adapter = RevisionChangingAdapter {
            revision: Arc::clone(&revision),
        };
        let mut router = ExecutionRouter::with_clock(adapter, FixedClock(110));
        let settings = Settings::default();
        let readiness = ReadinessState::default();
        let compliance = ComplianceState::default();
        let heartbeat = HeartbeatState::default();
        let recovery = RecoveryReport {
            live_unlock_allowed: false,
            reason: "paper".to_string(),
            replayed_records: 0,
            in_flight_attempts: 0,
        };
        let market = market();
        let book = book();
        let risk_state = risk_state();
        let mut context = checked_context(CheckedContextInputs {
            settings: &settings,
            readiness: &readiness,
            compliance: &compliance,
            heartbeat: &heartbeat,
            recovery: &recovery,
            market: &market,
            book: &book,
            risk_state: &risk_state,
        });
        context.account_revision = Some((&revision, 0));

        let outcome = router
            .submit_checked(&mut journal, &intent(), &context)
            .await
            .unwrap();

        let CheckedSubmission::Ambiguous { result, reason } = outcome else {
            panic!("expected an account-revision ambiguity");
        };
        assert_eq!(
            result.unwrap().exchange_order_id.as_deref(),
            Some("exchange-1")
        );
        assert!(reason.contains("account_revision_changed_during_submission"));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn client_order_id_remains_unique_across_restart_and_compaction_sequences() {
        let path = temp_path();
        let _ = std::fs::remove_file(&path);
        {
            let mut first = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
            first
                .append_write_ahead("old-decision", "client-0", "s", "accepted", "old")
                .unwrap();
        }

        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        let mut router = ExecutionRouter::with_clock(PaperExecution::default(), FixedClock(110));
        let settings = Settings::default();
        let readiness = ReadinessState::default();
        let compliance = ComplianceState::default();
        let heartbeat = HeartbeatState::default();
        let recovery = RecoveryReport {
            live_unlock_allowed: false,
            reason: "paper".to_string(),
            replayed_records: 0,
            in_flight_attempts: 0,
        };
        let market = market();
        let book = book();
        let risk_state = risk_state();
        let context = checked_context(CheckedContextInputs {
            settings: &settings,
            readiness: &readiness,
            compliance: &compliance,
            heartbeat: &heartbeat,
            recovery: &recovery,
            market: &market,
            book: &book,
            risk_state: &risk_state,
        });

        let outcome = router
            .submit_checked(&mut journal, &intent(), &context)
            .await
            .unwrap();
        let CheckedSubmission::Recorded(result) = outcome else {
            panic!("expected a durably recorded paper result");
        };
        assert!(result.client_order_id.starts_with("client-"));
        assert_ne!(result.client_order_id, "client-0");
        assert_eq!(journal.next_sequence(), 3);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn allowance_parser_rejects_non_numeric_or_zero_values() {
        assert!(!nonzero_integer("0"));
        assert!(!nonzero_integer("000"));
        assert!(!nonzero_integer("1.0"));
        assert!(nonzero_integer("00010"));
        assert!(!allowances_configured(std::iter::empty()));
        assert!(allowances_configured(["1", "2"]));
        assert!(!allowances_configured(["1", "0"]));
    }

    #[test]
    fn authoritative_order_status_mapping_and_identity_are_fail_closed() {
        assert_eq!(
            map_api_order_status(ApiOrderStatus::Live),
            OrderStatus::Acknowledged
        );
        assert_eq!(
            map_api_order_status(ApiOrderStatus::Delayed),
            OrderStatus::Acknowledged
        );
        assert_eq!(
            map_api_order_status(ApiOrderStatus::Unmatched),
            OrderStatus::Acknowledged
        );
        assert_eq!(
            map_api_order_status(ApiOrderStatus::Matched),
            OrderStatus::Filled
        );
        assert_eq!(
            map_api_order_status(ApiOrderStatus::Canceled),
            OrderStatus::Cancelled
        );
        assert_eq!(
            map_api_order_status(ApiOrderStatus::Unknown("future".to_string())),
            OrderStatus::Unknown
        );
        assert!(validate_order_status_identity("order-1", "order-1").is_ok());
        assert!(validate_order_status_identity("order-1", "order-2").is_err());
    }

    fn sdk_trade_response(
        trader_side: &str,
        owner: &str,
        maker_orders: serde_json::Value,
    ) -> TradeResponse {
        serde_json::from_value(serde_json::json!({
            "id": "trade-1",
            "taker_order_id": "taker-1",
            "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
            "asset_id": "1",
            "side": "BUY",
            "size": "2",
            "fee_rate_bps": "0",
            "price": "0.42",
            "status": "MATCHED",
            "match_time": "1705322096",
            "last_update": "1705322130",
            "outcome": "YES",
            "bucket_index": 0,
            "owner": owner,
            "maker_address": "0x2222222222222222222222222222222222222222",
            "maker_orders": maker_orders,
            "transaction_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            "trader_side": trader_side
        }))
        .unwrap()
    }

    #[test]
    fn authoritative_trade_boundary_selects_only_the_accounts_side() {
        let own_key = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
        let maker_orders = serde_json::json!([
            {
                "order_id": "maker-own",
                "owner": own_key,
                "maker_address": "0x4444444444444444444444444444444444444444",
                "matched_amount": "1.5",
                "price": "0.41",
                "fee_rate_bps": "0",
                "asset_id": "2",
                "outcome": "NO",
                "side": "SELL"
            },
            {
                "order_id": "maker-other",
                "owner": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "maker_address": "0x5555555555555555555555555555555555555555",
                "matched_amount": "0.5",
                "price": "0.41",
                "fee_rate_bps": "0",
                "asset_id": "2",
                "outcome": "NO",
                "side": "SELL"
            }
        ]);

        let taker = authoritative_trade_from_response(
            &sdk_trade_response("TAKER", &own_key.to_string(), maker_orders.clone()),
            own_key,
        )
        .unwrap();
        assert_eq!(taker.asset_id, AssetId::from("1"));
        assert_eq!(taker.side, Side::Buy);
        assert_eq!(taker.size.to_string(), "2");
        assert_eq!(taker.fee_rate_bps, Fixed::ZERO);
        assert_eq!(
            taker.order_ids,
            ["taker-1".to_string()].into_iter().collect()
        );
        assert_eq!(taker.timestamp_ms, Some(1_705_322_096_000));

        let maker = authoritative_trade_from_response(
            &sdk_trade_response(
                "MAKER",
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                maker_orders,
            ),
            own_key,
        )
        .unwrap();
        assert_eq!(maker.asset_id, AssetId::from("2"));
        assert_eq!(maker.side, Side::Sell);
        assert_eq!(maker.price.to_string(), "0.41");
        assert_eq!(maker.size.to_string(), "1.5");
        assert_eq!(maker.fee_rate_bps, Fixed::ZERO);
        assert_eq!(
            maker.order_ids,
            ["maker-own".to_string()].into_iter().collect()
        );
    }

    #[test]
    fn authoritative_trade_boundary_rejects_ambiguous_maker_identity() {
        let own_key = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
        let trade = sdk_trade_response("MAKER", &own_key.to_string(), serde_json::json!([]));

        let error = authoritative_trade_from_response(&trade, own_key).unwrap_err();

        assert!(error.to_string().contains("maker_order_cardinality"));
    }

    #[test]
    fn account_snapshot_consistency_detects_position_or_order_changes() {
        let first = LiveAccountComponents {
            collateral_balance_usdc: "100".parse().unwrap(),
            positions: vec![LivePositionSnapshot {
                asset_id: AssetId::from("a"),
                size: "2".parse().unwrap(),
                average_price: "0.4".parse().unwrap(),
                current_value_usdc: "1".parse().unwrap(),
                cash_pnl_usdc: "0.2".parse().unwrap(),
            }],
            open_orders: vec![OwnRestingOrder {
                asset_id: AssetId::from("a"),
                side: Side::Buy,
                price: "0.3".parse().unwrap(),
                size: "1".parse().unwrap(),
            }],
            open_order_ids: ["order-1".to_string()].into_iter().collect(),
            positions_complete: true,
            orders_complete: true,
        };
        let mut second = first.clone();
        second.positions[0].current_value_usdc = "1.1".parse().unwrap();
        assert!(account_components_consistent(&first, &second));

        second.positions[0].size = "3".parse().unwrap();
        assert!(!account_components_consistent(&first, &second));
        second = first.clone();
        second.open_orders.clear();
        assert!(!account_components_consistent(&first, &second));
    }

    #[tokio::test]
    async fn operation_deadline_bounds_the_entire_composed_future() {
        let error = bounded_operation("test_snapshot", Duration::from_millis(1), async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(())
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("test_snapshot:timeout"));
    }

    #[tokio::test]
    async fn execution_rechecks_risk_before_write_ahead() {
        let path = temp_path();
        let _ = std::fs::remove_file(&path);
        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        let mut router = ExecutionRouter::with_clock(PaperExecution::default(), FixedClock(110));
        let settings = Settings::default();
        let readiness = ReadinessState::default();
        let compliance = ComplianceState::default();
        let heartbeat = HeartbeatState::default();
        let recovery = RecoveryReport {
            live_unlock_allowed: false,
            reason: "paper".to_string(),
            replayed_records: 0,
            in_flight_attempts: 0,
        };
        let market = market();
        let book = book();
        let risk_state = RiskState {
            own_order_cache_certain: false,
            ..risk_state()
        };
        let context = checked_context(CheckedContextInputs {
            settings: &settings,
            readiness: &readiness,
            compliance: &compliance,
            heartbeat: &heartbeat,
            recovery: &recovery,
            market: &market,
            book: &book,
            risk_state: &risk_state,
        });
        let err = router
            .submit_checked(&mut journal, &intent(), &context)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("own_order_cache_uncertain"));
        assert!(std::fs::read_to_string(&path).unwrap().is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn execution_rechecks_intent_expiry_at_submission_time() {
        let path = temp_path();
        let _ = std::fs::remove_file(&path);
        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        let mut router =
            ExecutionRouter::with_clock(PaperExecution::default(), SequenceClock::new([110, 800]));
        let settings = Settings {
            max_book_age_ms: 1_000,
            ..Settings::default()
        };
        let readiness = ReadinessState::default();
        let compliance = ComplianceState::default();
        let heartbeat = HeartbeatState::default();
        let recovery = RecoveryReport {
            live_unlock_allowed: false,
            reason: "paper".to_string(),
            replayed_records: 0,
            in_flight_attempts: 0,
        };
        let market = market();
        let book = book();
        let risk_state = risk_state();
        let context = checked_context(CheckedContextInputs {
            settings: &settings,
            readiness: &readiness,
            compliance: &compliance,
            heartbeat: &heartbeat,
            recovery: &recovery,
            market: &market,
            book: &book,
            risk_state: &risk_state,
        });
        let expiring_intent = intent();

        let err = router
            .submit_checked(&mut journal, &expiring_intent, &context)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("expired_intent"));
        let records = journal.records_locked().unwrap();
        assert_eq!(records.len(), 2);
        assert!(records[0].client_order_id.starts_with("client-"));
        assert_eq!(records[0].client_order_id, records[1].client_order_id);
        assert_eq!(router.adapter.engine().orders().count(), 0);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn ambiguous_live_transport_failure_remains_nonterminal() {
        let path = temp_path();
        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        let mut router = ExecutionRouter::with_clock(AmbiguousLiveAdapter, FixedClock(110));
        let settings = Settings {
            mode: BotMode::Live,
            ..Settings::default()
        };
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
        let compliance = ComplianceState::from_geoblock_result(false, "CA", "BC", true, 100);
        let heartbeat = HeartbeatState {
            live_enabled: true,
            consecutive_failures: 0,
            max_failures: 3,
            degraded: false,
        };
        let recovery = RecoveryReport {
            live_unlock_allowed: true,
            reason: "reconciled".to_string(),
            replayed_records: 0,
            in_flight_attempts: 0,
        };
        let market = market();
        let book = book();
        let risk_state = risk_state();
        let context = checked_context(CheckedContextInputs {
            settings: &settings,
            readiness: &readiness,
            compliance: &compliance,
            heartbeat: &heartbeat,
            recovery: &recovery,
            market: &market,
            book: &book,
            risk_state: &risk_state,
        });

        let outcome = router
            .submit_checked(&mut journal, &intent(), &context)
            .await
            .unwrap();
        let CheckedSubmission::Ambiguous { reason, result } = outcome else {
            panic!("expected an ambiguous live submission");
        };
        assert!(result.is_none());
        assert!(reason.contains("live_submission_state_unknown"));
        drop(journal);
        let records = replay_records(&path).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].event_kind, JournalEventKind::Submitted);
        assert!(!records[1].is_terminal());
        let _ = std::fs::remove_file(path);
    }
}
