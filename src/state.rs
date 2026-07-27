use crate::execution::LiveRiskSnapshot;
use crate::external_feeds::ExternalQuote;
use crate::fixed::Fixed;
use crate::latency::LatencySummary;
use crate::paper::{PaperFill, PaperPortfolioSnapshot};
use crate::types::{AssetId, BookState, BotMode, ConditionId, Side};
use crate::wallet_watch::WalletTradeObservation;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, RwLock};

pub const MAX_RECENT_EVENTS: usize = 200;
pub const MAX_RECENT_FILLS: usize = 100;
pub const MAX_RECENT_WALLET_TRADES: usize = 100;

pub type SharedRuntimeState = Arc<RwLock<RuntimeState>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimePhase {
    Starting,
    Running,
    Paused,
    Degraded,
    ShuttingDown,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionStatus {
    Disabled,
    Connecting,
    Connected,
    Degraded,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedHealth {
    pub status: ConnectionStatus,
    pub last_message_at_ms: Option<u64>,
    pub last_error: Option<String>,
    pub reconnects: u64,
}

impl FeedHealth {
    pub const fn connecting() -> Self {
        Self {
            status: ConnectionStatus::Connecting,
            last_message_at_ms: None,
            last_error: None,
            reconnects: 0,
        }
    }

    pub const fn disabled() -> Self {
        Self {
            status: ConnectionStatus::Disabled,
            last_message_at_ms: None,
            last_error: None,
            reconnects: 0,
        }
    }

    pub fn connected(&mut self, now_ms: u64) {
        self.status = ConnectionStatus::Connected;
        self.last_message_at_ms = Some(now_ms);
        self.last_error = None;
    }

    pub fn message(&mut self, now_ms: u64) {
        self.status = ConnectionStatus::Connected;
        self.last_message_at_ms = Some(now_ms);
    }

    pub fn degraded(&mut self, error: impl Into<String>) {
        self.status = ConnectionStatus::Degraded;
        self.last_error = Some(error.into());
        self.reconnects = self.reconnects.saturating_add(1);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketRuntimeView {
    pub condition_id: ConditionId,
    pub question: String,
    pub slug: String,
    pub book: BookState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeEventLevel {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEvent {
    pub timestamp_ms: u64,
    pub level: RuntimeEventLevel,
    pub component: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeFill {
    pub fill_id: String,
    pub source: String,
    pub asset_id: AssetId,
    pub side: Side,
    pub price: Fixed,
    pub size: Fixed,
    pub filled_at_ms: u64,
    pub terminal: bool,
}

impl RuntimeFill {
    pub fn from_paper(fill: &PaperFill) -> Self {
        Self {
            fill_id: format!(
                "{}:{}:{}",
                fill.client_order_id,
                fill.filled_at_ms,
                fill.size.raw()
            ),
            source: "paper_matching".to_string(),
            asset_id: fill.asset_id.clone(),
            side: fill.side,
            price: fill.price,
            size: fill.size,
            filled_at_ms: fill.filled_at_ms,
            terminal: fill.terminal,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeState {
    pub started_at_ms: u64,
    pub updated_at_ms: u64,
    pub revision: u64,
    pub mode: BotMode,
    pub phase: RuntimePhase,
    pub paused: bool,
    pub live_submission_enabled: bool,
    pub strategy_enabled: bool,
    pub readiness: BTreeMap<String, bool>,
    pub market_feed: FeedHealth,
    pub user_feed: FeedHealth,
    pub external_feed: FeedHealth,
    pub wallet_feed: FeedHealth,
    pub compliance_status: String,
    pub compliance_location: String,
    pub catalog_ready: bool,
    pub markets: BTreeMap<AssetId, MarketRuntimeView>,
    pub portfolio: Option<PaperPortfolioSnapshot>,
    pub live_account: Option<LiveRiskSnapshot>,
    pub external_quotes: BTreeMap<String, ExternalQuote>,
    pub recent_wallet_trades: VecDeque<WalletTradeObservation>,
    pub recent_fills: VecDeque<RuntimeFill>,
    pub latency: BTreeMap<String, LatencySummary>,
    pub journal_next_sequence: u64,
    pub journal_last_hash: String,
    pub rejected_intents: u64,
    pub submitted_orders: u64,
    pub recent_events: VecDeque<RuntimeEvent>,
}

impl RuntimeState {
    pub fn new(
        mode: BotMode,
        started_at_ms: u64,
        external_enabled: bool,
        wallet_enabled: bool,
    ) -> Self {
        Self {
            started_at_ms,
            updated_at_ms: started_at_ms,
            revision: 0,
            mode,
            phase: RuntimePhase::Starting,
            paused: true,
            live_submission_enabled: false,
            strategy_enabled: false,
            readiness: BTreeMap::new(),
            market_feed: FeedHealth::connecting(),
            user_feed: if mode == BotMode::Live {
                FeedHealth::connecting()
            } else {
                FeedHealth::disabled()
            },
            external_feed: if external_enabled {
                FeedHealth::connecting()
            } else {
                FeedHealth::disabled()
            },
            wallet_feed: if wallet_enabled {
                FeedHealth::connecting()
            } else {
                FeedHealth::disabled()
            },
            compliance_status: "unverified".to_string(),
            compliance_location: "Location unavailable".to_string(),
            catalog_ready: false,
            markets: BTreeMap::new(),
            portfolio: None,
            live_account: None,
            external_quotes: BTreeMap::new(),
            recent_wallet_trades: VecDeque::new(),
            recent_fills: VecDeque::new(),
            latency: BTreeMap::new(),
            journal_next_sequence: 0,
            journal_last_hash: String::new(),
            rejected_intents: 0,
            submitted_orders: 0,
            recent_events: VecDeque::new(),
        }
    }

    pub fn touch(&mut self, now_ms: u64) {
        self.updated_at_ms = now_ms;
        self.revision = self.revision.saturating_add(1);
    }

    pub fn push_event(
        &mut self,
        now_ms: u64,
        level: RuntimeEventLevel,
        component: impl Into<String>,
        message: impl Into<String>,
    ) {
        self.recent_events.push_front(RuntimeEvent {
            timestamp_ms: now_ms,
            level,
            component: component.into(),
            message: message.into(),
        });
        self.recent_events.truncate(MAX_RECENT_EVENTS);
        self.touch(now_ms);
    }

    pub fn push_fill(&mut self, fill: RuntimeFill) {
        self.recent_fills.push_front(fill);
        self.recent_fills.truncate(MAX_RECENT_FILLS);
    }

    pub fn push_live_fill(&mut self, fill: RuntimeFill) {
        if self
            .recent_fills
            .iter()
            .any(|existing| existing.source == "live_user_ws" && existing.fill_id == fill.fill_id)
        {
            return;
        }
        self.push_fill(fill);
    }

    pub fn push_wallet_trade(&mut self, trade: WalletTradeObservation) {
        self.recent_wallet_trades.push_front(trade);
        self.recent_wallet_trades.truncate(MAX_RECENT_WALLET_TRADES);
    }
}

pub fn shared_runtime_state(state: RuntimeState) -> SharedRuntimeState {
    Arc::new(RwLock::new(state))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_history_is_bounded() {
        let mut state = RuntimeState::new(BotMode::Paper, 1, false, false);
        for value in 0..(MAX_RECENT_EVENTS + 5) {
            state.push_event(
                u64::try_from(value).unwrap(),
                RuntimeEventLevel::Info,
                "test",
                value.to_string(),
            );
        }
        assert_eq!(state.recent_events.len(), MAX_RECENT_EVENTS);
        assert_eq!(state.recent_events.front().unwrap().message, "204");
    }
}
