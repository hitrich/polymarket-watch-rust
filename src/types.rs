use crate::fixed::Fixed;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AssetId(pub String);

impl From<&str> for AssetId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for AssetId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl AsRef<str> for AssetId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Display for AssetId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConditionId(pub String);

impl From<&str> for ConditionId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for ConditionId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl AsRef<str> for ConditionId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Display for ConditionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BotMode {
    Paper,
    Live,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClobProtocol {
    V1,
    V2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalletMode {
    DepositWallet,
    GnosisSafe,
    Proxy,
    Eoa,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignatureType {
    Poly1271,
    GnosisSafe,
    Proxy,
    Eoa,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    Gtc,
    Gtd,
    Fok,
    Fak,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Level {
    pub price: Fixed,
    pub size: Fixed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketMeta {
    pub condition_id: ConditionId,
    pub asset_id_yes: AssetId,
    pub asset_id_no: AssetId,
    pub tick_size: Fixed,
    pub min_order_size: Fixed,
    pub neg_risk: bool,
    pub active: bool,
    pub accepting_orders: bool,
    pub resolved: bool,
    pub paused: bool,
    pub taker_delay_enabled: bool,
    pub fees_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookState {
    pub asset_id: AssetId,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub best_bid: Option<Fixed>,
    pub best_ask: Option<Fixed>,
    pub last_trade_price: Option<Fixed>,
    #[serde(default)]
    pub last_trade_timestamp_ms: Option<u64>,
    pub tick_size: Fixed,
    pub min_order_size: Fixed,
    pub book_hash: Option<String>,
    pub exchange_timestamp_ms: u64,
    pub local_received_at_ms: u64,
    pub tradeable: bool,
}

impl BookState {
    pub fn empty(asset_id: impl Into<AssetId>, tick_size: Fixed, min_order_size: Fixed) -> Self {
        Self {
            asset_id: asset_id.into(),
            bids: Vec::new(),
            asks: Vec::new(),
            best_bid: None,
            best_ask: None,
            last_trade_price: None,
            last_trade_timestamp_ms: None,
            tick_size,
            min_order_size,
            book_hash: None,
            exchange_timestamp_ms: 0,
            local_received_at_ms: 0,
            tradeable: false,
        }
    }

    pub fn spread(&self) -> Option<Fixed> {
        self.best_ask?.checked_sub(self.best_bid?).ok()
    }

    pub fn age_ms(&self, now_ms: u64) -> Option<u64> {
        now_ms.checked_sub(self.local_received_at_ms)
    }

    pub fn event_lag_ms(&self) -> Option<u64> {
        self.local_received_at_ms
            .checked_sub(self.exchange_timestamp_ms)
    }

    pub fn top_ask_size(&self) -> Fixed {
        self.asks.first().map(|v| v.size).unwrap_or(Fixed::ZERO)
    }

    pub fn top_bid_size(&self) -> Fixed {
        self.bids.first().map(|v| v.size).unwrap_or(Fixed::ZERO)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderIntent {
    pub asset_id: AssetId,
    pub side: Side,
    pub limit_price: Fixed,
    pub size: Fixed,
    pub time_in_force: TimeInForce,
    pub post_only: bool,
    pub local_expires_at_ms: u64,
    pub wire_expiration_s: Option<u64>,
    pub reason: String,
    pub strategy_id: String,
    pub feature_snapshot_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Intent,
    WriteAhead,
    Submitted,
    Acknowledged,
    PartiallyFilled,
    Filled,
    CancelRequested,
    Cancelled,
    Rejected,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnRestingOrder {
    pub asset_id: AssetId,
    pub side: Side,
    pub price: Fixed,
    pub size: Fixed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderState {
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub intent: OrderIntent,
    pub status: OrderStatus,
    pub submitted_at_ms: Option<u64>,
    pub acknowledged_at_ms: Option<u64>,
    pub matched_at_ms: Option<u64>,
    pub settled_at_ms: Option<u64>,
    pub cancel_requested_at_ms: Option<u64>,
    pub terminal_reason: Option<String>,
}
