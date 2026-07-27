use crate::error::{BotError, Result};
use crate::fixed::Fixed;
use crate::types::{AssetId, ConditionId, Side};
use futures_util::StreamExt as _;
use polymarket_client_sdk_v2::auth::Credentials;
use polymarket_client_sdk_v2::clob::types::{Side as ApiSide, TraderSide as ApiTraderSide};
use polymarket_client_sdk_v2::clob::ws::types::response::OrderMessageType as ApiOrderMessageType;
use polymarket_client_sdk_v2::clob::ws::{
    ChannelType, Client, OrderMessage, TradeMessage, WsMessage,
};
use polymarket_client_sdk_v2::types::{Address, B256};
use polymarket_client_sdk_v2::ws::config::Config as WsConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::str::FromStr as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const USER_WS_ENDPOINT: &str = "wss://ws-subscriptions-clob.polymarket.com";
pub const PING_INTERVAL_MS: u64 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserSubscription {
    pub condition_ids: Vec<ConditionId>,
    pub api_key_present: bool,
    pub secret_present: bool,
    pub passphrase_present: bool,
}

impl UserSubscription {
    pub fn new(
        condition_ids: Vec<ConditionId>,
        api_key: &str,
        secret: &str,
        passphrase: &str,
    ) -> Self {
        Self {
            condition_ids,
            api_key_present: !api_key.is_empty(),
            secret_present: !secret.is_empty(),
            passphrase_present: !passphrase.is_empty(),
        }
    }

    pub fn is_authenticated(&self) -> bool {
        self.api_key_present && self.secret_present && self.passphrase_present
    }
}

#[derive(Clone)]
pub struct UserWsAuth {
    api_key: Uuid,
    api_secret: String,
    api_passphrase: String,
    address: Address,
}

impl UserWsAuth {
    pub fn new(
        api_key: &str,
        api_secret: String,
        api_passphrase: String,
        address: &str,
    ) -> Result<Self> {
        if api_secret.trim().is_empty() || api_passphrase.trim().is_empty() {
            return Err(BotError::Config("user_ws_credentials_missing".to_string()));
        }
        Ok(Self {
            api_key: Uuid::parse_str(api_key)
                .map_err(|_| BotError::Config("user_ws_api_key_invalid".to_string()))?,
            api_secret,
            api_passphrase,
            address: Address::from_str(address)
                .map_err(|_| BotError::Config("user_ws_address_invalid".to_string()))?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum UserEvent {
    Order {
        condition_id: ConditionId,
        order_id: String,
        asset_id: AssetId,
        side: Side,
        price: Fixed,
        original_size: Option<Fixed>,
        size_matched: Option<Fixed>,
        status: String,
        timestamp_ms: Option<u64>,
    },
    Trade {
        condition_id: ConditionId,
        trade_id: String,
        asset_id: AssetId,
        side: Side,
        price: Fixed,
        size: Fixed,
        status: String,
        order_ids: Vec<String>,
        timestamp_ms: Option<u64>,
    },
    Disconnect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserFeedMessage {
    Connected { revision: u64 },
    Event { revision: u64, event: UserEvent },
    Disconnected { revision: u64, error: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OwnOrderCacheState {
    pub certain: bool,
    pub snapshot_revision: u64,
    pub open_order_ids: BTreeSet<String>,
}

impl OwnOrderCacheState {
    pub fn apply_authoritative_snapshot(
        &mut self,
        open_order_ids: impl IntoIterator<Item = String>,
    ) {
        self.open_order_ids = open_order_ids.into_iter().collect();
        self.snapshot_revision = self.snapshot_revision.saturating_add(1);
        self.certain = true;
    }
}

pub fn apply_user_event(state: &mut OwnOrderCacheState, event: &UserEvent) {
    match event {
        UserEvent::Order {
            order_id, status, ..
        } => {
            if order_status_is_terminal(status) {
                state.open_order_ids.remove(order_id);
            } else if state.certain {
                state.open_order_ids.insert(order_id.clone());
            }
        }
        UserEvent::Trade { .. } => {
            // A trade does not prove the complete open-order set. The next order
            // update or REST reconciliation is authoritative for order status.
        }
        UserEvent::Disconnect => state.certain = false,
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the feed task keeps endpoint, auth, revision clock, channel, reconnect, and shutdown ownership explicit"
)]
pub async fn run_user_feed(
    endpoint: String,
    condition_ids: Vec<ConditionId>,
    auth: UserWsAuth,
    reconnect_min_ms: u64,
    reconnect_max_ms: u64,
    account_revision: Arc<AtomicU64>,
    sender: mpsc::Sender<UserFeedMessage>,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut backoff_ms = reconnect_min_ms.max(1);
    while !shutdown.is_cancelled() {
        let mut connected_once = false;
        let result = run_user_session(
            &endpoint,
            &condition_ids,
            auth.clone(),
            &account_revision,
            &sender,
            &shutdown,
            &mut connected_once,
        )
        .await;
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let detail = result
            .err()
            .map(|error| error.to_string())
            .unwrap_or_else(|| "user_ws_ended".to_string());
        let revision = account_revision
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        sender
            .send(UserFeedMessage::Disconnected {
                revision,
                error: detail,
            })
            .await
            .map_err(|_| BotError::Execution("user_feed_receiver_closed".to_string()))?;
        if connected_once {
            backoff_ms = reconnect_min_ms.max(1);
        }
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            () = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
        }
        backoff_ms = backoff_ms.saturating_mul(2).min(reconnect_max_ms.max(1));
    }
    Ok(())
}

async fn run_user_session(
    endpoint: &str,
    condition_ids: &[ConditionId],
    auth: UserWsAuth,
    account_revision: &Arc<AtomicU64>,
    sender: &mpsc::Sender<UserFeedMessage>,
    shutdown: &CancellationToken,
    connected_once: &mut bool,
) -> Result<()> {
    let markets = condition_ids
        .iter()
        .map(|condition| {
            B256::from_str(condition.as_ref())
                .map_err(|_| BotError::Config(format!("invalid_condition_id:{condition}")))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut config = WsConfig::default();
    config.heartbeat_interval = Duration::from_millis(PING_INTERVAL_MS);
    let api_key = auth.api_key;
    let credentials = Credentials::new(api_key, auth.api_secret, auth.api_passphrase);
    let client = Client::new(endpoint, config)
        .map_err(|error| BotError::Protocol(format!("user_ws_client:{error}")))?
        .authenticate(credentials, auth.address)
        .map_err(|error| BotError::Protocol(format!("user_ws_auth:{error}")))?;
    let mut stream = Box::pin(
        client
            .subscribe_user_events(markets)
            .map_err(|error| BotError::Protocol(format!("user_ws_subscribe:{error}")))?,
    );
    let mut connected = false;
    let mut connection_check = tokio::time::interval(Duration::from_millis(100));
    connection_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            _ = connection_check.tick(), if !connected => {
                if client.connection_state(ChannelType::User).is_connected() {
                    let revision = account_revision
                        .fetch_add(1, Ordering::AcqRel)
                        .saturating_add(1);
                    sender.send(UserFeedMessage::Connected { revision }).await.map_err(|_| {
                        BotError::Execution("user_feed_receiver_closed".to_string())
                    })?;
                    connected = true;
                    *connected_once = true;
                }
            }
            value = stream.next() => {
                let value = value.ok_or_else(|| BotError::Protocol("user_ws_eof".to_string()))?
                    .map_err(|error| BotError::Protocol(format!("user_ws_stream:{error}")))?;
                let Some(event) = convert_message(value, api_key)? else {
                    continue;
                };
                if !connected {
                    let revision = account_revision
                        .fetch_add(1, Ordering::AcqRel)
                        .saturating_add(1);
                    sender.send(UserFeedMessage::Connected { revision }).await.map_err(|_| {
                        BotError::Execution("user_feed_receiver_closed".to_string())
                    })?;
                    connected = true;
                    *connected_once = true;
                }
                let revision = if user_event_changes_account_revision(&event) {
                    account_revision
                        .fetch_add(1, Ordering::AcqRel)
                        .saturating_add(1)
                } else {
                    account_revision.load(Ordering::Acquire)
                };
                sender.send(UserFeedMessage::Event { revision, event }).await.map_err(|_| {
                    BotError::Execution("user_feed_receiver_closed".to_string())
                })?;
            }
        }
    }
}

pub(crate) fn user_event_changes_account_revision(event: &UserEvent) -> bool {
    match event {
        UserEvent::Trade { .. } | UserEvent::Disconnect => true,
        UserEvent::Order {
            status,
            size_matched,
            ..
        } => {
            order_status_is_terminal(status) || size_matched.is_some_and(|size| size > Fixed::ZERO)
        }
    }
}

pub(crate) fn user_event_requires_holdings_reconciliation(event: &UserEvent) -> bool {
    match event {
        UserEvent::Trade { .. } => true,
        UserEvent::Order {
            status,
            size_matched,
            ..
        } => {
            status.eq_ignore_ascii_case("matched")
                || size_matched.is_some_and(|size| size > Fixed::ZERO)
        }
        UserEvent::Disconnect => false,
    }
}

fn convert_message(message: WsMessage, api_key: Uuid) -> Result<Option<UserEvent>> {
    match message {
        WsMessage::Order(value) => convert_order(value, api_key).map(Some),
        WsMessage::Trade(value) => convert_trade(value, api_key).map(Some),
        _ => Ok(None),
    }
}

fn convert_order(value: OrderMessage, api_key: Uuid) -> Result<UserEvent> {
    if value.owner != Some(api_key) && value.order_owner != Some(api_key) {
        return Err(BotError::Protocol(format!(
            "user_order_owner_not_attributable:{}",
            value.id
        )));
    }
    let status = value.status.as_ref().map_or_else(
        || match value.msg_type.as_ref() {
            Some(ApiOrderMessageType::Placement) => "Placement".to_string(),
            Some(ApiOrderMessageType::Update)
                if value.original_size.is_some() && value.original_size == value.size_matched =>
            {
                "Matched".to_string()
            }
            Some(ApiOrderMessageType::Update) if value.size_matched.is_some() => {
                "PartiallyFilled".to_string()
            }
            Some(ApiOrderMessageType::Update) => "Update".to_string(),
            Some(ApiOrderMessageType::Cancellation) => "Cancelled".to_string(),
            Some(ApiOrderMessageType::Unknown(kind)) => format!("Unknown({kind})"),
            Some(_) | None => "unknown".to_string(),
        },
        |status| format!("{status:?}"),
    );
    Ok(UserEvent::Order {
        condition_id: ConditionId::from(value.market.to_string()),
        order_id: value.id,
        asset_id: AssetId::from(value.asset_id.to_string()),
        side: convert_side(value.side)?,
        price: value.price.to_string().parse()?,
        original_size: value
            .original_size
            .map(|size| size.to_string().parse())
            .transpose()?,
        size_matched: value
            .size_matched
            .map(|size| size.to_string().parse())
            .transpose()?,
        status,
        timestamp_ms: optional_timestamp(value.timestamp)?,
    })
}

fn convert_trade(value: TradeMessage, api_key: Uuid) -> Result<UserEvent> {
    let own_maker_orders = value
        .maker_orders
        .iter()
        .filter(|order| order.owner == api_key)
        .collect::<Vec<_>>();
    let account_is_maker = match value.trader_side.as_ref() {
        Some(ApiTraderSide::Maker) => true,
        Some(ApiTraderSide::Taker) => false,
        Some(ApiTraderSide::Unknown(side)) => {
            return Err(BotError::Protocol(format!(
                "unsupported_user_trader_side:{}:{side}",
                value.id
            )));
        }
        Some(_) => {
            return Err(BotError::Protocol(format!(
                "unsupported_user_trader_side:{}",
                value.id
            )));
        }
        None if own_maker_orders.len() == 1 => true,
        None if own_maker_orders.is_empty()
            && (value.owner == Some(api_key) || value.trade_owner == Some(api_key)) =>
        {
            false
        }
        None => {
            return Err(BotError::Protocol(format!(
                "user_trade_side_not_attributable:{}",
                value.id
            )));
        }
    };
    let (asset_id, side, price, size, order_ids) = if account_is_maker {
        let [maker] = own_maker_orders.as_slice() else {
            return Err(BotError::Protocol(format!(
                "user_maker_trade_order_cardinality:{}:{}",
                value.id,
                own_maker_orders.len()
            )));
        };
        let side = match convert_side(value.side)? {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        (
            AssetId::from(maker.asset_id.to_string()),
            side,
            maker.price.to_string().parse()?,
            maker.matched_amount.to_string().parse()?,
            vec![maker.order_id.clone()],
        )
    } else {
        if !own_maker_orders.is_empty() {
            return Err(BotError::Protocol(format!(
                "user_taker_trade_contains_owned_maker:{}",
                value.id
            )));
        }
        {
            let order_id = value
                .taker_order_id
                .clone()
                .filter(|id| !id.trim().is_empty())
                .ok_or_else(|| {
                    BotError::Protocol("user_taker_trade_order_id_missing".to_string())
                })?;
            (
                AssetId::from(value.asset_id.to_string()),
                convert_side(value.side)?,
                value.price.to_string().parse()?,
                value.size.to_string().parse()?,
                vec![order_id],
            )
        }
    };
    if size <= Fixed::ZERO || price <= Fixed::ZERO || price >= Fixed::ONE {
        return Err(BotError::Protocol(format!(
            "user_trade_terms_invalid:{}",
            value.id
        )));
    }
    Ok(UserEvent::Trade {
        condition_id: ConditionId::from(value.market.to_string()),
        trade_id: value.id,
        asset_id,
        side,
        price,
        size,
        status: format!("{:?}", value.status),
        order_ids,
        timestamp_ms: optional_timestamp(value.timestamp.or(value.matchtime))?,
    })
}

fn convert_side(value: ApiSide) -> Result<Side> {
    match value {
        ApiSide::Buy => Ok(Side::Buy),
        ApiSide::Sell => Ok(Side::Sell),
        _ => Err(BotError::Protocol(
            "unsupported_user_event_side".to_string(),
        )),
    }
}

fn optional_timestamp(value: Option<i64>) -> Result<Option<u64>> {
    value
        .map(|timestamp| {
            let timestamp = u64::try_from(timestamp)
                .map_err(|_| BotError::Protocol("negative_user_event_timestamp".to_string()))?;
            if timestamp < 10_000_000_000 {
                timestamp
                    .checked_mul(1_000)
                    .ok_or_else(|| BotError::Protocol("user_event_timestamp_overflow".to_string()))
            } else {
                Ok(timestamp)
            }
        })
        .transpose()
}

pub(crate) fn order_status_is_terminal(status: &str) -> bool {
    let status = status.to_ascii_lowercase();
    matches!(status.as_str(), "matched" | "cancelled" | "canceled")
}

pub fn user_subscription_payload_redacted(subscription: &UserSubscription) -> String {
    let markets = subscription
        .condition_ids
        .iter()
        .map(|id| format!("\"{}\"", id.as_ref()))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"type\":\"user\",\"markets\":[{markets}],\"auth\":{{\"apiKey\":\"<redacted>\",\"secret\":\"<redacted>\",\"passphrase\":\"<redacted>\"}}}}"
    )
}

pub fn user_subscription_payload_live(_subscription: &UserSubscription) -> Result<String> {
    Err(BotError::Protocol(
        "manual_user_ws_payload_forbidden_use_typed_sdk".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_event_does_not_make_cache_authoritative() {
        let mut state = OwnOrderCacheState::default();
        let event = UserEvent::Order {
            condition_id: ConditionId::from("c"),
            order_id: "o1".to_string(),
            asset_id: AssetId::from("1"),
            side: Side::Buy,
            price: "0.5".parse().unwrap(),
            original_size: Some("1".parse().unwrap()),
            size_matched: Some(Fixed::ZERO),
            status: "Open".to_string(),
            timestamp_ms: Some(1),
        };
        apply_user_event(&mut state, &event);
        assert!(!state.certain);
        assert!(state.open_order_ids.is_empty());
        state.apply_authoritative_snapshot(["existing".to_string()]);
        apply_user_event(&mut state, &event);
        assert!(state.open_order_ids.contains("o1"));
        apply_user_event(&mut state, &UserEvent::Disconnect);
        assert!(!state.certain);
    }

    #[test]
    fn user_subscription_is_redacted() {
        let subscription = UserSubscription::new(
            vec![ConditionId::from("condition")],
            "actual-key-value",
            "actual-secret-value",
            "actual-passphrase-value",
        );
        assert!(subscription.is_authenticated());
        let redacted = user_subscription_payload_redacted(&subscription);
        assert!(!redacted.contains("actual-secret-value"));
        assert!(!redacted.contains("actual-passphrase-value"));
        assert!(redacted.contains("<redacted>"));
    }

    #[test]
    fn maker_trade_conversion_uses_only_the_authenticated_accounts_clip() {
        let api_key = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
        let message: WsMessage = serde_json::from_value(serde_json::json!({
            "event_type": "trade",
            "id": "trade-1",
            "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
            "asset_id": "1",
            "side": "BUY",
            "size": "2",
            "price": "0.42",
            "status": "MATCHED",
            "maker_orders": [
                {
                    "asset_id": "2",
                    "matched_amount": "1.5",
                    "order_id": "maker-own",
                    "outcome": "NO",
                    "owner": api_key,
                    "price": "0.41"
                },
                {
                    "asset_id": "2",
                    "matched_amount": "0.5",
                    "order_id": "maker-other",
                    "outcome": "NO",
                    "owner": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                    "price": "0.41"
                }
            ]
        }))
        .unwrap();

        let event = convert_message(message, api_key).unwrap().unwrap();

        let UserEvent::Trade {
            asset_id,
            side,
            price,
            size,
            order_ids,
            ..
        } = event
        else {
            panic!("expected trade event")
        };
        assert_eq!(asset_id, AssetId::from("2"));
        assert_eq!(side, Side::Sell);
        assert_eq!(price.to_string(), "0.41");
        assert_eq!(size.to_string(), "1.5");
        assert_eq!(order_ids, vec!["maker-own".to_string()]);
    }

    #[test]
    fn order_type_fallback_recovers_cancellation_and_normalizes_seconds() {
        let api_key = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
        let message: WsMessage = serde_json::from_value(serde_json::json!({
            "event_type": "order",
            "id": "order-1",
            "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
            "asset_id": "1",
            "side": "SELL",
            "price": "0.57",
            "type": "CANCELLATION",
            "owner": api_key,
            "order_owner": api_key,
            "original_size": "10",
            "size_matched": "0",
            "timestamp": "1672290687"
        }))
        .unwrap();

        let event = convert_message(message, api_key).unwrap().unwrap();

        let UserEvent::Order {
            status,
            timestamp_ms,
            ..
        } = event
        else {
            panic!("expected order event")
        };
        assert_eq!(status, "Cancelled");
        assert_eq!(timestamp_ms, Some(1_672_290_687_000));
        assert!(user_event_changes_account_revision(&UserEvent::Order {
            condition_id: ConditionId::from("condition"),
            order_id: "order-1".to_string(),
            asset_id: AssetId::from("1"),
            side: Side::Sell,
            price: "0.57".parse().unwrap(),
            original_size: Some("10".parse().unwrap()),
            size_matched: Some(Fixed::ZERO),
            status,
            timestamp_ms,
        }));
    }

    #[test]
    fn user_timestamp_normalization_preserves_milliseconds_and_rejects_negative_values() {
        assert_eq!(
            optional_timestamp(Some(1_782_750_000_000)).unwrap(),
            Some(1_782_750_000_000)
        );
        assert!(optional_timestamp(Some(-1)).is_err());
    }

    #[test]
    fn only_final_order_statuses_are_terminal() {
        assert!(order_status_is_terminal("Matched"));
        assert!(order_status_is_terminal("Cancelled"));
        assert!(order_status_is_terminal("CANCELED"));
        assert!(!order_status_is_terminal("Cancellation"));
        assert!(!order_status_is_terminal("PartiallyFilled"));
        assert!(!order_status_is_terminal("Unknown"));
    }

    #[test]
    fn only_account_affecting_order_updates_advance_the_revision() {
        let open = UserEvent::Order {
            condition_id: ConditionId::from("c"),
            order_id: "o1".to_string(),
            asset_id: AssetId::from("1"),
            side: Side::Buy,
            price: "0.5".parse().unwrap(),
            original_size: Some("1".parse().unwrap()),
            size_matched: Some(Fixed::ZERO),
            status: "Open".to_string(),
            timestamp_ms: Some(1),
        };
        assert!(!user_event_changes_account_revision(&open));

        let partially_filled = UserEvent::Order {
            condition_id: ConditionId::from("c"),
            order_id: "o1".to_string(),
            asset_id: AssetId::from("1"),
            side: Side::Buy,
            price: "0.5".parse().unwrap(),
            original_size: Some("1".parse().unwrap()),
            size_matched: Some("0.1".parse().unwrap()),
            status: "Open".to_string(),
            timestamp_ms: Some(2),
        };
        assert!(user_event_changes_account_revision(&partially_filled));
    }

    #[test]
    fn unfilled_cancellation_does_not_create_a_holdings_reconciliation_lock() {
        let canceled = UserEvent::Order {
            condition_id: ConditionId::from("c"),
            order_id: "o1".to_string(),
            asset_id: AssetId::from("1"),
            side: Side::Buy,
            price: "0.5".parse().unwrap(),
            original_size: Some("1".parse().unwrap()),
            size_matched: Some(Fixed::ZERO),
            status: "Cancelled".to_string(),
            timestamp_ms: Some(1),
        };

        assert!(user_event_changes_account_revision(&canceled));
        assert!(!user_event_requires_holdings_reconciliation(&canceled));

        let partially_filled_cancellation = UserEvent::Order {
            condition_id: ConditionId::from("c"),
            order_id: "o1".to_string(),
            asset_id: AssetId::from("1"),
            side: Side::Buy,
            price: "0.5".parse().unwrap(),
            original_size: Some("1".parse().unwrap()),
            size_matched: Some("0.1".parse().unwrap()),
            status: "Cancelled".to_string(),
            timestamp_ms: Some(2),
        };
        assert!(user_event_requires_holdings_reconciliation(
            &partially_filled_cancellation
        ));
    }
}
