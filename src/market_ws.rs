use crate::book::{apply_level_change, sort_and_refresh_top};
use crate::error::{BotError, Result};
use crate::fixed::Fixed;
use crate::types::{AssetId, BookState, ConditionId, Level, Side};
use futures_util::{SinkExt as _, StreamExt as _};
use polymarket_client_sdk_v2::clob::types::Side as ApiSide;
use polymarket_client_sdk_v2::clob::ws::{
    BestBidAsk as ApiBestBidAsk, BookUpdate, LastTradePrice as ApiLastTradePrice,
    MarketResolved as ApiMarketResolved, PriceChange as ApiPriceChange,
    TickSizeChange as ApiTickSizeChange, WsMessage,
};
use polymarket_client_sdk_v2::types::U256;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeSet, VecDeque};
use std::str::FromStr as _;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, sleep, Duration, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message, WebSocketStream};
use tokio_util::sync::CancellationToken;

pub const MARKET_WS_ENDPOINT: &str = "wss://ws-subscriptions-clob.polymarket.com";
pub const PING_INTERVAL_MS: u64 = 10_000;
const MAX_LEVELS_PER_SIDE: usize = 20_000;
const MAX_PENDING_MARKET_BATCHES: usize = 64;
const MAX_PENDING_MARKET_EVENTS: usize = 65_536;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketSubscription {
    pub asset_ids: Vec<AssetId>,
    pub custom_feature_enabled: bool,
}

impl MarketSubscription {
    pub fn new(asset_ids: Vec<AssetId>) -> Self {
        Self {
            asset_ids,
            custom_feature_enabled: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceChange {
    pub asset_id: AssetId,
    pub price: Fixed,
    pub size: Fixed,
    pub side: Side,
    pub best_bid: Option<Fixed>,
    pub best_ask: Option<Fixed>,
    pub hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum MarketEvent {
    Book {
        asset_id: AssetId,
        market: String,
        timestamp_ms: u64,
        hash: Option<String>,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    PriceChange {
        market: String,
        timestamp_ms: u64,
        price_changes: Vec<PriceChange>,
    },
    TickSizeChange {
        asset_id: AssetId,
        market: String,
        timestamp_ms: u64,
        old_tick_size: Fixed,
        new_tick_size: Fixed,
    },
    LastTradePrice {
        asset_id: AssetId,
        market: String,
        timestamp_ms: u64,
        price: Fixed,
        size: Option<Fixed>,
        side: Option<Side>,
    },
    BestBidAsk {
        asset_id: AssetId,
        market: String,
        timestamp_ms: u64,
        best_bid: Fixed,
        best_ask: Fixed,
    },
    NewMarket {
        condition_id: ConditionId,
        asset_ids: Vec<AssetId>,
    },
    MarketResolved {
        condition_id: ConditionId,
        asset_ids: Vec<AssetId>,
        winning_asset_id: AssetId,
    },
}

pub enum MarketFeedMessage {
    Connected,
    Events {
        events: Vec<MarketEvent>,
        processed: oneshot::Sender<Vec<AssetId>>,
    },
    Disconnected(String),
}

pub async fn run_market_feed(
    endpoint: String,
    asset_ids: Vec<AssetId>,
    reconnect_min_ms: u64,
    reconnect_max_ms: u64,
    sender: mpsc::Sender<MarketFeedMessage>,
    shutdown: CancellationToken,
) -> Result<()> {
    asset_ids.iter().try_for_each(|asset| {
        U256::from_str(asset.as_ref())
            .map_err(|_| BotError::Config(format!("invalid_market_asset_id:{asset}")))
            .map(|_| ())
    })?;
    if asset_ids.is_empty() {
        return Err(BotError::Config("market_feed_requires_assets".to_string()));
    }

    let mut backoff_ms = reconnect_min_ms.max(1);
    while !shutdown.is_cancelled() {
        let mut connected_once = false;
        let result = run_market_session(
            &endpoint,
            &asset_ids,
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
            .unwrap_or_else(|| "market_stream_ended".to_string());
        sender
            .send(MarketFeedMessage::Disconnected(detail))
            .await
            .map_err(|_| BotError::Execution("market_feed_receiver_closed".to_string()))?;
        if connected_once {
            backoff_ms = reconnect_min_ms.max(1);
        }
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            () = sleep(Duration::from_millis(backoff_ms)) => {}
        }
        backoff_ms = backoff_ms.saturating_mul(2).min(reconnect_max_ms.max(1));
    }
    Ok(())
}

async fn run_market_session(
    endpoint: &str,
    assets: &[AssetId],
    sender: &mpsc::Sender<MarketFeedMessage>,
    shutdown: &CancellationToken,
    connected_once: &mut bool,
) -> Result<()> {
    let url = market_channel_url(endpoint);
    let (socket, _) = connect_async(&url)
        .await
        .map_err(|error| BotError::Protocol(format!("market_ws_connect:{error}")))?;
    run_connected_market_session(
        socket,
        assets,
        sender,
        shutdown,
        connected_once,
        PING_INTERVAL_MS,
    )
    .await
}

async fn run_connected_market_session<S>(
    socket: WebSocketStream<S>,
    assets: &[AssetId],
    sender: &mpsc::Sender<MarketFeedMessage>,
    shutdown: &CancellationToken,
    connected_once: &mut bool,
    ping_interval_ms: u64,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut write, mut read) = socket.split();
    let subscription = MarketSubscription::new(assets.to_vec());
    write
        .send(Message::Text(
            market_subscription_payload(&subscription).into(),
        ))
        .await
        .map_err(|error| BotError::Protocol(format!("market_ws_subscribe:{error}")))?;
    sender
        .send(MarketFeedMessage::Connected)
        .await
        .map_err(|_| BotError::Execution("market_feed_receiver_closed".to_string()))?;
    *connected_once = true;
    let mut heartbeat = interval(Duration::from_millis(ping_interval_ms.max(1)));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_received = Instant::now();

    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            _ = heartbeat.tick() => {
                if heartbeat_timed_out(last_received.elapsed(), ping_interval_ms) {
                    return Err(BotError::Protocol("market_ws_heartbeat_timeout".to_string()));
                }
                write.send(Message::Text("PING".into())).await
                    .map_err(|error| BotError::Protocol(format!("market_ws_ping:{error}")))?;
            }
            frame = read.next() => {
                let frame = frame
                    .ok_or_else(|| BotError::Protocol("market_stream_eof".to_string()))?
                    .map_err(|error| BotError::Protocol(format!("market_stream:{error}")))?;
                last_received = Instant::now();
                match frame {
                    Message::Text(text) if text == "PONG" => {}
                    Message::Text(text) => {
                        let events = parse_wire_events(text.as_bytes())?;
                        if events.is_empty() {
                            continue;
                        }
                        let mut pending = VecDeque::from([events]);
                        let mut pending_event_count = pending.front().map_or(0, Vec::len);
                        while let Some(events) = pending.pop_front() {
                            pending_event_count = pending_event_count.saturating_sub(events.len());
                            let (processed, completion) = oneshot::channel();
                            sender.send(MarketFeedMessage::Events { events, processed }).await.map_err(|_| {
                                BotError::Execution("market_feed_receiver_closed".to_string())
                            })?;
                            let unresolved = await_market_batch_completion(
                                completion,
                                &mut write,
                                &mut read,
                                &mut heartbeat,
                                &mut last_received,
                                &mut pending,
                                &mut pending_event_count,
                                shutdown,
                                ping_interval_ms,
                            ).await?;
                            if let Some(asset_id) = unresolved.first() {
                                return Err(BotError::Protocol(format!(
                                    "market_book_repair_requested:{asset_id}"
                                )));
                            }
                        }
                    }
                    Message::Ping(payload) => {
                        write.send(Message::Pong(payload)).await.map_err(|error| {
                            BotError::Protocol(format!("market_ws_pong:{error}"))
                        })?;
                    }
                    Message::Pong(_) => {}
                    Message::Close(frame) => {
                        return Err(BotError::Protocol(format!(
                            "market_stream_closed:{frame:?}"
                        )));
                    }
                    Message::Binary(_) | Message::Frame(_) => {}
                }
            }
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the pending-batch loop must retain ordered socket, heartbeat, queue, and shutdown state"
)]
async fn await_market_batch_completion<S>(
    mut completion: oneshot::Receiver<Vec<AssetId>>,
    write: &mut futures_util::stream::SplitSink<WebSocketStream<S>, Message>,
    read: &mut futures_util::stream::SplitStream<WebSocketStream<S>>,
    heartbeat: &mut tokio::time::Interval,
    last_received: &mut Instant,
    pending: &mut VecDeque<Vec<MarketEvent>>,
    pending_event_count: &mut usize,
    shutdown: &CancellationToken,
    ping_interval_ms: u64,
) -> Result<Vec<AssetId>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                return Err(BotError::Execution("market_session_shutdown".to_string()));
            }
            result = &mut completion => {
                return result.map_err(|_| {
                    BotError::Execution("market_batch_processing_abandoned".to_string())
                });
            }
            _ = heartbeat.tick() => {
                if heartbeat_timed_out(last_received.elapsed(), ping_interval_ms) {
                    return Err(BotError::Protocol("market_ws_heartbeat_timeout".to_string()));
                }
                write.send(Message::Text("PING".into())).await
                    .map_err(|error| BotError::Protocol(format!("market_ws_ping:{error}")))?;
            }
            frame = read.next() => {
                let frame = frame
                    .ok_or_else(|| BotError::Protocol("market_stream_eof".to_string()))?
                    .map_err(|error| BotError::Protocol(format!("market_stream:{error}")))?;
                *last_received = Instant::now();
                match frame {
                    Message::Text(text) if text == "PONG" => {}
                    Message::Text(text) => {
                        let events = parse_wire_events(text.as_bytes())?;
                        if events.is_empty() {
                            continue;
                        }
                        let next_count = pending_event_count
                            .checked_add(events.len())
                            .ok_or_else(|| BotError::Protocol("market_batch_backlog_overflow".to_string()))?;
                        if pending.len() >= MAX_PENDING_MARKET_BATCHES
                            || next_count > MAX_PENDING_MARKET_EVENTS
                        {
                            return Err(BotError::Protocol(
                                "market_batch_backlog_exceeded".to_string(),
                            ));
                        }
                        *pending_event_count = next_count;
                        pending.push_back(events);
                    }
                    Message::Ping(payload) => {
                        write.send(Message::Pong(payload)).await.map_err(|error| {
                            BotError::Protocol(format!("market_ws_pong:{error}"))
                        })?;
                    }
                    Message::Pong(_) => {}
                    Message::Close(frame) => {
                        return Err(BotError::Protocol(format!(
                            "market_stream_closed:{frame:?}"
                        )));
                    }
                    Message::Binary(_) | Message::Frame(_) => {}
                }
            }
        }
    }
}

fn heartbeat_timed_out(silence: Duration, ping_interval_ms: u64) -> bool {
    silence > Duration::from_millis(ping_interval_ms.saturating_mul(3))
}

fn market_channel_url(endpoint: &str) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    if endpoint.ends_with("/ws/market") {
        endpoint.to_string()
    } else {
        format!("{endpoint}/ws/market")
    }
}

fn parse_wire_events(raw: &[u8]) -> Result<Vec<MarketEvent>> {
    let value: Value = serde_json::from_slice(raw)
        .map_err(|error| BotError::Parse(format!("market_json:{error}")))?;
    let values = match value {
        Value::Object(_) => vec![value],
        Value::Array(values) => values,
        _ => return Ok(Vec::new()),
    };
    values
        .into_iter()
        .filter(|value| value.get("event_type").is_some())
        .map(|value| {
            let message: WsMessage = serde_json::from_value(value)
                .map_err(|error| BotError::Parse(format!("market_message:{error}")))?;
            convert_ws_message(message)
        })
        .collect()
}

fn convert_ws_message(message: WsMessage) -> Result<MarketEvent> {
    match message {
        WsMessage::Book(value) => convert_book(value),
        WsMessage::PriceChange(value) => convert_price_change(value),
        WsMessage::TickSizeChange(value) => convert_tick_size(value),
        WsMessage::LastTradePrice(value) => convert_last_trade(value),
        WsMessage::BestBidAsk(value) => convert_best_bid_ask(value),
        WsMessage::MarketResolved(value) => convert_resolution(value),
        WsMessage::NewMarket(value) => Ok(MarketEvent::NewMarket {
            condition_id: ConditionId::from(value.market.to_string()),
            asset_ids: value
                .asset_ids
                .into_iter()
                .map(|asset| AssetId::from(asset.to_string()))
                .collect(),
        }),
        WsMessage::Trade(_) | WsMessage::Order(_) => Err(BotError::Protocol(
            "user_message_received_on_market_channel".to_string(),
        )),
        _ => Err(BotError::Protocol("unsupported_market_message".to_string())),
    }
}

pub fn apply_market_event(
    book: &mut BookState,
    event: &MarketEvent,
    local_received_at_ms: u64,
) -> Result<()> {
    match event {
        MarketEvent::Book {
            asset_id,
            timestamp_ms,
            hash,
            bids,
            asks,
            ..
        } => {
            if &book.asset_id != asset_id {
                return Ok(());
            }
            invalidate_on_error(
                book,
                validate_event_timestamp(*timestamp_ms, local_received_at_ms),
            )?;
            invalidate_on_error(book, validate_event_order(*timestamp_ms, book))?;
            invalidate_on_error(book, validate_snapshot_levels(bids, asks, book.tick_size))?;
            if hash.as_deref().is_none_or(str::is_empty) {
                book.tradeable = false;
                return Err(BotError::Protocol(
                    "authoritative_book_hash_missing".to_string(),
                ));
            }
            book.bids.clone_from(bids);
            book.asks.clone_from(asks);
            book.book_hash.clone_from(hash);
            book.exchange_timestamp_ms = *timestamp_ms;
            book.local_received_at_ms = local_received_at_ms;
            sort_and_refresh_top(book);
            book.tradeable = book.best_bid.is_some() && book.best_ask.is_some();
        }
        MarketEvent::PriceChange {
            timestamp_ms,
            price_changes,
            ..
        } => {
            let matching = price_changes
                .iter()
                .filter(|change| change.asset_id == book.asset_id)
                .collect::<Vec<_>>();
            if matching.is_empty() {
                return Ok(());
            }
            if !book.tradeable || book.book_hash.as_deref().is_none_or(str::is_empty) {
                book.tradeable = false;
                return Err(BotError::Protocol(
                    "delta_without_authoritative_book".to_string(),
                ));
            }
            invalidate_on_error(
                book,
                validate_event_timestamp(*timestamp_ms, local_received_at_ms),
            )?;
            invalidate_on_error(book, validate_event_order(*timestamp_ms, book))?;
            let final_hash = matching
                .last()
                .and_then(|change| change.hash.as_deref())
                .filter(|hash| !hash.is_empty())
                .ok_or_else(|| {
                    book.tradeable = false;
                    BotError::Protocol("price_change_hash_missing".to_string())
                })?
                .to_string();
            for change in &matching {
                invalidate_on_error(
                    book,
                    validate_delta_level(change.price, change.size, book.tick_size),
                )?;
            }
            for change in &matching {
                apply_level_change(book, change.side, change.price, change.size);
            }
            book.book_hash = Some(final_hash);
            let final_change = matching.last().expect("matching changes are non-empty");
            if final_change
                .best_bid
                .is_some_and(|expected| book.best_bid != Some(expected))
                || final_change
                    .best_ask
                    .is_some_and(|expected| book.best_ask != Some(expected))
            {
                book.tradeable = false;
                return Err(BotError::Protocol(
                    "book_top_diverged_after_delta".to_string(),
                ));
            }
            book.exchange_timestamp_ms = *timestamp_ms;
            book.local_received_at_ms = local_received_at_ms;
            if matches!((book.best_bid, book.best_ask), (Some(bid), Some(ask)) if bid >= ask) {
                book.tradeable = false;
                return Err(BotError::Protocol(
                    "crossed_or_locked_book_after_delta".to_string(),
                ));
            }
        }
        MarketEvent::TickSizeChange {
            asset_id,
            timestamp_ms,
            old_tick_size,
            new_tick_size,
            ..
        } if asset_id == &book.asset_id => {
            invalidate_on_error(
                book,
                validate_event_timestamp(*timestamp_ms, local_received_at_ms),
            )?;
            invalidate_on_error(book, validate_event_order(*timestamp_ms, book))?;
            if *new_tick_size <= Fixed::ZERO || *old_tick_size != book.tick_size {
                book.tradeable = false;
                return Err(BotError::Protocol(
                    "tick_size_transition_invalid".to_string(),
                ));
            }
            book.tick_size = *new_tick_size;
            book.exchange_timestamp_ms = *timestamp_ms;
            book.local_received_at_ms = local_received_at_ms;
            book.tradeable = false;
        }
        MarketEvent::BestBidAsk {
            asset_id,
            timestamp_ms,
            best_bid,
            best_ask,
            ..
        } if asset_id == &book.asset_id => {
            if *timestamp_ms < book.exchange_timestamp_ms {
                return Ok(());
            }
            invalidate_on_error(
                book,
                validate_event_timestamp(*timestamp_ms, local_received_at_ms),
            )?;
            if best_bid >= best_ask
                || !best_bid.is_aligned_to_tick(book.tick_size)
                || !best_ask.is_aligned_to_tick(book.tick_size)
                || book.best_bid != Some(*best_bid)
                || book.best_ask != Some(*best_ask)
            {
                book.tradeable = false;
                return Err(BotError::Protocol("best_bid_ask_diverged".to_string()));
            }
        }
        MarketEvent::LastTradePrice {
            asset_id,
            timestamp_ms,
            price,
            size,
            ..
        } if asset_id == &book.asset_id => {
            if *timestamp_ms < book.exchange_timestamp_ms {
                return Ok(());
            }
            invalidate_on_error(
                book,
                validate_event_timestamp(*timestamp_ms, local_received_at_ms),
            )?;
            if *price <= Fixed::ZERO
                || *price >= Fixed::ONE
                || size.is_some_and(|value| value <= Fixed::ZERO)
            {
                book.tradeable = false;
                return Err(BotError::Protocol("last_trade_invalid".to_string()));
            }
            if book
                .last_trade_timestamp_ms
                .is_none_or(|previous| *timestamp_ms > previous)
            {
                book.last_trade_price = Some(*price);
                book.last_trade_timestamp_ms = Some(*timestamp_ms);
            }
        }
        MarketEvent::MarketResolved { asset_ids, .. }
            if asset_ids.iter().any(|asset| asset == &book.asset_id) =>
        {
            book.tradeable = false;
        }
        MarketEvent::LastTradePrice { .. }
        | MarketEvent::NewMarket { .. }
        | MarketEvent::TickSizeChange { .. }
        | MarketEvent::BestBidAsk { .. }
        | MarketEvent::MarketResolved { .. } => {}
    }
    Ok(())
}

fn invalidate_on_error(book: &mut BookState, result: Result<()>) -> Result<()> {
    if result.is_err() {
        book.tradeable = false;
    }
    result
}

fn validate_event_timestamp(exchange_timestamp_ms: u64, local_received_at_ms: u64) -> Result<()> {
    if exchange_timestamp_ms > local_received_at_ms {
        return Err(BotError::Protocol(
            "market_event_timestamp_after_receipt".to_string(),
        ));
    }
    Ok(())
}

fn validate_event_order(exchange_timestamp_ms: u64, book: &BookState) -> Result<()> {
    if book.exchange_timestamp_ms > 0 && exchange_timestamp_ms < book.exchange_timestamp_ms {
        return Err(BotError::Protocol("out_of_order_market_event".to_string()));
    }
    Ok(())
}

fn validate_snapshot_levels(bids: &[Level], asks: &[Level], tick_size: Fixed) -> Result<()> {
    if bids.len() > MAX_LEVELS_PER_SIDE || asks.len() > MAX_LEVELS_PER_SIDE {
        return Err(BotError::Protocol("book_level_limit_exceeded".to_string()));
    }
    let mut bid_prices = BTreeSet::new();
    let mut ask_prices = BTreeSet::new();
    for level in bids {
        validate_level(level.price, level.size, tick_size, false)?;
        if !bid_prices.insert(level.price) {
            return Err(BotError::Protocol("duplicate_bid_price".to_string()));
        }
    }
    for level in asks {
        validate_level(level.price, level.size, tick_size, false)?;
        if !ask_prices.insert(level.price) {
            return Err(BotError::Protocol("duplicate_ask_price".to_string()));
        }
    }
    let best_bid = bids.iter().map(|level| level.price).max();
    let best_ask = asks.iter().map(|level| level.price).min();
    if matches!((best_bid, best_ask), (Some(bid), Some(ask)) if bid >= ask) {
        return Err(BotError::Protocol(
            "crossed_or_locked_book_snapshot".to_string(),
        ));
    }
    Ok(())
}

fn validate_delta_level(price: Fixed, size: Fixed, tick_size: Fixed) -> Result<()> {
    validate_level(price, size, tick_size, true)
}

fn validate_level(
    price: Fixed,
    size: Fixed,
    tick_size: Fixed,
    allow_zero_size: bool,
) -> Result<()> {
    if tick_size <= Fixed::ZERO {
        return Err(BotError::Protocol("tick_size_must_be_positive".to_string()));
    }
    if price <= Fixed::ZERO || price >= Fixed::ONE {
        return Err(BotError::Protocol("book_price_out_of_range".to_string()));
    }
    if !price.is_aligned_to_tick(tick_size) {
        return Err(BotError::Protocol(
            "book_price_not_tick_aligned".to_string(),
        ));
    }
    if size < Fixed::ZERO || (!allow_zero_size && size.is_zero()) {
        return Err(BotError::Protocol("book_size_must_be_positive".to_string()));
    }
    Ok(())
}

pub fn market_subscription_payload(subscription: &MarketSubscription) -> String {
    let ids = subscription
        .asset_ids
        .iter()
        .map(|id| format!("\"{}\"", id.as_ref()))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"type\":\"market\",\"assets_ids\":[{ids}],\"custom_feature_enabled\":{}}}",
        subscription.custom_feature_enabled
    )
}

pub fn parse_fixture_event(raw: &str) -> Result<MarketEvent> {
    let message: WsMessage = serde_json::from_str(raw)
        .map_err(|error| BotError::Parse(format!("market_json:{error}")))?;
    convert_ws_message(message)
}

fn convert_book(value: BookUpdate) -> Result<MarketEvent> {
    Ok(MarketEvent::Book {
        asset_id: AssetId::from(value.asset_id.to_string()),
        market: value.market.to_string(),
        timestamp_ms: timestamp_ms(value.timestamp)?,
        hash: value.hash,
        bids: value
            .bids
            .into_iter()
            .map(|level| fixed_level(level.price.to_string(), level.size.to_string()))
            .collect::<Result<Vec<_>>>()?,
        asks: value
            .asks
            .into_iter()
            .map(|level| fixed_level(level.price.to_string(), level.size.to_string()))
            .collect::<Result<Vec<_>>>()?,
    })
}

fn convert_price_change(value: ApiPriceChange) -> Result<MarketEvent> {
    Ok(MarketEvent::PriceChange {
        market: value.market.to_string(),
        timestamp_ms: timestamp_ms(value.timestamp)?,
        price_changes: value
            .price_changes
            .into_iter()
            .map(|change| {
                Ok(PriceChange {
                    asset_id: AssetId::from(change.asset_id.to_string()),
                    price: change.price.to_string().parse()?,
                    size: change
                        .size
                        .ok_or_else(|| BotError::Protocol("price_change_size_missing".to_string()))?
                        .to_string()
                        .parse()?,
                    side: convert_side(change.side)?,
                    best_bid: change
                        .best_bid
                        .map(|price| price.to_string().parse())
                        .transpose()?,
                    best_ask: change
                        .best_ask
                        .map(|price| price.to_string().parse())
                        .transpose()?,
                    hash: change.hash,
                })
            })
            .collect::<Result<Vec<_>>>()?,
    })
}

fn convert_tick_size(value: ApiTickSizeChange) -> Result<MarketEvent> {
    Ok(MarketEvent::TickSizeChange {
        asset_id: AssetId::from(value.asset_id.to_string()),
        market: value.market.to_string(),
        timestamp_ms: timestamp_ms(value.timestamp)?,
        old_tick_size: value.old_tick_size.to_string().parse()?,
        new_tick_size: value.new_tick_size.to_string().parse()?,
    })
}

fn convert_last_trade(value: ApiLastTradePrice) -> Result<MarketEvent> {
    Ok(MarketEvent::LastTradePrice {
        asset_id: AssetId::from(value.asset_id.to_string()),
        market: value.market.to_string(),
        timestamp_ms: timestamp_ms(value.timestamp)?,
        price: value.price.to_string().parse()?,
        size: value
            .size
            .map(|size| size.to_string().parse())
            .transpose()?,
        side: value.side.map(convert_side).transpose()?,
    })
}

fn convert_best_bid_ask(value: ApiBestBidAsk) -> Result<MarketEvent> {
    Ok(MarketEvent::BestBidAsk {
        asset_id: AssetId::from(value.asset_id.to_string()),
        market: value.market.to_string(),
        timestamp_ms: timestamp_ms(value.timestamp)?,
        best_bid: value.best_bid.to_string().parse()?,
        best_ask: value.best_ask.to_string().parse()?,
    })
}

fn convert_resolution(value: ApiMarketResolved) -> Result<MarketEvent> {
    Ok(MarketEvent::MarketResolved {
        condition_id: ConditionId::from(value.market.to_string()),
        asset_ids: value
            .asset_ids
            .into_iter()
            .map(|asset| AssetId::from(asset.to_string()))
            .collect(),
        winning_asset_id: AssetId::from(value.winning_asset_id.to_string()),
    })
}

fn convert_side(value: ApiSide) -> Result<Side> {
    match value {
        ApiSide::Buy => Ok(Side::Buy),
        ApiSide::Sell => Ok(Side::Sell),
        _ => Err(BotError::Protocol("unsupported_market_side".to_string())),
    }
}

fn fixed_level(price: String, size: String) -> Result<Level> {
    Ok(Level {
        price: price.parse()?,
        size: size.parse()?,
    })
}

fn timestamp_ms(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| BotError::Protocol("negative_market_timestamp".to_string()))
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    const ORDERED_TEST_BATCH: &str = r#"[
      {"event_type":"price_change","market":"0x1111111111111111111111111111111111111111111111111111111111111111","timestamp":"10","price_changes":[{"asset_id":"1","price":"0.48","size":"2","side":"BUY","hash":"delta-hash"}]},
      {"event_type":"book","asset_id":"1","market":"0x1111111111111111111111111111111111111111111111111111111111111111","timestamp":"10","hash":"book-hash","bids":[{"price":"0.48","size":"2"}],"asks":[{"price":"0.52","size":"2"}]}
    ]"#;

    async fn spawn_test_market_socket() -> (
        WebSocketStream<tokio::io::DuplexStream>,
        tokio::task::JoinHandle<()>,
        Arc<AtomicUsize>,
    ) {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let client = WebSocketStream::from_raw_socket(
            client_io,
            tokio_tungstenite::tungstenite::protocol::Role::Client,
            None,
        )
        .await;
        let mut socket = WebSocketStream::from_raw_socket(
            server_io,
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            None,
        )
        .await;
        let ping_count = Arc::new(AtomicUsize::new(0));
        let server_ping_count = Arc::clone(&ping_count);
        let task = tokio::spawn(async move {
            let subscription = socket.next().await.unwrap().unwrap();
            assert!(matches!(
                subscription,
                Message::Text(ref text) if text.contains("\"type\":\"market\"")
            ));
            socket
                .send(Message::Text(ORDERED_TEST_BATCH.into()))
                .await
                .unwrap();
            while let Some(frame) = socket.next().await {
                match frame {
                    Ok(Message::Text(text)) if text == "PING" => {
                        server_ping_count.fetch_add(1, Ordering::AcqRel);
                        if socket.send(Message::Text("PONG".into())).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Ping(payload)) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }
        });
        (client, task, ping_count)
    }

    async fn next_feed_message(
        receiver: &mut mpsc::Receiver<MarketFeedMessage>,
    ) -> MarketFeedMessage {
        tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("market feed message timed out")
            .expect("market feed sender closed")
    }

    #[test]
    fn subscription_enables_custom_features() {
        let sub = MarketSubscription::new(vec![AssetId::from("asset")]);
        assert!(sub.custom_feature_enabled);
        assert_eq!(
            market_subscription_payload(&sub),
            "{\"type\":\"market\",\"assets_ids\":[\"asset\"],\"custom_feature_enabled\":true}"
        );
    }

    #[test]
    fn heartbeat_timeout_uses_three_complete_ping_intervals() {
        assert!(!heartbeat_timed_out(
            Duration::from_millis(PING_INTERVAL_MS * 3),
            PING_INTERVAL_MS
        ));
        assert!(heartbeat_timed_out(
            Duration::from_millis(PING_INTERVAL_MS * 3 + 1),
            PING_INTERVAL_MS
        ));
    }

    #[tokio::test]
    async fn ordered_session_waits_for_batch_result_and_returns_repair_for_reconnect() {
        let (socket, server, ping_count) = spawn_test_market_socket().await;
        let (sender, mut receiver) = mpsc::channel(8);
        let shutdown = CancellationToken::new();
        let session_shutdown = shutdown.clone();
        let session = tokio::spawn(async move {
            let mut connected_once = false;
            run_connected_market_session(
                socket,
                &[AssetId::from("1")],
                &sender,
                &session_shutdown,
                &mut connected_once,
                5,
            )
            .await
        });

        assert!(matches!(
            next_feed_message(&mut receiver).await,
            MarketFeedMessage::Connected
        ));
        let MarketFeedMessage::Events { events, processed } =
            next_feed_message(&mut receiver).await
        else {
            panic!("expected an ordered market event batch")
        };
        assert!(matches!(
            events.as_slice(),
            [MarketEvent::PriceChange { .. }, MarketEvent::Book { .. }]
        ));
        assert!(!session.is_finished());
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(ping_count.load(Ordering::Acquire) > 0);
        assert!(!session.is_finished());
        processed.send(vec![AssetId::from("1")]).unwrap();

        let error = tokio::time::timeout(Duration::from_secs(2), session)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("market_book_repair_requested:1"));
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn parses_full_snapshot_and_multi_asset_delta() {
        let initial =
            parse_fixture_event(include_str!("../fixtures/ws/book_initial.json")).unwrap();
        let mut book = BookState::empty("1", "0.001".parse().unwrap(), "5".parse().unwrap());
        apply_market_event(&mut book, &initial, 1_782_750_000_000).unwrap();
        assert!(book.tradeable);
        let delta = parse_fixture_event(include_str!(
            "../fixtures/ws/price_changes_multi_asset.json"
        ))
        .unwrap();
        apply_market_event(&mut book, &delta, 1_782_750_000_001).unwrap();
        assert_eq!(book.best_bid.unwrap().to_string(), "0.5");
        assert_eq!(book.book_hash.as_deref(), Some("hash-a"));
    }

    #[test]
    fn wire_batch_preserves_same_timestamp_cross_type_order() {
        let raw = br#"[
          {"event_type":"price_change","market":"0x1111111111111111111111111111111111111111111111111111111111111111","timestamp":"10","price_changes":[{"asset_id":"1","price":"0.48","size":"2","side":"BUY","hash":"delta-hash"}]},
          {"event_type":"book","asset_id":"1","market":"0x1111111111111111111111111111111111111111111111111111111111111111","timestamp":"10","hash":"book-hash","bids":[{"price":"0.48","size":"2"}],"asks":[{"price":"0.52","size":"2"}]}
        ]"#;

        let events = parse_wire_events(raw).unwrap();

        assert!(matches!(
            events.as_slice(),
            [MarketEvent::PriceChange { .. }, MarketEvent::Book { .. }]
        ));
    }

    #[test]
    fn parser_accepts_leading_dot_price_and_all_levels() {
        let raw = r#"{
          "event_type":"book","asset_id":"1",
          "market":"0x1111111111111111111111111111111111111111111111111111111111111111",
          "timestamp":"10","hash":"h",
          "bids":[{"price":".48","size":"2"},{"price":".47","size":"3"}],
          "asks":[{"price":".52","size":"4"},{"price":".53","size":"5"}]
        }"#;
        let MarketEvent::Book { bids, asks, .. } = parse_fixture_event(raw).unwrap() else {
            panic!("expected book")
        };
        assert_eq!(bids.len(), 2);
        assert_eq!(asks.len(), 2);
        assert_eq!(bids[0].price.to_string(), "0.48");
    }

    #[test]
    fn tick_size_change_disables_until_fresh_snapshot() {
        let mut book = BookState::empty("1", "0.001".parse().unwrap(), "5".parse().unwrap());
        book.tradeable = true;
        apply_market_event(
            &mut book,
            &MarketEvent::TickSizeChange {
                asset_id: AssetId::from("1"),
                market: "m".to_string(),
                timestamp_ms: 1,
                old_tick_size: "0.001".parse().unwrap(),
                new_tick_size: "0.01".parse().unwrap(),
            },
            1,
        )
        .unwrap();
        assert!(!book.tradeable);
        assert_eq!(book.tick_size.to_string(), "0.01");
    }

    #[test]
    fn malformed_snapshot_invalidates_book() {
        let mut book = BookState::empty("1", "0.001".parse().unwrap(), "5".parse().unwrap());
        book.tradeable = true;
        let event = MarketEvent::Book {
            asset_id: AssetId::from("1"),
            market: "m".to_string(),
            timestamp_ms: 10,
            hash: Some("h".to_string()),
            bids: vec![Level {
                price: "0.4995".parse().unwrap(),
                size: "10".parse().unwrap(),
            }],
            asks: vec![Level {
                price: "0.501".parse().unwrap(),
                size: "10".parse().unwrap(),
            }],
        };
        let error = apply_market_event(&mut book, &event, 10).unwrap_err();
        assert!(error.to_string().contains("tick_aligned"));
        assert!(!book.tradeable);
    }

    #[test]
    fn invalid_last_trade_terms_invalidate_an_authoritative_book() {
        let mut book = BookState::empty("1", "0.001".parse().unwrap(), "5".parse().unwrap());
        book.tradeable = true;
        book.book_hash = Some("authoritative".to_string());
        let event = MarketEvent::LastTradePrice {
            asset_id: AssetId::from("1"),
            market: "m".to_string(),
            timestamp_ms: 10,
            price: Fixed::ONE,
            size: Some(Fixed::ZERO),
            side: Some(Side::Buy),
        };

        let error = apply_market_event(&mut book, &event, 10).unwrap_err();

        assert!(error.to_string().contains("last_trade_invalid"));
        assert!(!book.tradeable);
    }

    #[test]
    fn last_trade_older_than_the_authoritative_book_is_ignored() {
        let mut book = BookState::empty("1", "0.001".parse().unwrap(), "5".parse().unwrap());
        book.tradeable = true;
        book.book_hash = Some("authoritative".to_string());
        book.exchange_timestamp_ms = 100;
        book.local_received_at_ms = 100;
        let event = MarketEvent::LastTradePrice {
            asset_id: AssetId::from("1"),
            market: "m".to_string(),
            timestamp_ms: 90,
            price: "0.5".parse().unwrap(),
            size: Some("10".parse().unwrap()),
            side: Some(Side::Sell),
        };

        apply_market_event(&mut book, &event, 110).unwrap();

        assert!(book.tradeable);
        assert_eq!(book.exchange_timestamp_ms, 100);
        assert!(book.last_trade_price.is_none());
        assert!(book.last_trade_timestamp_ms.is_none());
    }

    #[test]
    fn unrelated_resolution_does_not_disable_book() {
        let mut book = BookState::empty("1", "0.001".parse().unwrap(), "5".parse().unwrap());
        book.tradeable = true;
        let event = MarketEvent::MarketResolved {
            condition_id: ConditionId::from("c"),
            asset_ids: vec![AssetId::from("2")],
            winning_asset_id: AssetId::from("2"),
        };
        apply_market_event(&mut book, &event, 1).unwrap();
        assert!(book.tradeable);
    }

    #[test]
    fn delta_without_final_hash_invalidates_authoritative_book() {
        let initial =
            parse_fixture_event(include_str!("../fixtures/ws/book_initial.json")).unwrap();
        let mut book = BookState::empty("1", "0.001".parse().unwrap(), "5".parse().unwrap());
        apply_market_event(&mut book, &initial, 1_782_750_000_000).unwrap();
        let event = MarketEvent::PriceChange {
            market: "m".to_string(),
            timestamp_ms: 1_782_750_000_001,
            price_changes: vec![PriceChange {
                asset_id: AssetId::from("1"),
                price: "0.5".parse().unwrap(),
                size: "10".parse().unwrap(),
                side: Side::Buy,
                best_bid: Some("0.5".parse().unwrap()),
                best_ask: Some("0.501".parse().unwrap()),
                hash: None,
            }],
        };
        let error = apply_market_event(&mut book, &event, 1_782_750_000_001).unwrap_err();
        assert!(error.to_string().contains("hash_missing"));
        assert!(!book.tradeable);
    }

    #[test]
    fn out_of_order_snapshot_cannot_roll_book_back() {
        let initial =
            parse_fixture_event(include_str!("../fixtures/ws/book_initial.json")).unwrap();
        let mut book = BookState::empty("1", "0.001".parse().unwrap(), "5".parse().unwrap());
        apply_market_event(&mut book, &initial, 1_782_750_000_000).unwrap();
        let mut older = initial;
        if let MarketEvent::Book { timestamp_ms, .. } = &mut older {
            *timestamp_ms = 1_782_749_999_999;
        }
        let error = apply_market_event(&mut book, &older, 1_782_750_000_001).unwrap_err();
        assert!(error.to_string().contains("out_of_order"));
        assert!(!book.tradeable);
    }
}
