use crate::error::{BotError, Result};
use crate::fixed::Fixed;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt as _, StreamExt as _};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

const EXTERNAL_IO_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalQuote {
    pub venue: String,
    pub symbol: String,
    pub price: Fixed,
    pub best_bid: Option<Fixed>,
    pub best_ask: Option<Fixed>,
    pub exchange_timestamp_ms: u64,
    pub local_received_at_ms: u64,
    pub sequence_num: u64,
    pub sequence_gap: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalFeedMessage {
    Connected,
    Quote(ExternalQuote),
    Disconnected(String),
}

#[derive(Debug, Default, Clone)]
pub struct ExternalFeatureCache {
    quotes: BTreeMap<String, ExternalQuote>,
}

impl ExternalFeatureCache {
    pub fn update(&mut self, quote: ExternalQuote) {
        self.quotes.insert(quote.symbol.clone(), quote);
    }

    pub fn update_price(&mut self, symbol: impl Into<String>, price: Fixed) {
        let symbol = symbol.into();
        self.update(ExternalQuote {
            venue: "manual".to_string(),
            symbol,
            price,
            best_bid: None,
            best_ask: None,
            exchange_timestamp_ms: 0,
            local_received_at_ms: 0,
            sequence_num: 0,
            sequence_gap: false,
        });
    }

    pub fn price(&self, symbol: &str) -> Option<Fixed> {
        self.quotes.get(symbol).map(|quote| quote.price)
    }

    pub fn quote(&self, symbol: &str) -> Option<&ExternalQuote> {
        self.quotes.get(symbol)
    }

    pub fn fresh_quote(
        &self,
        symbol: &str,
        now_ms: u64,
        max_age_ms: u64,
    ) -> Option<&ExternalQuote> {
        let quote = self.quote(symbol)?;
        (quote.local_received_at_ms <= now_ms
            && now_ms - quote.local_received_at_ms <= max_age_ms
            && quote.exchange_timestamp_ms > 0
            && quote.exchange_timestamp_ms <= quote.local_received_at_ms
            && quote.local_received_at_ms - quote.exchange_timestamp_ms <= max_age_ms
            && quote.price > Fixed::ZERO
            && quote.best_bid.is_none_or(|price| price > Fixed::ZERO)
            && quote.best_ask.is_none_or(|price| price > Fixed::ZERO)
            && !matches!((quote.best_bid, quote.best_ask), (Some(bid), Some(ask)) if bid > ask)
            && !quote.sequence_gap)
            .then_some(quote)
    }

    pub fn quotes(&self) -> impl Iterator<Item = &ExternalQuote> {
        self.quotes.values()
    }
}

pub async fn run_coinbase_feed(
    endpoint: String,
    symbols: Vec<String>,
    reconnect_min_ms: u64,
    reconnect_max_ms: u64,
    sender: mpsc::Sender<ExternalFeedMessage>,
    shutdown: CancellationToken,
) -> Result<()> {
    if symbols.is_empty() {
        return Ok(());
    }
    let mut backoff_ms = reconnect_min_ms.max(1);
    while !shutdown.is_cancelled() {
        let mut connected_once = false;
        let result =
            run_coinbase_connection(&endpoint, &symbols, &sender, &shutdown, &mut connected_once)
                .await;
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let detail = result
            .err()
            .map(|error| error.to_string())
            .unwrap_or_else(|| "coinbase_stream_ended".to_string());
        let _ = sender.send(ExternalFeedMessage::Disconnected(detail)).await;
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

async fn run_coinbase_connection(
    endpoint: &str,
    symbols: &[String],
    sender: &mpsc::Sender<ExternalFeedMessage>,
    shutdown: &CancellationToken,
    connected_once: &mut bool,
) -> Result<()> {
    let (stream, _) = tokio::time::timeout(
        EXTERNAL_IO_TIMEOUT,
        tokio_tungstenite::connect_async(endpoint),
    )
    .await
    .map_err(|_| BotError::Protocol("coinbase_connect:timeout".to_string()))?
    .map_err(|error| BotError::Protocol(format!("coinbase_connect:{error}")))?;
    let (mut write, mut read) = stream.split();
    let ticker_subscription = serde_json::json!({
        "type": "subscribe",
        "product_ids": symbols,
        "channel": "ticker"
    });
    tokio::time::timeout(
        EXTERNAL_IO_TIMEOUT,
        write.send(Message::Text(ticker_subscription.to_string().into())),
    )
    .await
    .map_err(|_| BotError::Protocol("coinbase_subscribe_ticker:timeout".to_string()))?
    .map_err(|error| BotError::Protocol(format!("coinbase_subscribe_ticker:{error}")))?;
    tokio::time::timeout(
        EXTERNAL_IO_TIMEOUT,
        write.send(Message::Text(
            serde_json::json!({"type":"subscribe","channel":"heartbeats"})
                .to_string()
                .into(),
        )),
    )
    .await
    .map_err(|_| BotError::Protocol("coinbase_subscribe_heartbeat:timeout".to_string()))?
    .map_err(|error| BotError::Protocol(format!("coinbase_subscribe_heartbeat:{error}")))?;
    sender
        .send(ExternalFeedMessage::Connected)
        .await
        .map_err(|_| BotError::Execution("external_feed_receiver_closed".to_string()))?;
    *connected_once = true;

    let mut last_ticker_sequence: Option<u64> = None;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                let _ = write.send(Message::Close(None)).await;
                return Ok(());
            }
            message = read.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        let envelope = parse_coinbase_message(text.as_str())?;
                        if envelope.channel != "ticker" {
                            continue;
                        }
                        if last_ticker_sequence.is_some_and(|last| envelope.sequence_num <= last) {
                            continue;
                        }
                        let sequence_gap = last_ticker_sequence
                            .is_some_and(|last| envelope.sequence_num > last.saturating_add(1));
                        last_ticker_sequence = Some(envelope.sequence_num);
                        let received_at_ms = system_now_ms();
                        for event in envelope.events {
                            for ticker in event.tickers {
                                let quote = ExternalQuote {
                                    venue: "coinbase".to_string(),
                                    symbol: ticker.product_id,
                                    price: ticker.price.parse()?,
                                    best_bid: ticker.best_bid.map(|value| value.parse()).transpose()?,
                                    best_ask: ticker.best_ask.map(|value| value.parse()).transpose()?,
                                    exchange_timestamp_ms: envelope.exchange_timestamp_ms,
                                    local_received_at_ms: received_at_ms,
                                    sequence_num: envelope.sequence_num,
                                    sequence_gap,
                                };
                                sender.send(ExternalFeedMessage::Quote(quote)).await.map_err(|_| {
                                    BotError::Execution("external_feed_receiver_closed".to_string())
                                })?;
                            }
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        write.send(Message::Pong(payload)).await.map_err(|error| {
                            BotError::Protocol(format!("coinbase_pong:{error}"))
                        })?;
                    }
                    Some(Ok(Message::Close(frame))) => {
                        return Err(BotError::Protocol(format!("coinbase_closed:{frame:?}")));
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        return Err(BotError::Protocol(format!("coinbase_stream:{error}")));
                    }
                    None => return Err(BotError::Protocol("coinbase_stream_eof".to_string())),
                }
            }
        }
    }
}

struct ParsedCoinbaseEnvelope {
    channel: String,
    exchange_timestamp_ms: u64,
    sequence_num: u64,
    events: Vec<CoinbaseEvent>,
}

#[derive(Debug, Deserialize)]
struct CoinbaseEnvelope {
    #[serde(default)]
    channel: String,
    #[serde(default)]
    timestamp: String,
    #[serde(default)]
    sequence_num: u64,
    #[serde(default)]
    events: Vec<CoinbaseEvent>,
}

#[derive(Debug, Deserialize)]
struct CoinbaseEvent {
    #[serde(default)]
    tickers: Vec<CoinbaseTicker>,
}

#[derive(Debug, Deserialize)]
struct CoinbaseTicker {
    product_id: String,
    price: String,
    #[serde(default)]
    best_bid: Option<String>,
    #[serde(default)]
    best_ask: Option<String>,
}

fn parse_coinbase_message(raw: &str) -> Result<ParsedCoinbaseEnvelope> {
    let envelope: CoinbaseEnvelope = serde_json::from_str(raw)
        .map_err(|error| BotError::Parse(format!("coinbase_json:{error}")))?;
    if envelope.channel != "ticker" {
        return Ok(ParsedCoinbaseEnvelope {
            channel: envelope.channel,
            exchange_timestamp_ms: parse_rfc3339_ms(&envelope.timestamp).unwrap_or(0),
            sequence_num: envelope.sequence_num,
            events: Vec::new(),
        });
    }
    Ok(ParsedCoinbaseEnvelope {
        channel: envelope.channel,
        exchange_timestamp_ms: parse_rfc3339_ms(&envelope.timestamp)
            .ok_or_else(|| BotError::Parse("coinbase_ticker_timestamp_invalid".to_string()))?,
        sequence_num: envelope.sequence_num,
        events: envelope.events,
    })
}

fn parse_rfc3339_ms(value: &str) -> Option<u64> {
    let timestamp = DateTime::parse_from_rfc3339(value)
        .ok()?
        .with_timezone(&Utc);
    u64::try_from(timestamp.timestamp_millis()).ok()
}

fn system_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_coinbase_ticker_without_floating_point() {
        let raw = r#"{
          "channel":"ticker",
          "timestamp":"2026-07-16T12:34:56.123Z",
          "sequence_num":42,
          "events":[{"type":"update","tickers":[{
            "type":"ticker","product_id":"BTC-USD","price":"65432.12",
            "best_bid":"65431.99","best_ask":"65432.13"
          }]}]
        }"#;
        let envelope = parse_coinbase_message(raw).unwrap();
        assert_eq!(envelope.sequence_num, 42);
        assert_eq!(envelope.events[0].tickers[0].product_id, "BTC-USD");
        assert!(envelope.exchange_timestamp_ms > 0);
    }

    #[test]
    fn cache_rejects_stale_or_gapped_quotes() {
        let mut cache = ExternalFeatureCache::default();
        cache.update(ExternalQuote {
            venue: "coinbase".to_string(),
            symbol: "BTC-USD".to_string(),
            price: "100".parse().unwrap(),
            best_bid: None,
            best_ask: None,
            exchange_timestamp_ms: 900,
            local_received_at_ms: 1_000,
            sequence_num: 1,
            sequence_gap: false,
        });
        assert!(cache.fresh_quote("BTC-USD", 1_100, 100).is_some());
        assert!(cache.fresh_quote("BTC-USD", 1_101, 100).is_none());
        cache.quotes.get_mut("BTC-USD").unwrap().sequence_gap = true;
        assert!(cache.fresh_quote("BTC-USD", 1_100, 100).is_none());
        let quote = cache.quotes.get_mut("BTC-USD").unwrap();
        quote.sequence_gap = false;
        quote.exchange_timestamp_ms = 1_001;
        quote.local_received_at_ms = 1_000;
        assert!(cache.fresh_quote("BTC-USD", 1_000, 100).is_none());
    }
}
