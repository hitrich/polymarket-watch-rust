use crate::error::{BotError, Result};
use crate::fixed::Fixed;
use crate::types::Side;
use polymarket_client_sdk_v2::data::{
    types::{request::TradesRequest, response::Trade, Side as ApiSide},
    Client,
};
use polymarket_client_sdk_v2::types::Address;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::str::FromStr as _;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

const MAX_SEEN_TRADES_PER_WALLET: usize = 10_000;
const TRADE_PAGE_SIZE: i32 = 500;
const MAX_TRADE_OFFSET: i32 = 10_000;
const DATA_REQUEST_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalletScore {
    pub address: String,
    pub score: Fixed,
    pub max_copy_slippage: Fixed,
    pub max_copy_size: Fixed,
    pub attribution_confidence: Fixed,
    pub confidential_info_flag: bool,
}

impl WalletScore {
    pub fn validate(&self) -> Result<()> {
        Address::from_str(&self.address)
            .map_err(|_| BotError::Config(format!("invalid_wallet_address:{}", self.address)))?;
        let zero = Fixed::ZERO;
        let hundred = "100".parse::<Fixed>()?;
        if self.score < zero || self.score > hundred {
            return Err(BotError::Config("wallet_score_out_of_range".to_string()));
        }
        if self.attribution_confidence < zero || self.attribution_confidence > hundred {
            return Err(BotError::Config(
                "wallet_attribution_confidence_out_of_range".to_string(),
            ));
        }
        if self.max_copy_slippage < zero || self.max_copy_slippage > Fixed::ONE {
            return Err(BotError::Config(
                "wallet_max_copy_slippage_out_of_range".to_string(),
            ));
        }
        if self.max_copy_size <= zero {
            return Err(BotError::Config(
                "wallet_max_copy_size_must_be_positive".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalletTradeObservation {
    pub wallet: String,
    pub transaction_hash: String,
    pub asset_id: String,
    pub condition_id: String,
    pub side: Side,
    pub size: Fixed,
    pub price: Fixed,
    pub timestamp_ms: u64,
    pub title: String,
    pub slug: String,
    pub outcome: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalletWatchMessage {
    Connected,
    Warmed {
        wallet: String,
        historical_trades: usize,
    },
    Trade(WalletTradeObservation),
    Disconnected(String),
}

pub fn should_copy(wallet: &WalletScore, wallet_entry_price: Fixed, current_price: Fixed) -> bool {
    should_copy_size(
        wallet,
        wallet_entry_price,
        current_price,
        wallet.max_copy_size,
    )
}

pub fn should_copy_size(
    wallet: &WalletScore,
    wallet_entry_price: Fixed,
    current_price: Fixed,
    requested_size: Fixed,
) -> bool {
    if wallet.validate().is_err()
        || wallet.confidential_info_flag
        || wallet.score < Fixed::from_scaled(70_000_000)
        || wallet.attribution_confidence < Fixed::from_scaled(90_000_000)
        || requested_size <= Fixed::ZERO
        || requested_size > wallet.max_copy_size
        || wallet_entry_price <= Fixed::ZERO
        || wallet_entry_price >= Fixed::ONE
        || current_price <= Fixed::ZERO
        || current_price >= Fixed::ONE
    {
        return false;
    }
    current_price
        .checked_sub(wallet_entry_price)
        .and_then(Fixed::checked_abs)
        .map(|distance| distance <= wallet.max_copy_slippage)
        .unwrap_or(false)
}

pub async fn run_wallet_watch(
    data_api_host: String,
    wallets: Vec<String>,
    poll_interval_ms: u64,
    sender: mpsc::Sender<WalletWatchMessage>,
    shutdown: CancellationToken,
) -> Result<()> {
    if wallets.is_empty() {
        return Ok(());
    }
    let client = Client::new(&data_api_host)
        .map_err(|error| BotError::Protocol(format!("wallet_data_client:{error}")))?;
    let addresses = wallets
        .into_iter()
        .map(|raw| {
            Address::from_str(&raw)
                .map(|address| (raw, address))
                .map_err(|_| BotError::Config("invalid_watched_wallet".to_string()))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut seen = BTreeMap::<String, SeenTrades>::new();
    let mut ticker = interval(Duration::from_millis(poll_interval_ms.max(1)));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut connected = false;

    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            _ = ticker.tick() => {
                let mut cycle_healthy = true;
                for (wallet, address) in &addresses {
                    let warmed = seen.get(wallet).is_some_and(|wallet_seen| wallet_seen.warmed);
                    if !warmed {
                        match fetch_trade_page(&client, *address, 0).await {
                            Ok(mut trades) => {
                                trades.sort_by_key(|trade| trade.timestamp);
                                let wallet_seen = seen.entry(wallet.clone()).or_default();
                                for trade in &trades {
                                    wallet_seen.remember(trade_key(trade));
                                }
                                wallet_seen.warmed = true;
                                sender.send(WalletWatchMessage::Warmed {
                                    wallet: wallet.clone(),
                                    historical_trades: trades.len(),
                                }).await.map_err(|_| BotError::Execution(
                                    "wallet_watch_receiver_closed".to_string()
                                ))?;
                            }
                            Err(error) => {
                                mark_wallet_disconnected(
                                    &sender,
                                    wallet,
                                    &error.to_string(),
                                    &mut cycle_healthy,
                                    &mut connected,
                                ).await?;
                            }
                        }
                        continue;
                    }

                    let wallet_seen = seen.get(wallet).ok_or_else(|| {
                        BotError::Execution("wallet_seen_state_missing".to_string())
                    })?;
                    match fetch_incremental_trades(&client, *address, wallet_seen).await {
                        Ok(TradeWindow::Complete(mut trades)) => {
                            trades.sort_by_key(|trade| trade.timestamp);
                            let wallet_seen = seen.get_mut(wallet).ok_or_else(|| {
                                BotError::Execution("wallet_seen_state_missing".to_string())
                            })?;
                            for trade in trades {
                                let key = trade_key(&trade);
                                if wallet_seen.contains(&key) {
                                    continue;
                                }
                                let observation = convert_trade(wallet, trade)?;
                                wallet_seen.remember(key);
                                sender.send(WalletWatchMessage::Trade(observation)).await
                                    .map_err(|_| BotError::Execution(
                                        "wallet_watch_receiver_closed".to_string()
                                    ))?;
                            }
                        }
                        Ok(TradeWindow::Gap { scanned }) => {
                            seen.get_mut(wallet).ok_or_else(|| {
                                BotError::Execution("wallet_seen_state_missing".to_string())
                            })?.reset_for_rewarm();
                            mark_wallet_disconnected(
                                &sender,
                                wallet,
                                &format!("wallet_trade_continuity_gap_after_{scanned}_records"),
                                &mut cycle_healthy,
                                &mut connected,
                            ).await?;
                        }
                        Err(error) => {
                            mark_wallet_disconnected(
                                &sender,
                                wallet,
                                &error.to_string(),
                                &mut cycle_healthy,
                                &mut connected,
                            ).await?;
                        }
                    }
                }
                if cycle_healthy && !connected {
                    sender.send(WalletWatchMessage::Connected).await.map_err(|_| {
                        BotError::Execution("wallet_watch_receiver_closed".to_string())
                    })?;
                    connected = true;
                }
            }
        }
    }
}

#[derive(Debug)]
enum TradeWindow {
    Complete(Vec<Trade>),
    Gap { scanned: usize },
}

async fn fetch_trade_page(client: &Client, address: Address, offset: i32) -> Result<Vec<Trade>> {
    let request = TradesRequest::builder()
        .user(address)
        .limit(TRADE_PAGE_SIZE)
        .map_err(|error| BotError::Config(format!("wallet_trade_limit:{error}")))?
        .offset(offset)
        .map_err(|error| BotError::Config(format!("wallet_trade_offset:{error}")))?
        .taker_only(false)
        .build();
    match tokio::time::timeout(DATA_REQUEST_TIMEOUT, client.trades(&request)).await {
        Ok(Ok(trades)) => Ok(trades),
        Ok(Err(error)) => Err(BotError::Protocol(format!(
            "wallet_trade_page:{offset}:{error}"
        ))),
        Err(_) => Err(BotError::Protocol(format!(
            "wallet_trade_page:{offset}:timeout"
        ))),
    }
}

async fn fetch_incremental_trades(
    client: &Client,
    address: Address,
    seen: &SeenTrades,
) -> Result<TradeWindow> {
    let mut collected = Vec::new();
    let mut collected_keys = HashSet::new();

    for offset in (0..=MAX_TRADE_OFFSET).step_by(TRADE_PAGE_SIZE as usize) {
        let page = fetch_trade_page(client, address, offset).await?;
        let page_len = page.len();
        let overlap_found = page.iter().any(|trade| seen.contains(&trade_key(trade)));
        for trade in page {
            if collected_keys.insert(trade_key(&trade)) {
                collected.push(trade);
            }
        }

        if overlap_found || (seen.keys.is_empty() && page_len < TRADE_PAGE_SIZE as usize) {
            return Ok(TradeWindow::Complete(collected));
        }
        if page_len < TRADE_PAGE_SIZE as usize {
            return Ok(TradeWindow::Gap {
                scanned: collected.len(),
            });
        }
    }

    Ok(TradeWindow::Gap {
        scanned: collected.len(),
    })
}

async fn mark_wallet_disconnected(
    sender: &mpsc::Sender<WalletWatchMessage>,
    wallet: &str,
    reason: &str,
    cycle_healthy: &mut bool,
    connected: &mut bool,
) -> Result<()> {
    *cycle_healthy = false;
    *connected = false;
    sender
        .send(WalletWatchMessage::Disconnected(format!(
            "wallet_poll:{wallet}:{reason}"
        )))
        .await
        .map_err(|_| BotError::Execution("wallet_watch_receiver_closed".to_string()))
}

#[derive(Debug, Default)]
struct SeenTrades {
    warmed: bool,
    ordered: VecDeque<String>,
    keys: HashSet<String>,
}

impl SeenTrades {
    fn contains(&self, key: &str) -> bool {
        self.keys.contains(key)
    }

    fn remember(&mut self, key: String) {
        if !self.keys.insert(key.clone()) {
            return;
        }
        self.ordered.push_back(key);
        while self.ordered.len() > MAX_SEEN_TRADES_PER_WALLET {
            if let Some(oldest) = self.ordered.pop_front() {
                self.keys.remove(&oldest);
            }
        }
    }

    fn reset_for_rewarm(&mut self) {
        self.warmed = false;
        self.ordered.clear();
        self.keys.clear();
    }
}

fn trade_key(trade: &Trade) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}:{}",
        trade.transaction_hash,
        trade.condition_id,
        trade.asset,
        trade.side,
        trade.size,
        trade.price,
        trade.timestamp
    )
}

fn convert_trade(wallet: &str, trade: Trade) -> Result<WalletTradeObservation> {
    if !trade.proxy_wallet.to_string().eq_ignore_ascii_case(wallet) {
        return Err(BotError::Protocol(
            "wallet_trade_proxy_mismatch".to_string(),
        ));
    }
    let side = match trade.side {
        ApiSide::Buy => Side::Buy,
        ApiSide::Sell => Side::Sell,
        ApiSide::Unknown(value) => {
            return Err(BotError::Protocol(format!(
                "wallet_trade_unknown_side:{value}"
            )));
        }
        _ => {
            return Err(BotError::Protocol(
                "wallet_trade_unsupported_side".to_string(),
            ))
        }
    };
    let timestamp_ms = u64::try_from(trade.timestamp)
        .ok()
        .and_then(|seconds| seconds.checked_mul(1_000))
        .ok_or_else(|| BotError::Protocol("wallet_trade_timestamp_invalid".to_string()))?;
    let size: Fixed = trade.size.to_string().parse()?;
    let price: Fixed = trade.price.to_string().parse()?;
    if size <= Fixed::ZERO || price <= Fixed::ZERO || price >= Fixed::ONE {
        return Err(BotError::Protocol(
            "wallet_trade_values_invalid".to_string(),
        ));
    }
    Ok(WalletTradeObservation {
        wallet: wallet.to_string(),
        transaction_hash: trade.transaction_hash.to_string(),
        asset_id: trade.asset.to_string(),
        condition_id: trade.condition_id.to_string(),
        side,
        size,
        price,
        timestamp_ms,
        title: trade.title,
        slug: trade.slug,
        outcome: trade.outcome,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wallet() -> WalletScore {
        WalletScore {
            address: "0x0000000000000000000000000000000000000001".to_string(),
            score: "80".parse().unwrap(),
            max_copy_slippage: "0.02".parse().unwrap(),
            max_copy_size: "25".parse().unwrap(),
            attribution_confidence: "95".parse().unwrap(),
            confidential_info_flag: false,
        }
    }

    #[test]
    fn copy_policy_enforces_size_slippage_and_confidence() {
        let score = wallet();
        assert!(should_copy_size(
            &score,
            "0.40".parse().unwrap(),
            "0.41".parse().unwrap(),
            "10".parse().unwrap()
        ));
        assert!(!should_copy_size(
            &score,
            "0.40".parse().unwrap(),
            "0.43".parse().unwrap(),
            "10".parse().unwrap()
        ));
        assert!(!should_copy_size(
            &score,
            "0.40".parse().unwrap(),
            "0.41".parse().unwrap(),
            "26".parse().unwrap()
        ));
    }

    #[test]
    fn confidential_or_invalid_wallet_is_never_copied() {
        let mut score = wallet();
        score.confidential_info_flag = true;
        assert!(!should_copy(
            &score,
            "0.40".parse().unwrap(),
            "0.40".parse().unwrap()
        ));
        score.confidential_info_flag = false;
        score.address = "not-an-address".to_string();
        assert!(!should_copy(
            &score,
            "0.40".parse().unwrap(),
            "0.40".parse().unwrap()
        ));
    }

    #[test]
    fn seen_trade_cache_is_bounded_and_deduplicates() {
        let mut seen = SeenTrades::default();
        seen.remember("same".to_string());
        seen.remember("same".to_string());
        assert_eq!(seen.ordered.len(), 1);
        for index in 0..=MAX_SEEN_TRADES_PER_WALLET {
            seen.remember(index.to_string());
        }
        assert_eq!(seen.ordered.len(), MAX_SEEN_TRADES_PER_WALLET);
        assert!(!seen.contains("same"));

        seen.warmed = true;
        seen.reset_for_rewarm();
        assert!(!seen.warmed);
        assert!(seen.ordered.is_empty());
        assert!(seen.keys.is_empty());
    }
}
