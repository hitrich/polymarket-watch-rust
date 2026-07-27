use crate::error::{BotError, Result};
use crate::fixed::Fixed;
use crate::types::{AssetId, BotMode, ClobProtocol, ConditionId, SignatureType, WalletMode};
use crate::wallet_watch::WalletScore;
use serde::Deserialize;
use std::collections::BTreeSet;
use std::fs;
use url::Url;

pub const LIVE_CONFIRMATION_PHRASE: &str = "ENABLE_LIVE_POLYMARKET_ORDERS";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub mode: BotMode,
    pub venue: String,
    pub clob_protocol: ClobProtocol,
    pub clob_host: String,
    pub chain_id: u64,
    pub collateral_asset: String,
    pub wallet_mode: WalletMode,
    pub signature_type: SignatureType,
    pub funder_address: String,
    pub geoblock_url: String,
    pub max_geoblock_age_ms: u64,
    pub max_book_age_ms: u64,
    pub max_event_lag_ms: u64,
    pub max_order_usdc: Fixed,
    pub max_market_exposure_usdc: Fixed,
    pub max_daily_loss_usdc: Fixed,
    pub max_slippage_ticks: u64,
    pub max_market_impact_bps: u64,
    pub min_top_book_size: Fixed,
    pub order_ttl_ms: u64,
    pub max_new_orders_per_second: u64,
    pub max_cancels_per_second: u64,
    pub max_position_notional_usdc: Fixed,
    pub neg_risk_enabled: bool,
    pub asset_ids: Vec<AssetId>,
    pub condition_ids: Vec<ConditionId>,
    #[serde(default)]
    pub watched_wallets: Vec<String>,
    #[serde(default)]
    pub controlled_wallets: Vec<String>,
    #[serde(default)]
    pub wallet_scores: Vec<WalletScore>,
    #[serde(default)]
    pub external_symbols: Vec<String>,

    #[serde(default = "default_market_ws_endpoint")]
    pub market_ws_endpoint: String,
    #[serde(default = "default_user_ws_endpoint")]
    pub user_ws_endpoint: String,
    #[serde(default = "default_data_api_host")]
    pub data_api_host: String,
    #[serde(default = "default_coinbase_ws_endpoint")]
    pub coinbase_ws_endpoint: String,
    #[serde(default = "default_journal_path")]
    pub journal_path: String,
    #[serde(default = "default_live_risk_baseline_path")]
    pub live_risk_baseline_path: String,
    #[serde(default = "default_compliance_latch_path")]
    pub compliance_latch_path: String,
    #[serde(default = "default_max_clock_drift_ms")]
    pub max_clock_drift_ms: u64,
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    #[serde(default = "default_snapshot_interval_ms")]
    pub snapshot_interval_ms: u64,
    #[serde(default = "default_reconnect_min_ms")]
    pub reconnect_min_ms: u64,
    #[serde(default = "default_reconnect_max_ms")]
    pub reconnect_max_ms: u64,
    #[serde(default = "default_wallet_poll_interval_ms")]
    pub wallet_poll_interval_ms: u64,
    #[serde(default = "default_wallet_signal_max_age_ms")]
    pub wallet_signal_max_age_ms: u64,
    #[serde(default = "default_external_stale_ms")]
    pub external_stale_ms: u64,
    #[serde(default = "default_strategy_size")]
    pub strategy_size: Fixed,
    #[serde(default = "default_min_spread")]
    pub min_spread: Fixed,
    #[serde(default = "default_paper_starting_cash")]
    pub paper_starting_cash_usdc: Fixed,
    #[serde(default)]
    pub paper_fee_bps: u64,
    #[serde(default = "default_paper_fill_participation_bps")]
    pub paper_fill_participation_bps: u64,
    #[serde(default)]
    pub enable_wallet_copying: bool,
    #[serde(default)]
    pub enable_external_signal: bool,
    #[serde(default)]
    pub live_order_submission_enabled: bool,
    #[serde(default)]
    pub live_confirmation: String,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            mode: BotMode::Paper,
            venue: "polymarket_international".to_string(),
            clob_protocol: ClobProtocol::V2,
            clob_host: "https://clob.polymarket.com".to_string(),
            chain_id: 137,
            collateral_asset: "pUSD".to_string(),
            wallet_mode: WalletMode::DepositWallet,
            signature_type: SignatureType::Poly1271,
            funder_address: "0x0000000000000000000000000000000000000000".to_string(),
            geoblock_url: "https://polymarket.com/api/geoblock".to_string(),
            max_geoblock_age_ms: 60_000,
            max_book_age_ms: 250,
            max_event_lag_ms: 500,
            max_order_usdc: Fixed::from_scaled(10_000_000),
            max_market_exposure_usdc: Fixed::from_scaled(100_000_000),
            max_daily_loss_usdc: Fixed::from_scaled(50_000_000),
            max_slippage_ticks: 2,
            max_market_impact_bps: 25,
            min_top_book_size: Fixed::from_scaled(5_000_000),
            order_ttl_ms: 750,
            max_new_orders_per_second: 3,
            max_cancels_per_second: 5,
            max_position_notional_usdc: Fixed::from_scaled(250_000_000),
            neg_risk_enabled: false,
            asset_ids: Vec::new(),
            condition_ids: Vec::new(),
            watched_wallets: Vec::new(),
            controlled_wallets: Vec::new(),
            wallet_scores: Vec::new(),
            external_symbols: Vec::new(),
            market_ws_endpoint: default_market_ws_endpoint(),
            user_ws_endpoint: default_user_ws_endpoint(),
            data_api_host: default_data_api_host(),
            coinbase_ws_endpoint: default_coinbase_ws_endpoint(),
            journal_path: default_journal_path(),
            live_risk_baseline_path: default_live_risk_baseline_path(),
            compliance_latch_path: default_compliance_latch_path(),
            max_clock_drift_ms: default_max_clock_drift_ms(),
            heartbeat_interval_ms: default_heartbeat_interval_ms(),
            snapshot_interval_ms: default_snapshot_interval_ms(),
            reconnect_min_ms: default_reconnect_min_ms(),
            reconnect_max_ms: default_reconnect_max_ms(),
            wallet_poll_interval_ms: default_wallet_poll_interval_ms(),
            wallet_signal_max_age_ms: default_wallet_signal_max_age_ms(),
            external_stale_ms: default_external_stale_ms(),
            strategy_size: default_strategy_size(),
            min_spread: default_min_spread(),
            paper_starting_cash_usdc: default_paper_starting_cash(),
            paper_fee_bps: 0,
            paper_fill_participation_bps: default_paper_fill_participation_bps(),
            enable_wallet_copying: false,
            enable_external_signal: false,
            live_order_submission_enabled: false,
            live_confirmation: String::new(),
        }
    }
}

impl Settings {
    pub fn load(path: &str) -> Result<Self> {
        let raw = fs::read_to_string(path)?;
        Self::from_toml_like(&raw)
    }

    pub fn from_toml_like(raw: &str) -> Result<Self> {
        let settings: Self = toml::from_str(raw)
            .map_err(|error| BotError::Config(format!("invalid TOML: {error}")))?;
        settings.validate()?;
        Ok(settings)
    }

    pub fn validate(&self) -> Result<()> {
        if self.venue.trim().is_empty() {
            return Err(BotError::Config("missing venue".to_string()));
        }
        validate_url("clob_host", &self.clob_host, &["https"])?;
        validate_url("geoblock_url", &self.geoblock_url, &["https"])?;
        validate_url("market_ws_endpoint", &self.market_ws_endpoint, &["wss"])?;
        validate_url("user_ws_endpoint", &self.user_ws_endpoint, &["wss"])?;
        validate_url("data_api_host", &self.data_api_host, &["https"])?;
        validate_url("coinbase_ws_endpoint", &self.coinbase_ws_endpoint, &["wss"])?;

        if self.collateral_asset.trim().is_empty() {
            return Err(BotError::Config("missing collateral_asset".to_string()));
        }
        if !is_evm_address(&self.funder_address)
            || self.funder_address == "0x0000000000000000000000000000000000000000"
        {
            return Err(BotError::Config("invalid funder_address".to_string()));
        }
        if self.asset_ids.is_empty() {
            return Err(BotError::Config("missing asset_ids".to_string()));
        }
        if self.condition_ids.is_empty() {
            return Err(BotError::Config("missing condition_ids".to_string()));
        }
        if self.asset_ids.len() != self.condition_ids.len() {
            return Err(BotError::Config(
                "asset_ids and condition_ids must have matching lengths".to_string(),
            ));
        }
        validate_unique_asset_ids(&self.asset_ids)?;
        validate_unique_pairs(&self.asset_ids, &self.condition_ids)?;
        for asset_id in &self.asset_ids {
            if !valid_asset_id(asset_id.as_ref()) {
                return Err(BotError::Config(format!("invalid asset_id:{asset_id}")));
            }
        }
        for condition_id in &self.condition_ids {
            if !valid_condition_id(condition_id.as_ref()) {
                return Err(BotError::Config(format!(
                    "invalid condition_id:{condition_id}"
                )));
            }
        }
        for wallet in &self.watched_wallets {
            if !is_evm_address(wallet) {
                return Err(BotError::Config(format!("invalid watched_wallet:{wallet}")));
            }
        }
        if has_duplicates_case_insensitive(self.watched_wallets.iter().map(String::as_str)) {
            return Err(BotError::Config("duplicate watched_wallet".to_string()));
        }
        for wallet in &self.controlled_wallets {
            if !is_evm_address(wallet) {
                return Err(BotError::Config(format!(
                    "invalid controlled_wallet:{wallet}"
                )));
            }
        }
        if has_duplicates_case_insensitive(self.controlled_wallets.iter().map(String::as_str)) {
            return Err(BotError::Config("duplicate controlled_wallet".to_string()));
        }
        if !self
            .controlled_wallets
            .iter()
            .any(|wallet| wallet.eq_ignore_ascii_case(&self.funder_address))
        {
            return Err(BotError::Config(
                "controlled_wallets_must_declare_funder_address".to_string(),
            ));
        }
        if self.watched_wallets.iter().any(|watched| {
            self.controlled_wallets
                .iter()
                .any(|controlled| controlled.eq_ignore_ascii_case(watched))
        }) {
            return Err(BotError::Config(
                "watched_wallet_must_not_be_operator_controlled".to_string(),
            ));
        }
        let mut policy_wallets = BTreeSet::new();
        for policy in &self.wallet_scores {
            policy.validate()?;
            if !self
                .watched_wallets
                .iter()
                .any(|wallet| wallet.eq_ignore_ascii_case(&policy.address))
            {
                return Err(BotError::Config(format!(
                    "wallet_score_not_in_watched_wallets:{}",
                    policy.address
                )));
            }
            if !policy_wallets.insert(policy.address.to_ascii_lowercase()) {
                return Err(BotError::Config("duplicate_wallet_score".to_string()));
            }
        }
        if self.enable_wallet_copying && self.wallet_scores.is_empty() {
            return Err(BotError::Config(
                "wallet_copying_requires_wallet_scores".to_string(),
            ));
        }
        for symbol in &self.external_symbols {
            if symbol.is_empty()
                || !symbol
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            {
                return Err(BotError::Config(format!(
                    "invalid external_symbol:{symbol}"
                )));
            }
        }
        if has_duplicates(self.external_symbols.iter().map(String::as_str)) {
            return Err(BotError::Config("duplicate external_symbol".to_string()));
        }
        if self.enable_external_signal && self.external_symbols.is_empty() {
            return Err(BotError::Config(
                "external_signal_requires_external_symbols".to_string(),
            ));
        }

        for (name, value) in [
            ("max_order_usdc", self.max_order_usdc),
            ("max_market_exposure_usdc", self.max_market_exposure_usdc),
            ("max_daily_loss_usdc", self.max_daily_loss_usdc),
            ("min_top_book_size", self.min_top_book_size),
            (
                "max_position_notional_usdc",
                self.max_position_notional_usdc,
            ),
            ("strategy_size", self.strategy_size),
            ("min_spread", self.min_spread),
            ("paper_starting_cash_usdc", self.paper_starting_cash_usdc),
        ] {
            if value <= Fixed::ZERO {
                return Err(BotError::Config(format!("{name} must be positive")));
            }
        }
        for (name, value) in [
            ("max_geoblock_age_ms", self.max_geoblock_age_ms),
            ("max_book_age_ms", self.max_book_age_ms),
            ("max_event_lag_ms", self.max_event_lag_ms),
            ("order_ttl_ms", self.order_ttl_ms),
            ("max_new_orders_per_second", self.max_new_orders_per_second),
            ("max_cancels_per_second", self.max_cancels_per_second),
            ("snapshot_interval_ms", self.snapshot_interval_ms),
            ("reconnect_min_ms", self.reconnect_min_ms),
            ("reconnect_max_ms", self.reconnect_max_ms),
            ("wallet_poll_interval_ms", self.wallet_poll_interval_ms),
            ("wallet_signal_max_age_ms", self.wallet_signal_max_age_ms),
            ("external_stale_ms", self.external_stale_ms),
            ("max_clock_drift_ms", self.max_clock_drift_ms),
            ("heartbeat_interval_ms", self.heartbeat_interval_ms),
        ] {
            if value == 0 {
                return Err(BotError::Config(format!("{name} must be positive")));
            }
        }
        if self.reconnect_min_ms > self.reconnect_max_ms {
            return Err(BotError::Config(
                "reconnect_min_ms must not exceed reconnect_max_ms".to_string(),
            ));
        }
        if self.max_geoblock_age_ms < 2_000 {
            return Err(BotError::Config(
                "max_geoblock_age_ms must be at least 2000".to_string(),
            ));
        }
        if self.heartbeat_interval_ms > 5_000 {
            return Err(BotError::Config(
                "heartbeat_interval_ms must not exceed 5000".to_string(),
            ));
        }
        if self.max_order_usdc > self.max_market_exposure_usdc
            || self.max_order_usdc > self.max_position_notional_usdc
        {
            return Err(BotError::Config(
                "max_order_usdc exceeds an exposure limit".to_string(),
            ));
        }
        if self.max_market_impact_bps > 10_000
            || self.paper_fee_bps > 10_000
            || self.paper_fill_participation_bps == 0
            || self.paper_fill_participation_bps > 10_000
        {
            return Err(BotError::Config(
                "basis-point settings must be within 1..=10000 where applicable".to_string(),
            ));
        }
        if self.min_spread >= Fixed::ONE {
            return Err(BotError::Config("min_spread must be below 1".to_string()));
        }
        if self.mode == BotMode::Live
            && self.strategy_size.floor_to_decimals(2)? != self.strategy_size
        {
            return Err(BotError::Config(
                "live strategy_size must use at most two decimal places".to_string(),
            ));
        }
        if self.neg_risk_enabled {
            return Err(BotError::Config(
                "negative-risk execution is not supported by the current exposure model"
                    .to_string(),
            ));
        }
        if self.journal_path.trim().is_empty() {
            return Err(BotError::Config(
                "journal_path must not be empty".to_string(),
            ));
        }
        if self.live_risk_baseline_path.trim().is_empty() {
            return Err(BotError::Config(
                "live_risk_baseline_path must not be empty".to_string(),
            ));
        }
        if self.compliance_latch_path.trim().is_empty() {
            return Err(BotError::Config(
                "compliance_latch_path must not be empty".to_string(),
            ));
        }
        if self.compliance_latch_path == self.journal_path
            || self.compliance_latch_path == self.live_risk_baseline_path
        {
            return Err(BotError::Config(
                "compliance_latch_path_must_be_dedicated".to_string(),
            ));
        }
        Ok(())
    }

    pub fn live_confirmation_valid(&self) -> bool {
        self.live_order_submission_enabled && self.live_confirmation == LIVE_CONFIRMATION_PHRASE
    }

    pub fn configured_markets(&self) -> impl Iterator<Item = (&AssetId, &ConditionId)> {
        self.asset_ids.iter().zip(&self.condition_ids)
    }
}

fn validate_url(name: &str, value: &str, allowed_schemes: &[&str]) -> Result<()> {
    let url = Url::parse(value).map_err(|_| BotError::Config(format!("invalid {name}")))?;
    if !allowed_schemes.contains(&url.scheme()) || url.host_str().is_none() {
        return Err(BotError::Config(format!("invalid {name}")));
    }
    Ok(())
}

fn validate_unique_asset_ids(asset_ids: &[AssetId]) -> Result<()> {
    if has_duplicates(asset_ids.iter().map(AsRef::as_ref)) {
        return Err(BotError::Config("duplicate asset_id".to_string()));
    }
    Ok(())
}

fn validate_unique_pairs(asset_ids: &[AssetId], condition_ids: &[ConditionId]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for (asset, condition) in asset_ids.iter().zip(condition_ids) {
        if !seen.insert((asset.as_ref(), condition.as_ref())) {
            return Err(BotError::Config("duplicate market mapping".to_string()));
        }
    }
    Ok(())
}

fn has_duplicates<'a>(values: impl Iterator<Item = &'a str>) -> bool {
    let mut seen = BTreeSet::new();
    values.into_iter().any(|value| !seen.insert(value))
}

fn has_duplicates_case_insensitive<'a>(values: impl Iterator<Item = &'a str>) -> bool {
    let mut seen = BTreeSet::new();
    values
        .into_iter()
        .any(|value| !seen.insert(value.to_ascii_lowercase()))
}

pub fn is_evm_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub fn valid_asset_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 78
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && value.bytes().any(|byte| byte != b'0')
}

pub fn valid_condition_id(value: &str) -> bool {
    value.len() == 66
        && value.starts_with("0x")
        && value[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn default_market_ws_endpoint() -> String {
    "wss://ws-subscriptions-clob.polymarket.com".to_string()
}

fn default_user_ws_endpoint() -> String {
    "wss://ws-subscriptions-clob.polymarket.com".to_string()
}

fn default_data_api_host() -> String {
    "https://data-api.polymarket.com".to_string()
}

fn default_coinbase_ws_endpoint() -> String {
    "wss://advanced-trade-ws.coinbase.com".to_string()
}

fn default_journal_path() -> String {
    "logs/decisions.jsonl".to_string()
}

fn default_live_risk_baseline_path() -> String {
    "logs/live-risk-baseline.json".to_string()
}

fn default_compliance_latch_path() -> String {
    "logs/compliance-latch.json".to_string()
}

const fn default_max_clock_drift_ms() -> u64 {
    2_000
}

const fn default_heartbeat_interval_ms() -> u64 {
    5_000
}

const fn default_snapshot_interval_ms() -> u64 {
    1_000
}

const fn default_reconnect_min_ms() -> u64 {
    250
}

const fn default_reconnect_max_ms() -> u64 {
    30_000
}

const fn default_wallet_poll_interval_ms() -> u64 {
    5_000
}

const fn default_wallet_signal_max_age_ms() -> u64 {
    120_000
}

const fn default_external_stale_ms() -> u64 {
    2_000
}

const fn default_strategy_size() -> Fixed {
    Fixed::from_scaled(1_000_000)
}

const fn default_min_spread() -> Fixed {
    Fixed::from_scaled(10_000)
}

const fn default_paper_starting_cash() -> Fixed {
    Fixed::from_scaled(10_000_000_000)
}

const fn default_paper_fill_participation_bps() -> u64 {
    2_500
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> String {
        include_str!("../config.example.toml").to_string()
    }

    #[test]
    fn parses_complete_example() {
        let settings = Settings::from_toml_like(&valid_config()).unwrap();
        assert_eq!(settings.mode, BotMode::Paper);
        assert_eq!(settings.asset_ids.len(), 1);
        assert_eq!(settings.condition_ids.len(), 1);
        assert_eq!(settings.max_order_usdc.to_string(), "10");
        assert!(!settings.live_confirmation_valid());
    }

    #[test]
    fn rejects_duplicate_safety_critical_config_key() {
        let raw = format!("{}\nmax_order_usdc = \"1000\"\n", valid_config());
        let err = Settings::from_toml_like(&raw).unwrap_err();
        assert!(err.to_string().contains("duplicate") || err.to_string().contains("invalid TOML"));
    }

    #[test]
    fn rejects_unknown_config_key() {
        let raw = format!("{}\nmagic_latency = 80\n", valid_config());
        let err = Settings::from_toml_like(&raw).unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn rejects_missing_critical_config_fields() {
        let err = Settings::from_toml_like(r#"mode = "paper""#).unwrap_err();
        assert!(err.to_string().contains("missing field"));
    }

    #[test]
    fn rejects_malformed_funder_address() {
        let raw = valid_config().replace(
            "0x1111111111111111111111111111111111111111",
            "not-an-address",
        );
        let err = Settings::from_toml_like(&raw).unwrap_err();
        assert!(err.to_string().contains("funder_address"));
    }

    #[test]
    fn rejects_non_numeric_asset_and_malformed_condition() {
        let raw = valid_config()
            .replace("1234567890123456789012345678901234567890", "asset-a")
            .replace(
                "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                "condition-a",
            );
        let err = Settings::from_toml_like(&raw).unwrap_err();
        assert!(err.to_string().contains("asset_id"));
    }

    #[test]
    fn live_confirmation_requires_both_switch_and_exact_phrase() {
        let mut settings = Settings {
            live_order_submission_enabled: true,
            live_confirmation: LIVE_CONFIRMATION_PHRASE.to_string(),
            ..Settings::default()
        };
        assert!(settings.live_confirmation_valid());
        settings.live_confirmation.push('!');
        assert!(!settings.live_confirmation_valid());
    }

    #[test]
    fn enabled_external_gate_requires_at_least_one_symbol() {
        let raw = valid_config()
            .replace("external_symbols = [\"BTC-USD\"]", "external_symbols = []")
            .replace(
                "enable_external_signal = false",
                "enable_external_signal = true",
            );
        let error = Settings::from_toml_like(&raw).unwrap_err();
        assert!(error.to_string().contains("external_symbols"));
    }

    #[test]
    fn controlled_wallets_must_include_funder_and_cannot_be_watched() {
        let missing = valid_config().replace(
            "controlled_wallets = [\"0x1111111111111111111111111111111111111111\"]",
            "controlled_wallets = []",
        );
        assert!(Settings::from_toml_like(&missing)
            .unwrap_err()
            .to_string()
            .contains("declare_funder"));

        let overlap = valid_config().replace(
            "watched_wallets = []",
            "watched_wallets = [\"0x1111111111111111111111111111111111111111\"]",
        );
        assert!(Settings::from_toml_like(&overlap)
            .unwrap_err()
            .to_string()
            .contains("operator_controlled"));
    }
}
