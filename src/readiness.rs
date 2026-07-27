use crate::config::Settings;
use crate::error::{BotError, Result};
use crate::types::{BotMode, ClobProtocol, SignatureType, WalletMode};

const PRODUCTION_MARKET_WS: &str = "wss://ws-subscriptions-clob.polymarket.com";
const PRODUCTION_USER_WS: &str = "wss://ws-subscriptions-clob.polymarket.com";
const PRODUCTION_DATA_API: &str = "https://data-api.polymarket.com";
const PRODUCTION_COINBASE_WS: &str = "wss://advanced-trade-ws.coinbase.com";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReadinessState {
    pub protocol_verified: bool,
    pub wallet_path_verified: bool,
    pub signer_authorized: bool,
    pub funder_verified: bool,
    pub balance_verified: bool,
    pub allowance_verified: bool,
    pub api_credentials_verified: bool,
    pub market_parameters_verified: bool,
    pub clock_synced: bool,
    pub journal_verified: bool,
    pub heartbeat_ready: bool,
    pub reconciled_after_startup: bool,
}

pub fn validate_protocol_compatibility(settings: &Settings) -> Result<()> {
    if settings.venue != "polymarket_international" {
        return Err(BotError::Readiness("venue_mismatch".to_string()));
    }
    if settings.geoblock_url != "https://polymarket.com/api/geoblock" {
        return Err(BotError::Readiness("geoblock_url_mismatch".to_string()));
    }
    match settings.clob_protocol {
        ClobProtocol::V2 => {
            if settings.clob_host != "https://clob.polymarket.com" {
                return Err(BotError::Readiness("v2_host_mismatch".to_string()));
            }
            if settings.collateral_asset != "pUSD" {
                return Err(BotError::Readiness("v2_collateral_mismatch".to_string()));
            }
            let wallet_signature_match = matches!(
                (settings.wallet_mode, settings.signature_type),
                (WalletMode::DepositWallet, SignatureType::Poly1271)
                    | (WalletMode::GnosisSafe, SignatureType::GnosisSafe)
                    | (WalletMode::Proxy, SignatureType::Proxy)
                    | (WalletMode::Eoa, SignatureType::Eoa)
            );
            if !wallet_signature_match {
                return Err(BotError::Readiness(
                    "v2_wallet_signature_mismatch".to_string(),
                ));
            }
        }
        ClobProtocol::V1 => {
            return Err(BotError::Readiness(
                "v1_production_protocol_retired".to_string(),
            ));
        }
    }
    if settings.chain_id != 137 {
        return Err(BotError::Readiness("chain_id_mismatch".to_string()));
    }
    if settings.mode == BotMode::Live {
        for (actual, expected, reason) in [
            (
                settings.market_ws_endpoint.as_str(),
                PRODUCTION_MARKET_WS,
                "market_ws_endpoint_mismatch",
            ),
            (
                settings.user_ws_endpoint.as_str(),
                PRODUCTION_USER_WS,
                "user_ws_endpoint_mismatch",
            ),
            (
                settings.data_api_host.as_str(),
                PRODUCTION_DATA_API,
                "data_api_host_mismatch",
            ),
        ] {
            if actual != expected {
                return Err(BotError::Readiness(reason.to_string()));
            }
        }
        if settings.enable_external_signal
            && settings.coinbase_ws_endpoint != PRODUCTION_COINBASE_WS
        {
            return Err(BotError::Readiness(
                "coinbase_ws_endpoint_mismatch".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn require_live_ready(settings: &Settings, state: &ReadinessState) -> Result<()> {
    validate_protocol_compatibility(settings)?;
    if settings.mode != BotMode::Live {
        return Ok(());
    }
    let checks = [
        (state.protocol_verified, "protocol_not_verified"),
        (state.wallet_path_verified, "wallet_path_not_verified"),
        (state.signer_authorized, "signer_not_authorized"),
        (state.funder_verified, "funder_not_verified"),
        (state.balance_verified, "balance_not_verified"),
        (state.allowance_verified, "allowance_not_verified"),
        (
            state.api_credentials_verified,
            "api_credentials_not_verified",
        ),
        (
            state.market_parameters_verified,
            "market_parameters_not_verified",
        ),
        (state.clock_synced, "clock_not_synced"),
        (state.journal_verified, "journal_not_verified"),
        (state.heartbeat_ready, "heartbeat_not_ready"),
        (state.reconciled_after_startup, "startup_not_reconciled"),
    ];
    for (passed, reason) in checks {
        if !passed {
            return Err(BotError::Readiness(reason.to_string()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_requires_deposit_wallet_poly1271() {
        let settings = Settings {
            signature_type: SignatureType::Eoa,
            ..Settings::default()
        };
        let err = validate_protocol_compatibility(&settings).unwrap_err();
        assert!(err.to_string().contains("signature"));
    }

    #[test]
    fn v2_accepts_matching_legacy_wallet_signature_pairs() {
        for (wallet_mode, signature_type) in [
            (WalletMode::GnosisSafe, SignatureType::GnosisSafe),
            (WalletMode::Proxy, SignatureType::Proxy),
            (WalletMode::Eoa, SignatureType::Eoa),
        ] {
            let settings = Settings {
                wallet_mode,
                signature_type,
                ..Settings::default()
            };
            assert!(validate_protocol_compatibility(&settings).is_ok());
        }
    }

    #[test]
    fn v1_is_rejected_for_production() {
        let settings = Settings {
            clob_protocol: ClobProtocol::V1,
            ..Settings::default()
        };
        let err = validate_protocol_compatibility(&settings).unwrap_err();
        assert!(err.to_string().contains("retired"));
    }

    #[test]
    fn pre_cutover_v2_host_is_rejected() {
        let settings = Settings {
            clob_host: "https://clob-v2.polymarket.com".to_string(),
            ..Settings::default()
        };
        let err = validate_protocol_compatibility(&settings).unwrap_err();
        assert!(err.to_string().contains("host"));
    }

    #[test]
    fn alternate_geoblock_endpoint_is_rejected() {
        let settings = Settings {
            geoblock_url: "https://example.com/geoblock".to_string(),
            ..Settings::default()
        };
        let err = validate_protocol_compatibility(&settings).unwrap_err();
        assert!(err.to_string().contains("geoblock_url"));
    }

    #[test]
    fn live_authenticated_and_risk_endpoints_are_pinned() {
        for (field, expected_reason) in [
            ("user", "user_ws_endpoint"),
            ("market", "market_ws_endpoint"),
            ("data", "data_api_host"),
        ] {
            let mut settings = Settings {
                mode: BotMode::Live,
                ..Settings::default()
            };
            match field {
                "user" => settings.user_ws_endpoint = "wss://example.com".to_string(),
                "market" => settings.market_ws_endpoint = "wss://example.com".to_string(),
                "data" => settings.data_api_host = "https://example.com".to_string(),
                _ => unreachable!(),
            }
            let error = validate_protocol_compatibility(&settings).unwrap_err();
            assert!(error.to_string().contains(expected_reason));
        }
    }

    #[test]
    fn live_mode_fails_closed_until_all_readiness_passes() {
        let settings = Settings {
            mode: BotMode::Live,
            ..Settings::default()
        };
        let err = require_live_ready(&settings, &ReadinessState::default()).unwrap_err();
        assert!(err.to_string().contains("protocol_not_verified"));
    }
}
