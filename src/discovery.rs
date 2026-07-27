use crate::error::{BotError, Result};
use crate::fixed::Fixed;
use crate::types::{AssetId, ConditionId, MarketMeta};
use polymarket_client_sdk_v2::clob::{Client, Config};
use polymarket_client_sdk_v2::types::U256;
use serde::{Deserialize, Serialize};
use std::str::FromStr as _;
use std::time::Duration;

const DISCOVERY_REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
const DISCOVERY_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveredMarket {
    pub configured_asset_id: AssetId,
    pub question: String,
    pub slug: String,
    pub meta: MarketMeta,
}

#[derive(Debug, Default, Clone)]
pub struct MarketCatalog {
    markets: Vec<DiscoveredMarket>,
}

impl MarketCatalog {
    pub fn new(markets: Vec<DiscoveredMarket>) -> Self {
        Self { markets }
    }

    pub fn by_asset_id(&self, asset_id: &AssetId) -> Option<&MarketMeta> {
        self.markets
            .iter()
            .find(|market| {
                &market.meta.asset_id_yes == asset_id || &market.meta.asset_id_no == asset_id
            })
            .map(|market| &market.meta)
    }

    pub fn discovered_by_asset(&self, asset_id: &AssetId) -> Option<&DiscoveredMarket> {
        self.markets.iter().find(|market| {
            &market.configured_asset_id == asset_id
                || &market.meta.asset_id_yes == asset_id
                || &market.meta.asset_id_no == asset_id
        })
    }

    pub fn markets(&self) -> impl Iterator<Item = &DiscoveredMarket> {
        self.markets.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.markets.is_empty()
    }
}

pub async fn discover_configured_markets(
    clob_host: &str,
    configured: impl IntoIterator<Item = (AssetId, ConditionId)>,
) -> Result<MarketCatalog> {
    let configured = configured.into_iter().collect::<Vec<_>>();
    match tokio::time::timeout(
        DISCOVERY_OPERATION_TIMEOUT,
        discover_configured_markets_inner(clob_host, configured),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(BotError::Protocol(
            "market_discovery_operation_timeout".to_string(),
        )),
    }
}

async fn discover_configured_markets_inner(
    clob_host: &str,
    configured: Vec<(AssetId, ConditionId)>,
) -> Result<MarketCatalog> {
    let client = Client::new(clob_host, Config::default())
        .map_err(|error| BotError::Protocol(format!("discovery_client:{error}")))?;
    let mut markets = Vec::new();
    for (asset_id, condition_id) in configured {
        U256::from_str(asset_id.as_ref())
            .map_err(|_| BotError::Config(format!("invalid_asset_id:{asset_id}")))?;
        let response = match tokio::time::timeout(
            DISCOVERY_REQUEST_TIMEOUT,
            client.market(condition_id.as_ref()),
        )
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                return Err(BotError::Protocol(format!(
                    "market_discovery:{condition_id}:{error}"
                )))
            }
            Err(_) => {
                return Err(BotError::Protocol(format!(
                    "market_discovery:{condition_id}:timeout"
                )))
            }
        };
        let response_condition = response.condition_id.ok_or_else(|| {
            BotError::Protocol(format!("market_condition_missing:{condition_id}"))
        })?;
        if !response_condition
            .to_string()
            .eq_ignore_ascii_case(condition_id.as_ref())
        {
            return Err(BotError::Protocol(format!(
                "market_condition_mismatch:{condition_id}"
            )));
        }
        if response.tokens.len() != 2 {
            return Err(BotError::Readiness(format!(
                "only_binary_markets_supported:{condition_id}:tokens={}",
                response.tokens.len()
            )));
        }
        let first = AssetId::from(response.tokens[0].token_id.to_string());
        let second = AssetId::from(response.tokens[1].token_id.to_string());
        if asset_id != first && asset_id != second {
            return Err(BotError::Protocol(format!(
                "configured_asset_not_in_market:{asset_id}:{condition_id}"
            )));
        }
        let (yes, no) = if response.tokens[0].outcome.eq_ignore_ascii_case("yes") {
            (first, second)
        } else if response.tokens[1].outcome.eq_ignore_ascii_case("yes") {
            (second, first)
        } else {
            (first, second)
        };
        let tick_size: Fixed = response.minimum_tick_size.to_string().parse()?;
        let min_order_size: Fixed = response.minimum_order_size.to_string().parse()?;
        if tick_size <= Fixed::ZERO || min_order_size <= Fixed::ZERO {
            return Err(BotError::Protocol(format!(
                "invalid_market_parameters:{condition_id}"
            )));
        }
        markets.push(DiscoveredMarket {
            configured_asset_id: asset_id,
            question: response.question,
            slug: response.market_slug,
            meta: MarketMeta {
                condition_id,
                asset_id_yes: yes,
                asset_id_no: no,
                tick_size,
                min_order_size,
                neg_risk: response.neg_risk,
                active: response.active,
                accepting_orders: response.accepting_orders,
                resolved: response.closed,
                paused: response.archived || !response.enable_order_book,
                taker_delay_enabled: response.seconds_delay > 0,
                fees_enabled: response.maker_base_fee
                    > polymarket_client_sdk_v2::types::Decimal::ZERO
                    || response.taker_base_fee > polymarket_client_sdk_v2::types::Decimal::ZERO,
            },
        });
    }
    Ok(MarketCatalog::new(markets))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn discovered(configured: &str, yes: &str, no: &str) -> DiscoveredMarket {
        DiscoveredMarket {
            configured_asset_id: AssetId::from(configured),
            question: "Question?".to_string(),
            slug: "question".to_string(),
            meta: MarketMeta {
                condition_id: ConditionId::from("c"),
                asset_id_yes: AssetId::from(yes),
                asset_id_no: AssetId::from(no),
                tick_size: "0.01".parse().unwrap(),
                min_order_size: "1".parse().unwrap(),
                neg_risk: false,
                active: true,
                accepting_orders: true,
                resolved: false,
                paused: false,
                taker_delay_enabled: false,
                fees_enabled: false,
            },
        }
    }

    #[test]
    fn catalog_finds_either_outcome_asset() {
        let catalog = MarketCatalog::new(vec![discovered("1", "1", "2")]);
        assert!(catalog.by_asset_id(&AssetId::from("1")).is_some());
        assert!(catalog.by_asset_id(&AssetId::from("2")).is_some());
        assert!(catalog.by_asset_id(&AssetId::from("3")).is_none());
    }
}
