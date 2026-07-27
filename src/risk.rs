use crate::behavior::{BehavioralPressure, BehavioralZone};
use crate::config::Settings;
use crate::error::{BotError, Result};
use crate::fixed::Fixed;
use crate::types::{BookState, MarketMeta, OrderIntent, OwnRestingOrder, Side, TimeInForce};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskState {
    pub now_ms: u64,
    pub market_exposure_usdc: Fixed,
    pub total_exposure_usdc: Fixed,
    pub daily_loss_usdc: Fixed,
    pub available_cash_usdc: Option<Fixed>,
    pub available_position_size: Option<Fixed>,
    pub own_resting_orders: Vec<OwnRestingOrder>,
    pub own_order_cache_certain: bool,
    pub prohibited_conduct_flag: bool,
    pub behavioral_pressure: Option<BehavioralPressure>,
}

impl Default for RiskState {
    fn default() -> Self {
        Self {
            now_ms: 0,
            market_exposure_usdc: Fixed::ZERO,
            total_exposure_usdc: Fixed::ZERO,
            daily_loss_usdc: Fixed::ZERO,
            available_cash_usdc: None,
            available_position_size: None,
            own_resting_orders: Vec::new(),
            own_order_cache_certain: false,
            prohibited_conduct_flag: false,
            behavioral_pressure: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RiskDecision {
    Accept,
    Reject(String),
}

impl RiskDecision {
    pub fn into_result(self) -> Result<()> {
        match self {
            Self::Accept => Ok(()),
            Self::Reject(reason) => Err(BotError::Risk(reason)),
        }
    }
}

pub fn check_order(
    settings: &Settings,
    market: &MarketMeta,
    book: Option<&BookState>,
    intent: &OrderIntent,
    state: &RiskState,
) -> RiskDecision {
    let Some(book) = book else {
        return RiskDecision::Reject("missing_book".to_string());
    };
    if intent.asset_id.as_ref() != book.asset_id.as_ref() {
        return RiskDecision::Reject("intent_book_asset_mismatch".to_string());
    }
    if intent.asset_id != market.asset_id_yes && intent.asset_id != market.asset_id_no {
        return RiskDecision::Reject("intent_market_asset_mismatch".to_string());
    }
    if market.tick_size != book.tick_size || market.min_order_size != book.min_order_size {
        return RiskDecision::Reject("market_book_metadata_mismatch".to_string());
    }
    if !state.own_order_cache_certain {
        return RiskDecision::Reject("own_order_cache_uncertain".to_string());
    }
    if state.prohibited_conduct_flag {
        return RiskDecision::Reject("prohibited_conduct".to_string());
    }
    if !market.active || !market.accepting_orders || market.paused || market.resolved {
        return RiskDecision::Reject("market_not_tradeable".to_string());
    }
    if market.neg_risk {
        return RiskDecision::Reject("negative_risk_disabled".to_string());
    }
    if !book.tradeable || book.book_hash.is_none() {
        return RiskDecision::Reject("book_not_tradeable".to_string());
    }
    if book
        .best_bid
        .is_some_and(|price| price <= Fixed::ZERO || price >= Fixed::ONE)
        || book
            .best_ask
            .is_some_and(|price| price <= Fixed::ZERO || price >= Fixed::ONE)
    {
        return RiskDecision::Reject("book_price_out_of_range".to_string());
    }
    if book
        .best_bid
        .is_some_and(|price| !price.is_aligned_to_tick(book.tick_size))
        || book
            .best_ask
            .is_some_and(|price| !price.is_aligned_to_tick(book.tick_size))
    {
        return RiskDecision::Reject("book_price_not_tick_aligned".to_string());
    }
    if matches!((book.best_bid, book.best_ask), (Some(bid), Some(ask)) if bid >= ask) {
        return RiskDecision::Reject("crossed_or_locked_book".to_string());
    }
    let Some(book_age_ms) = book.age_ms(state.now_ms) else {
        return RiskDecision::Reject("book_received_in_future".to_string());
    };
    if book_age_ms > settings.max_book_age_ms {
        return RiskDecision::Reject("stale_book".to_string());
    }
    let Some(event_lag_ms) = book.event_lag_ms() else {
        return RiskDecision::Reject("market_event_timestamp_after_receipt".to_string());
    };
    if event_lag_ms > settings.max_event_lag_ms {
        return RiskDecision::Reject("stale_market_event".to_string());
    }
    if intent.local_expires_at_ms <= state.now_ms {
        return RiskDecision::Reject("expired_intent".to_string());
    }
    let Some(max_local_expiry_ms) = state.now_ms.checked_add(settings.order_ttl_ms) else {
        return RiskDecision::Reject("intent_ttl_window_overflow".to_string());
    };
    if intent.local_expires_at_ms > max_local_expiry_ms {
        return RiskDecision::Reject("intent_ttl_exceeds_configured_max".to_string());
    }
    match intent.time_in_force {
        TimeInForce::Gtc | TimeInForce::Fok | TimeInForce::Fak
            if intent.wire_expiration_s.is_some() =>
        {
            return RiskDecision::Reject("wire_expiration_requires_gtd".to_string());
        }
        TimeInForce::Gtd => {
            let Some(wire_expiration_s) = intent.wire_expiration_s else {
                return RiskDecision::Reject("gtd_requires_wire_expiration".to_string());
            };
            let Some(minimum_expiration_s) = (state.now_ms / 1_000).checked_add(180) else {
                return RiskDecision::Reject("gtd_expiration_window_overflow".to_string());
            };
            if wire_expiration_s < minimum_expiration_s {
                return RiskDecision::Reject(
                    "gtd_expiration_below_three_minute_minimum".to_string(),
                );
            }
        }
        TimeInForce::Fok | TimeInForce::Fak if intent.post_only => {
            return RiskDecision::Reject("post_only_requires_gtc_or_gtd".to_string());
        }
        _ => {}
    }
    if !intent.post_only {
        return RiskDecision::Reject("non_post_only_execution_not_implemented".to_string());
    }
    if intent.limit_price <= Fixed::ZERO || intent.limit_price >= Fixed::ONE {
        return RiskDecision::Reject("price_out_of_range".to_string());
    }
    if !intent.limit_price.is_aligned_to_tick(book.tick_size) {
        return RiskDecision::Reject("price_not_tick_aligned".to_string());
    }
    if intent.size < book.min_order_size || intent.size < market.min_order_size {
        return RiskDecision::Reject("size_below_minimum".to_string());
    }
    if intent.side == Side::Sell {
        let Some(available_position) = state.available_position_size else {
            return RiskDecision::Reject("available_position_unknown".to_string());
        };
        if intent.size > available_position {
            return RiskDecision::Reject("insufficient_available_position".to_string());
        }
    }
    let notional = match intent.limit_price.checked_mul_ceil(intent.size) {
        Ok(v) => v,
        Err(e) => return RiskDecision::Reject(e.to_string()),
    };
    if notional > settings.max_order_usdc {
        return RiskDecision::Reject("order_too_large".to_string());
    }
    if intent.side == Side::Buy
        && state
            .available_cash_usdc
            .is_some_and(|available| notional > available)
    {
        return RiskDecision::Reject("insufficient_available_cash".to_string());
    }
    let projected_market_exposure = match intent.side {
        Side::Buy => match state.market_exposure_usdc.checked_add(notional) {
            Ok(value) => value,
            Err(err) => return RiskDecision::Reject(err.to_string()),
        },
        Side::Sell => state.market_exposure_usdc,
    };
    if projected_market_exposure > settings.max_market_exposure_usdc {
        return RiskDecision::Reject("market_exposure_limit".to_string());
    }
    let projected_total_exposure = match intent.side {
        Side::Buy => match state.total_exposure_usdc.checked_add(notional) {
            Ok(value) => value,
            Err(err) => return RiskDecision::Reject(err.to_string()),
        },
        Side::Sell => state.total_exposure_usdc,
    };
    if projected_total_exposure > settings.max_position_notional_usdc {
        return RiskDecision::Reject("total_exposure_limit".to_string());
    }
    if state.daily_loss_usdc >= settings.max_daily_loss_usdc {
        return RiskDecision::Reject("daily_loss_limit".to_string());
    }
    if has_self_trade(intent, &state.own_resting_orders) {
        return RiskDecision::Reject("self_trade_risk".to_string());
    }
    if let Some(pressure) = &state.behavioral_pressure {
        match (pressure.zone, intent.side) {
            (BehavioralZone::NearPanicThreshold, Side::Sell) => {
                return RiskDecision::Reject("behavioral_price_pressure_prohibited".to_string());
            }
            (BehavioralZone::ThroughPanicThreshold, Side::Buy)
                if pressure.expected_adverse_move_bps > settings.max_market_impact_bps as i64 =>
            {
                return RiskDecision::Reject("behavioral_cascade_risk".to_string());
            }
            _ => {}
        }
    }
    match intent.side {
        Side::Buy => {
            let Some(ask) = book.best_ask else {
                return RiskDecision::Reject("missing_ask".to_string());
            };
            if book.top_ask_size() < settings.min_top_book_size {
                return RiskDecision::Reject("insufficient_top_ask_size".to_string());
            }
            if intent.limit_price >= ask {
                return RiskDecision::Reject("buy_crosses_best_ask".to_string());
            }
            if ask
                .ticks_between(intent.limit_price, book.tick_size)
                .unwrap_or(u64::MAX)
                > settings.max_slippage_ticks
            {
                return RiskDecision::Reject("slippage_limit".to_string());
            }
        }
        Side::Sell => {
            let Some(bid) = book.best_bid else {
                return RiskDecision::Reject("missing_bid".to_string());
            };
            if book.top_bid_size() < settings.min_top_book_size {
                return RiskDecision::Reject("insufficient_top_bid_size".to_string());
            }
            if intent.limit_price <= bid {
                return RiskDecision::Reject("sell_crosses_best_bid".to_string());
            }
            if bid
                .ticks_between(intent.limit_price, book.tick_size)
                .unwrap_or(u64::MAX)
                > settings.max_slippage_ticks
            {
                return RiskDecision::Reject("slippage_limit".to_string());
            }
        }
    }
    RiskDecision::Accept
}

fn has_self_trade(intent: &OrderIntent, own_orders: &[OwnRestingOrder]) -> bool {
    own_orders.iter().any(|own| {
        own.asset_id.as_ref() == intent.asset_id.as_ref()
            && own.side != intent.side
            && match intent.side {
                Side::Buy => intent.limit_price >= own.price,
                Side::Sell => intent.limit_price <= own.price,
            }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::behavior::{BehavioralPressure, BehavioralZone, ExpectedFlow};
    use crate::types::{AssetId, ConditionId, MarketMeta, TimeInForce};

    fn market(neg_risk: bool) -> MarketMeta {
        MarketMeta {
            condition_id: ConditionId::from("c"),
            asset_id_yes: AssetId::from("a"),
            asset_id_no: AssetId::from("b"),
            tick_size: "0.001".parse().unwrap(),
            min_order_size: "1".parse().unwrap(),
            neg_risk,
            active: true,
            accepting_orders: true,
            resolved: false,
            paused: false,
            taker_delay_enabled: false,
            fees_enabled: false,
        }
    }

    fn book() -> BookState {
        let mut book = BookState::empty("a", "0.001".parse().unwrap(), "1".parse().unwrap());
        book.asks.push(crate::types::Level {
            price: "0.501".parse().unwrap(),
            size: "10".parse().unwrap(),
        });
        book.bids.push(crate::types::Level {
            price: "0.499".parse().unwrap(),
            size: "10".parse().unwrap(),
        });
        book.best_ask = Some("0.501".parse().unwrap());
        book.best_bid = Some("0.499".parse().unwrap());
        book.book_hash = Some("h".to_string());
        book.exchange_timestamp_ms = 900;
        book.local_received_at_ms = 1000;
        book.tradeable = true;
        book
    }

    fn intent() -> OrderIntent {
        OrderIntent {
            asset_id: AssetId::from("a"),
            side: Side::Buy,
            limit_price: "0.500".parse().unwrap(),
            size: "2".parse().unwrap(),
            time_in_force: TimeInForce::Gtc,
            post_only: true,
            local_expires_at_ms: 1_500,
            wire_expiration_s: None,
            reason: "test".to_string(),
            strategy_id: "s".to_string(),
            feature_snapshot_id: "f".to_string(),
        }
    }

    fn state() -> RiskState {
        RiskState {
            now_ms: 1100,
            own_order_cache_certain: true,
            available_position_size: Some("10".parse().unwrap()),
            ..RiskState::default()
        }
    }

    #[test]
    fn rejects_negative_risk_by_default() {
        let decision = check_order(
            &Settings::default(),
            &market(true),
            Some(&book()),
            &intent(),
            &state(),
        );
        assert_eq!(
            decision,
            RiskDecision::Reject("negative_risk_disabled".to_string())
        );
    }

    #[test]
    fn rejects_negative_risk_even_when_config_flag_is_set() {
        let settings = Settings {
            neg_risk_enabled: true,
            ..Settings::default()
        };
        let decision = check_order(&settings, &market(true), Some(&book()), &intent(), &state());
        assert_eq!(
            decision,
            RiskDecision::Reject("negative_risk_disabled".to_string())
        );
    }

    #[test]
    fn rejects_missing_book() {
        let decision = check_order(
            &Settings::default(),
            &market(false),
            None,
            &intent(),
            &state(),
        );
        assert_eq!(decision, RiskDecision::Reject("missing_book".to_string()));
    }

    #[test]
    fn accepts_conservative_post_only_intent() {
        let decision = check_order(
            &Settings::default(),
            &market(false),
            Some(&book()),
            &intent(),
            &state(),
        );
        assert_eq!(decision, RiskDecision::Accept);
    }

    #[test]
    fn rejects_mismatched_intent_and_book_asset() {
        let mut intent = intent();
        intent.asset_id = AssetId::from("b");
        let decision = check_order(
            &Settings::default(),
            &market(false),
            Some(&book()),
            &intent,
            &state(),
        );
        assert_eq!(
            decision,
            RiskDecision::Reject("intent_book_asset_mismatch".to_string())
        );
    }

    #[test]
    fn rejects_market_book_metadata_mismatch() {
        let mut market = market(false);
        market.tick_size = "0.01".parse().unwrap();
        let decision = check_order(
            &Settings::default(),
            &market,
            Some(&book()),
            &intent(),
            &state(),
        );
        assert_eq!(
            decision,
            RiskDecision::Reject("market_book_metadata_mismatch".to_string())
        );
    }

    #[test]
    fn rejects_book_prices_outside_probability_range() {
        for invalid_price in [Fixed::ZERO, Fixed::ONE, Fixed::from_scaled(i128::MAX)] {
            let mut book = book();
            book.best_ask = Some(invalid_price);
            let decision = check_order(
                &Settings::default(),
                &market(false),
                Some(&book),
                &intent(),
                &state(),
            );
            assert_eq!(
                decision,
                RiskDecision::Reject("book_price_out_of_range".to_string())
            );
        }
    }

    #[test]
    fn rejects_book_prices_not_aligned_to_tick() {
        let mut book = book();
        book.best_ask = Some("0.501999".parse().unwrap());
        let decision = check_order(
            &Settings::default(),
            &market(false),
            Some(&book),
            &intent(),
            &state(),
        );
        assert_eq!(
            decision,
            RiskDecision::Reject("book_price_not_tick_aligned".to_string())
        );
    }

    #[test]
    fn rejects_stale_exchange_event_even_when_locally_recent() {
        let mut book = book();
        book.exchange_timestamp_ms = 1;
        let decision = check_order(
            &Settings::default(),
            &market(false),
            Some(&book),
            &intent(),
            &state(),
        );
        assert_eq!(
            decision,
            RiskDecision::Reject("stale_market_event".to_string())
        );
    }

    #[test]
    fn rejects_book_receipt_timestamp_from_the_future() {
        let mut book = book();
        book.local_received_at_ms = state().now_ms + 1;
        let decision = check_order(
            &Settings::default(),
            &market(false),
            Some(&book),
            &intent(),
            &state(),
        );
        assert_eq!(
            decision,
            RiskDecision::Reject("book_received_in_future".to_string())
        );
    }

    #[test]
    fn rejects_exchange_timestamp_after_local_receipt() {
        let mut book = book();
        book.exchange_timestamp_ms = book.local_received_at_ms + 1;
        let decision = check_order(
            &Settings::default(),
            &market(false),
            Some(&book),
            &intent(),
            &state(),
        );
        assert_eq!(
            decision,
            RiskDecision::Reject("market_event_timestamp_after_receipt".to_string())
        );
    }

    #[test]
    fn rejects_non_post_only_until_impact_path_is_implemented() {
        let mut intent = intent();
        intent.post_only = false;
        let decision = check_order(
            &Settings::default(),
            &market(false),
            Some(&book()),
            &intent,
            &state(),
        );
        assert_eq!(
            decision,
            RiskDecision::Reject("non_post_only_execution_not_implemented".to_string())
        );
    }

    #[test]
    fn enforces_local_ttl_bound_and_v2_order_type_contract() {
        let settings = Settings::default();

        let mut too_long = intent();
        too_long.local_expires_at_ms = state().now_ms + settings.order_ttl_ms + 1;
        assert_eq!(
            check_order(
                &settings,
                &market(false),
                Some(&book()),
                &too_long,
                &state(),
            ),
            RiskDecision::Reject("intent_ttl_exceeds_configured_max".to_string())
        );

        let mut gtc_with_expiration = intent();
        gtc_with_expiration.wire_expiration_s = Some(100);
        assert_eq!(
            check_order(
                &settings,
                &market(false),
                Some(&book()),
                &gtc_with_expiration,
                &state(),
            ),
            RiskDecision::Reject("wire_expiration_requires_gtd".to_string())
        );

        let mut gtd = intent();
        gtd.time_in_force = TimeInForce::Gtd;
        assert_eq!(
            check_order(&settings, &market(false), Some(&book()), &gtd, &state(),),
            RiskDecision::Reject("gtd_requires_wire_expiration".to_string())
        );
        gtd.wire_expiration_s = Some(state().now_ms / 1_000 + 179);
        assert_eq!(
            check_order(&settings, &market(false), Some(&book()), &gtd, &state(),),
            RiskDecision::Reject("gtd_expiration_below_three_minute_minimum".to_string())
        );
        gtd.wire_expiration_s = Some(state().now_ms / 1_000 + 180);
        assert_eq!(
            check_order(&settings, &market(false), Some(&book()), &gtd, &state(),),
            RiskDecision::Accept
        );

        for order_type in [TimeInForce::Fok, TimeInForce::Fak] {
            let mut market_order = intent();
            market_order.time_in_force = order_type;
            assert_eq!(
                check_order(
                    &settings,
                    &market(false),
                    Some(&book()),
                    &market_order,
                    &state(),
                ),
                RiskDecision::Reject("post_only_requires_gtc_or_gtd".to_string())
            );

            market_order.wire_expiration_s = Some(100);
            assert_eq!(
                check_order(
                    &settings,
                    &market(false),
                    Some(&book()),
                    &market_order,
                    &state(),
                ),
                RiskDecision::Reject("wire_expiration_requires_gtd".to_string())
            );
        }
    }

    #[test]
    fn rejects_at_daily_loss_limit() {
        let state = RiskState {
            daily_loss_usdc: Settings::default().max_daily_loss_usdc,
            ..state()
        };
        let decision = check_order(
            &Settings::default(),
            &market(false),
            Some(&book()),
            &intent(),
            &state,
        );
        assert_eq!(
            decision,
            RiskDecision::Reject("daily_loss_limit".to_string())
        );
    }

    #[test]
    fn rejects_buy_above_available_cash() {
        let state = RiskState {
            available_cash_usdc: Some("0.999999".parse().unwrap()),
            ..state()
        };
        let decision = check_order(
            &Settings::default(),
            &market(false),
            Some(&book()),
            &intent(),
            &state,
        );
        assert_eq!(
            decision,
            RiskDecision::Reject("insufficient_available_cash".to_string())
        );
    }

    #[test]
    fn rejects_sell_above_available_position() {
        let mut order = intent();
        order.side = Side::Sell;
        order.limit_price = "0.5".parse().unwrap();
        let state = RiskState {
            available_position_size: Some("1".parse().unwrap()),
            ..state()
        };
        assert_eq!(
            check_order(
                &Settings::default(),
                &market(false),
                Some(&book()),
                &order,
                &state,
            ),
            RiskDecision::Reject("insufficient_available_position".to_string())
        );
    }

    #[test]
    fn notional_limit_rounds_sub_micro_remainder_up() {
        let settings = Settings {
            max_order_usdc: "0.499999".parse().unwrap(),
            ..Settings::default()
        };
        let mut market = market(false);
        market.tick_size = "0.000001".parse().unwrap();
        let mut book = book();
        book.tick_size = market.tick_size;
        let mut order = intent();
        order.limit_price = "0.499999".parse().unwrap();
        order.size = "1.000001".parse().unwrap();

        assert_eq!(
            check_order(&settings, &market, Some(&book), &order, &state()),
            RiskDecision::Reject("order_too_large".to_string())
        );
    }

    #[test]
    fn rejects_post_only_buy_equal_to_best_ask() {
        let mut intent = intent();
        intent.limit_price = "0.501".parse().unwrap();
        let decision = check_order(
            &Settings::default(),
            &market(false),
            Some(&book()),
            &intent,
            &state(),
        );
        assert_eq!(
            decision,
            RiskDecision::Reject("buy_crosses_best_ask".to_string())
        );
    }

    #[test]
    fn rejects_post_only_sell_equal_to_best_bid() {
        let mut intent = intent();
        intent.side = Side::Sell;
        intent.limit_price = "0.499".parse().unwrap();
        let decision = check_order(
            &Settings::default(),
            &market(false),
            Some(&book()),
            &intent,
            &state(),
        );
        assert_eq!(
            decision,
            RiskDecision::Reject("sell_crosses_best_bid".to_string())
        );
    }

    #[test]
    fn rejects_sell_pressure_near_behavioral_panic_threshold() {
        let mut intent = intent();
        intent.side = Side::Sell;
        intent.limit_price = "0.500".parse().unwrap();
        let state = RiskState {
            behavioral_pressure: Some(BehavioralPressure {
                drawdown_bps: -1480,
                zone: BehavioralZone::NearPanicThreshold,
                expected_flow: ExpectedFlow::ElevatedRetailSelling,
                expected_adverse_move_bps: -20,
            }),
            ..state()
        };
        let decision = check_order(
            &Settings::default(),
            &market(false),
            Some(&book()),
            &intent,
            &state,
        );
        assert_eq!(
            decision,
            RiskDecision::Reject("behavioral_price_pressure_prohibited".to_string())
        );
    }

    #[test]
    fn rejects_buy_when_behavioral_cascade_risk_exceeds_limit() {
        let state = RiskState {
            behavioral_pressure: Some(BehavioralPressure {
                drawdown_bps: -1500,
                zone: BehavioralZone::ThroughPanicThreshold,
                expected_flow: ExpectedFlow::PanicSelling,
                expected_adverse_move_bps: 150,
            }),
            ..state()
        };
        let decision = check_order(
            &Settings::default(),
            &market(false),
            Some(&book()),
            &intent(),
            &state,
        );
        assert_eq!(
            decision,
            RiskDecision::Reject("behavioral_cascade_risk".to_string())
        );
    }
}
