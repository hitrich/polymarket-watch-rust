use crate::error::{BotError, Result};
use crate::execution::ExecutionResult;
use crate::fixed::Fixed;
use crate::risk::RiskState;
use crate::types::{
    AssetId, BookState, OrderIntent, OrderState, OrderStatus, OwnRestingOrder, Side,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperPosition {
    pub asset_id: AssetId,
    pub size: Fixed,
    pub average_price: Fixed,
    pub realized_pnl_usdc: Fixed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperOrder {
    pub state: OrderState,
    pub filled_size: Fixed,
    pub remaining_size: Fixed,
    pub fees_paid_usdc: Fixed,
    #[serde(default)]
    pub last_book_version: Option<String>,
    #[serde(default)]
    pub last_trade_timestamp_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperFill {
    pub client_order_id: String,
    pub exchange_order_id: String,
    pub asset_id: AssetId,
    pub side: Side,
    pub price: Fixed,
    pub size: Fixed,
    pub notional_usdc: Fixed,
    pub fee_usdc: Fixed,
    pub filled_at_ms: u64,
    pub terminal: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperPortfolioSnapshot {
    pub starting_cash_usdc: Fixed,
    pub cash_usdc: Fixed,
    pub available_cash_usdc: Fixed,
    pub market_value_usdc: Fixed,
    pub equity_usdc: Fixed,
    pub realized_pnl_usdc: Fixed,
    pub unrealized_pnl_usdc: Fixed,
    pub fees_paid_usdc: Fixed,
    pub positions: Vec<PaperPosition>,
    pub open_orders: Vec<PaperOrder>,
}

pub const PAPER_TRANSITION_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperTransition {
    pub schema_version: u32,
    pub reason: String,
    pub committed_at_ms: u64,
    pub engine: PaperEngine,
    pub fills: Vec<PaperFill>,
    pub cancelled_order_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperEngine {
    starting_cash_usdc: Fixed,
    cash_usdc: Fixed,
    realized_pnl_usdc: Fixed,
    fees_paid_usdc: Fixed,
    fee_bps: u64,
    fill_participation_bps: u64,
    positions: BTreeMap<AssetId, PaperPosition>,
    orders: BTreeMap<String, PaperOrder>,
}

impl PaperEngine {
    pub fn new(
        starting_cash_usdc: Fixed,
        fee_bps: u64,
        fill_participation_bps: u64,
    ) -> Result<Self> {
        if starting_cash_usdc <= Fixed::ZERO {
            return Err(BotError::Execution(
                "paper_starting_cash_must_be_positive".to_string(),
            ));
        }
        if fee_bps > 10_000 || fill_participation_bps == 0 || fill_participation_bps > 10_000 {
            return Err(BotError::Execution(
                "paper_basis_points_out_of_range".to_string(),
            ));
        }
        Ok(Self {
            starting_cash_usdc,
            cash_usdc: starting_cash_usdc,
            realized_pnl_usdc: Fixed::ZERO,
            fees_paid_usdc: Fixed::ZERO,
            fee_bps,
            fill_participation_bps,
            positions: BTreeMap::new(),
            orders: BTreeMap::new(),
        })
    }

    pub fn configuration_matches(
        &self,
        starting_cash_usdc: Fixed,
        fee_bps: u64,
        fill_participation_bps: u64,
    ) -> bool {
        self.starting_cash_usdc == starting_cash_usdc
            && self.fee_bps == fee_bps
            && self.fill_participation_bps == fill_participation_bps
    }

    pub fn validate_state(&self) -> Result<()> {
        if self.starting_cash_usdc <= Fixed::ZERO
            || self.cash_usdc < Fixed::ZERO
            || self.fee_bps > 10_000
            || self.fill_participation_bps == 0
            || self.fill_participation_bps > 10_000
            || self.fees_paid_usdc < Fixed::ZERO
        {
            return Err(BotError::Journal(
                "paper_transition_engine_totals_invalid".to_string(),
            ));
        }
        for (asset_id, position) in &self.positions {
            if &position.asset_id != asset_id
                || position.size < Fixed::ZERO
                || position.average_price < Fixed::ZERO
                || position.average_price > Fixed::ONE
            {
                return Err(BotError::Journal(
                    "paper_transition_position_invalid".to_string(),
                ));
            }
        }
        for (client_order_id, order) in &self.orders {
            if &order.state.client_order_id != client_order_id
                || order.filled_size < Fixed::ZERO
                || order.remaining_size < Fixed::ZERO
                || order.fees_paid_usdc < Fixed::ZERO
                || order.filled_size.checked_add(order.remaining_size)? != order.state.intent.size
            {
                return Err(BotError::Journal(
                    "paper_transition_order_invalid".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub fn uses_only_assets(&self, configured: &[AssetId]) -> bool {
        self.positions
            .keys()
            .all(|asset_id| configured.contains(asset_id))
            && self
                .orders
                .values()
                .all(|order| configured.contains(&order.state.intent.asset_id))
    }

    pub fn submit(
        &mut self,
        intent: OrderIntent,
        client_order_id: impl Into<String>,
        now_ms: u64,
    ) -> Result<ExecutionResult> {
        let client_order_id = client_order_id.into();
        if self.orders.contains_key(&client_order_id) {
            return Err(BotError::Execution(
                "duplicate_paper_client_order_id".to_string(),
            ));
        }
        if intent.local_expires_at_ms <= now_ms {
            return Err(BotError::Execution("expired_paper_intent".to_string()));
        }
        let reserve = self.buy_reserve(intent.limit_price, intent.size)?;
        match intent.side {
            Side::Buy if reserve > self.available_cash_usdc()? => {
                return Err(BotError::Execution(
                    "paper_insufficient_available_cash".to_string(),
                ));
            }
            Side::Sell if intent.size > self.available_position(&intent.asset_id)? => {
                return Err(BotError::Execution(
                    "paper_insufficient_available_position".to_string(),
                ));
            }
            _ => {}
        }
        let exchange_order_id = format!("paper-{client_order_id}");
        let order = PaperOrder {
            state: OrderState {
                client_order_id: client_order_id.clone(),
                exchange_order_id: Some(exchange_order_id.clone()),
                intent: intent.clone(),
                status: OrderStatus::Acknowledged,
                submitted_at_ms: Some(now_ms),
                acknowledged_at_ms: Some(now_ms),
                matched_at_ms: None,
                settled_at_ms: None,
                cancel_requested_at_ms: None,
                terminal_reason: None,
            },
            filled_size: Fixed::ZERO,
            remaining_size: intent.size,
            fees_paid_usdc: Fixed::ZERO,
            last_book_version: None,
            last_trade_timestamp_ms: None,
        };
        self.orders.insert(client_order_id.clone(), order);
        Ok(ExecutionResult {
            client_order_id,
            exchange_order_id: Some(exchange_order_id),
            status: OrderStatus::Acknowledged,
            message: "paper_order_resting".to_string(),
        })
    }

    pub fn process_book(&mut self, book: &BookState, now_ms: u64) -> Result<Vec<PaperFill>> {
        self.process_book_with_cancellations(book, now_ms)
            .map(|(fills, _)| fills)
    }

    pub fn process_book_with_cancellations(
        &mut self,
        book: &BookState,
        now_ms: u64,
    ) -> Result<(Vec<PaperFill>, Vec<String>)> {
        let mut expired = Vec::new();
        let mut fill_plans = Vec::new();
        let mut buy_liquidity = if book.tradeable {
            participation_amount(book.top_ask_size(), self.fill_participation_bps)?
        } else {
            Fixed::ZERO
        };
        let mut sell_liquidity = if book.tradeable {
            participation_amount(book.top_bid_size(), self.fill_participation_bps)?
        } else {
            Fixed::ZERO
        };
        let version = format!(
            "{}:{}",
            book.exchange_timestamp_ms,
            book.book_hash.as_deref().unwrap_or_default()
        );
        for (client_order_id, order) in &mut self.orders {
            if !is_open(order.state.status) || order.state.intent.asset_id != book.asset_id {
                continue;
            }
            if order.state.intent.local_expires_at_ms <= now_ms {
                expired.push(client_order_id.clone());
                continue;
            }
            if !book.tradeable
                || book.book_hash.as_deref().is_none_or(str::is_empty)
                || order.last_book_version.as_deref() == Some(version.as_str())
            {
                continue;
            }
            order.last_book_version = Some(version.clone());
            if order_crosses_book(order, book) {
                let available = match order.state.intent.side {
                    Side::Buy => &mut buy_liquidity,
                    Side::Sell => &mut sell_liquidity,
                };
                let quantity = std::cmp::min(order.remaining_size, *available);
                if quantity > Fixed::ZERO {
                    fill_plans.push((client_order_id.clone(), quantity));
                    *available = available.checked_sub(quantity)?;
                }
            }
        }
        for client_order_id in &expired {
            self.cancel(client_order_id, now_ms, "paper_order_ttl_expired")?;
        }

        let mut fills = Vec::new();
        for (client_order_id, quantity) in fill_plans {
            fills.push(self.apply_fill(&client_order_id, quantity, now_ms)?);
        }
        Ok((fills, expired))
    }

    pub fn process_trade(
        &mut self,
        asset_id: &AssetId,
        trade_price: Fixed,
        trade_size: Option<Fixed>,
        aggressor_side: Option<Side>,
        exchange_timestamp_ms: u64,
        now_ms: u64,
    ) -> Result<Vec<PaperFill>> {
        if trade_price <= Fixed::ZERO || trade_price >= Fixed::ONE {
            return Err(BotError::Protocol(
                "paper_trade_price_out_of_range".to_string(),
            ));
        }
        let Some(trade_size) = trade_size.filter(|size| *size > Fixed::ZERO) else {
            return Ok(Vec::new());
        };
        let Some(aggressor_side) = aggressor_side else {
            return Ok(Vec::new());
        };
        let resting_side = match aggressor_side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        let mut simulated_liquidity =
            participation_amount(trade_size, self.fill_participation_bps)?;
        let mut plans = Vec::new();
        for (client_order_id, order) in &mut self.orders {
            if !is_open(order.state.status) || &order.state.intent.asset_id != asset_id {
                continue;
            }
            if order.state.intent.side != resting_side || simulated_liquidity.is_zero() {
                continue;
            }
            if order.state.intent.local_expires_at_ms <= now_ms {
                continue;
            }
            if order
                .state
                .submitted_at_ms
                .is_some_and(|submitted| exchange_timestamp_ms <= submitted)
                || order
                    .last_trade_timestamp_ms
                    .is_some_and(|last| exchange_timestamp_ms <= last)
            {
                continue;
            }
            order.last_trade_timestamp_ms = Some(exchange_timestamp_ms);
            let traded_through = match order.state.intent.side {
                Side::Buy => trade_price <= order.state.intent.limit_price,
                Side::Sell => trade_price >= order.state.intent.limit_price,
            };
            if traded_through {
                let quantity = std::cmp::min(order.remaining_size, simulated_liquidity);
                plans.push((client_order_id.clone(), quantity));
                simulated_liquidity = simulated_liquidity.checked_sub(quantity)?;
            }
        }
        let mut fills = Vec::new();
        for (client_order_id, quantity) in plans {
            if quantity > Fixed::ZERO {
                fills.push(self.apply_fill(&client_order_id, quantity, now_ms)?);
            }
        }
        Ok(fills)
    }

    pub fn cancel(&mut self, client_order_id: &str, now_ms: u64, reason: &str) -> Result<()> {
        let order = self
            .orders
            .get_mut(client_order_id)
            .ok_or_else(|| BotError::Execution("paper_order_not_found".to_string()))?;
        if !is_open(order.state.status) {
            return Ok(());
        }
        order.state.status = OrderStatus::Cancelled;
        order.state.cancel_requested_at_ms = Some(now_ms);
        order.state.settled_at_ms = Some(now_ms);
        order.state.terminal_reason = Some(reason.to_string());
        Ok(())
    }

    pub fn cancel_all(&mut self, now_ms: u64, reason: &str) -> usize {
        let ids = self
            .orders
            .iter()
            .filter(|(_, order)| is_open(order.state.status))
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in &ids {
            let _ = self.cancel(id, now_ms, reason);
        }
        ids.len()
    }

    pub fn cancel_expired(&mut self, now_ms: u64) -> Vec<String> {
        let ids = self
            .orders
            .iter()
            .filter(|(_, order)| {
                is_open(order.state.status) && order.state.intent.local_expires_at_ms <= now_ms
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in &ids {
            let _ = self.cancel(id, now_ms, "paper_order_ttl_expired");
        }
        ids
    }

    pub fn cancel_asset(&mut self, asset_id: &AssetId, now_ms: u64, reason: &str) -> Vec<String> {
        let ids = self
            .orders
            .iter()
            .filter(|(_, order)| {
                is_open(order.state.status) && &order.state.intent.asset_id == asset_id
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in &ids {
            let _ = self.cancel(id, now_ms, reason);
        }
        ids
    }

    pub fn prune_terminal_orders(&mut self) -> usize {
        let before = self.orders.len();
        self.orders.retain(|_, order| is_open(order.state.status));
        before.saturating_sub(self.orders.len())
    }

    pub fn flatten(
        &mut self,
        books: &BTreeMap<AssetId, BookState>,
        now_ms: u64,
        max_book_age_ms: u64,
        max_event_lag_ms: u64,
        max_slippage_ticks: u64,
    ) -> Result<Vec<PaperFill>> {
        let (staged, fills) = self.staged_flatten(
            books,
            now_ms,
            max_book_age_ms,
            max_event_lag_ms,
            max_slippage_ticks,
        )?;
        *self = staged;
        Ok(fills)
    }

    pub fn staged_flatten(
        &self,
        books: &BTreeMap<AssetId, BookState>,
        now_ms: u64,
        max_book_age_ms: u64,
        max_event_lag_ms: u64,
        max_slippage_ticks: u64,
    ) -> Result<(Self, Vec<PaperFill>)> {
        let mut staged = self.clone();
        let fills = staged.flatten_in_place(
            books,
            now_ms,
            max_book_age_ms,
            max_event_lag_ms,
            max_slippage_ticks,
        )?;
        Ok((staged, fills))
    }

    fn flatten_in_place(
        &mut self,
        books: &BTreeMap<AssetId, BookState>,
        now_ms: u64,
        max_book_age_ms: u64,
        max_event_lag_ms: u64,
        max_slippage_ticks: u64,
    ) -> Result<Vec<PaperFill>> {
        let positions = self
            .positions
            .values()
            .filter(|position| position.size > Fixed::ZERO)
            .cloned()
            .collect::<Vec<_>>();
        let mut plans = Vec::<(PaperPosition, String, Vec<(Fixed, Fixed)>)>::new();
        for position in positions {
            let book = books
                .get(&position.asset_id)
                .ok_or_else(|| BotError::Execution("paper_flatten_missing_book".to_string()))?;
            let hash = book
                .book_hash
                .as_deref()
                .filter(|hash| !hash.is_empty())
                .ok_or_else(|| BotError::Execution("paper_flatten_book_unverified".to_string()))?;
            if !book.tradeable
                || book.local_received_at_ms == 0
                || book.exchange_timestamp_ms > book.local_received_at_ms
                || book
                    .local_received_at_ms
                    .saturating_sub(book.exchange_timestamp_ms)
                    > max_event_lag_ms
                || book.local_received_at_ms > now_ms
                || now_ms.saturating_sub(book.local_received_at_ms) > max_book_age_ms
            {
                return Err(BotError::Execution(
                    "paper_flatten_book_not_fresh_tradeable".to_string(),
                ));
            }
            let best_bid = book
                .best_bid
                .ok_or_else(|| BotError::Execution("paper_flatten_missing_bid".to_string()))?;
            let slippage_raw = book
                .tick_size
                .raw()
                .checked_mul(i128::from(max_slippage_ticks))
                .ok_or_else(|| BotError::Risk("paper_flatten_slippage_overflow".to_string()))?;
            let minimum_price = std::cmp::max(
                best_bid.checked_sub(Fixed::from_scaled(slippage_raw))?,
                Fixed::ZERO,
            );
            let mut bids = book.bids.clone();
            bids.sort_by_key(|level| std::cmp::Reverse(level.price));
            let mut remaining = position.size;
            let mut clips = Vec::new();
            for level in bids {
                if remaining.is_zero() || level.price < minimum_price {
                    break;
                }
                let available = participation_amount(level.size, self.fill_participation_bps)?;
                let quantity = std::cmp::min(remaining, available);
                if quantity > Fixed::ZERO {
                    clips.push((level.price, quantity));
                    remaining = remaining.checked_sub(quantity)?;
                }
            }
            let executable_size = clips.iter().try_fold(Fixed::ZERO, |total, (_, quantity)| {
                total.checked_add(*quantity)
            })?;
            if executable_size < book.min_order_size {
                clips.clear();
            }
            plans.push((position, hash.to_string(), clips));
        }

        self.cancel_all(now_ms, "paper_flatten_cancel_open");
        let mut fills = Vec::new();
        for (position, book_hash, clips) in plans {
            if clips.is_empty() {
                continue;
            }
            let aggregate_size = clips.iter().try_fold(Fixed::ZERO, |total, (_, quantity)| {
                total.checked_add(*quantity)
            })?;
            let limit_price = clips
                .iter()
                .map(|(price, _)| *price)
                .min()
                .ok_or_else(|| BotError::Execution("paper_flatten_empty_plan".to_string()))?;
            let client_order_id = format!(
                "paper-flatten-{}-{now_ms}-{}",
                position.asset_id,
                uuid::Uuid::new_v4()
            );
            let intent = OrderIntent {
                asset_id: position.asset_id.clone(),
                side: Side::Sell,
                limit_price,
                size: aggregate_size,
                time_in_force: crate::types::TimeInForce::Fak,
                post_only: false,
                local_expires_at_ms: now_ms.saturating_add(1),
                wire_expiration_s: None,
                reason: "paper_flatten".to_string(),
                strategy_id: "operator".to_string(),
                feature_snapshot_id: book_hash.clone(),
            };
            let exchange_order_id = format!("paper-{client_order_id}");
            self.orders.insert(
                client_order_id.clone(),
                PaperOrder {
                    state: OrderState {
                        client_order_id: client_order_id.clone(),
                        exchange_order_id: Some(exchange_order_id),
                        intent,
                        status: OrderStatus::Acknowledged,
                        submitted_at_ms: Some(now_ms),
                        acknowledged_at_ms: Some(now_ms),
                        matched_at_ms: None,
                        settled_at_ms: None,
                        cancel_requested_at_ms: None,
                        terminal_reason: None,
                    },
                    filled_size: Fixed::ZERO,
                    remaining_size: aggregate_size,
                    fees_paid_usdc: Fixed::ZERO,
                    last_book_version: Some(book_hash),
                    last_trade_timestamp_ms: None,
                },
            );
            for (price, quantity) in clips {
                fills.push(self.apply_fill_at_price(&client_order_id, quantity, price, now_ms)?);
            }
        }
        Ok(fills)
    }

    fn apply_fill(
        &mut self,
        client_order_id: &str,
        quantity: Fixed,
        now_ms: u64,
    ) -> Result<PaperFill> {
        let price = self
            .orders
            .get(client_order_id)
            .ok_or_else(|| BotError::Execution("paper_order_not_found".to_string()))?
            .state
            .intent
            .limit_price;
        self.apply_fill_at_price(client_order_id, quantity, price, now_ms)
    }

    fn apply_fill_at_price(
        &mut self,
        client_order_id: &str,
        quantity: Fixed,
        price: Fixed,
        now_ms: u64,
    ) -> Result<PaperFill> {
        let (asset_id, side, limit_price, exchange_order_id, remaining_before) = {
            let order = self
                .orders
                .get(client_order_id)
                .ok_or_else(|| BotError::Execution("paper_order_not_found".to_string()))?;
            (
                order.state.intent.asset_id.clone(),
                order.state.intent.side,
                order.state.intent.limit_price,
                order.state.exchange_order_id.clone().unwrap_or_default(),
                order.remaining_size,
            )
        };
        if quantity <= Fixed::ZERO || quantity > remaining_before {
            return Err(BotError::Execution(
                "paper_fill_quantity_out_of_range".to_string(),
            ));
        }
        if price <= Fixed::ZERO
            || price >= Fixed::ONE
            || match side {
                Side::Buy => price > limit_price,
                Side::Sell => price < limit_price,
            }
        {
            return Err(BotError::Execution(
                "paper_fill_price_violates_limit".to_string(),
            ));
        }
        let notional = price.checked_mul_ceil(quantity)?;
        let fee = bps_amount(notional, self.fee_bps)?;
        match side {
            Side::Buy => self.apply_buy(&asset_id, quantity, price, notional, fee)?,
            Side::Sell => self.apply_sell(&asset_id, quantity, price, notional, fee)?,
        }
        let order = self.orders.get_mut(client_order_id).ok_or_else(|| {
            BotError::Execution("paper_order_disappeared_during_fill".to_string())
        })?;
        order.filled_size = order.filled_size.checked_add(quantity)?;
        order.remaining_size = order.remaining_size.checked_sub(quantity)?;
        order.fees_paid_usdc = order.fees_paid_usdc.checked_add(fee)?;
        order.state.matched_at_ms.get_or_insert(now_ms);
        let terminal = order.remaining_size.is_zero();
        order.state.status = if terminal {
            order.state.settled_at_ms = Some(now_ms);
            OrderStatus::Filled
        } else {
            OrderStatus::PartiallyFilled
        };
        Ok(PaperFill {
            client_order_id: client_order_id.to_string(),
            exchange_order_id,
            asset_id,
            side,
            price,
            size: quantity,
            notional_usdc: notional,
            fee_usdc: fee,
            filled_at_ms: now_ms,
            terminal,
        })
    }

    fn apply_buy(
        &mut self,
        asset_id: &AssetId,
        quantity: Fixed,
        price: Fixed,
        notional: Fixed,
        fee: Fixed,
    ) -> Result<()> {
        let total_cost = notional.checked_add(fee)?;
        if total_cost > self.cash_usdc {
            return Err(BotError::Execution(
                "paper_cash_changed_before_fill".to_string(),
            ));
        }
        self.cash_usdc = self.cash_usdc.checked_sub(total_cost)?;
        self.fees_paid_usdc = self.fees_paid_usdc.checked_add(fee)?;
        let position = self
            .positions
            .entry(asset_id.clone())
            .or_insert(PaperPosition {
                asset_id: asset_id.clone(),
                size: Fixed::ZERO,
                average_price: Fixed::ZERO,
                realized_pnl_usdc: Fixed::ZERO,
            });
        let existing_cost = position.average_price.checked_mul(position.size)?;
        let new_size = position.size.checked_add(quantity)?;
        let new_cost = existing_cost
            .checked_add(price.checked_mul(quantity)?)?
            .checked_add(fee)?;
        position.average_price = new_cost.checked_div(new_size)?;
        position.size = new_size;
        Ok(())
    }

    fn apply_sell(
        &mut self,
        asset_id: &AssetId,
        quantity: Fixed,
        _price: Fixed,
        notional: Fixed,
        fee: Fixed,
    ) -> Result<()> {
        let position = self
            .positions
            .get_mut(asset_id)
            .ok_or_else(|| BotError::Execution("paper_sell_without_position".to_string()))?;
        if quantity > position.size {
            return Err(BotError::Execution(
                "paper_sell_exceeds_position".to_string(),
            ));
        }
        let cost_basis = position.average_price.checked_mul(quantity)?;
        let proceeds = notional.checked_sub(fee)?;
        let realized = proceeds.checked_sub(cost_basis)?;
        self.cash_usdc = self.cash_usdc.checked_add(proceeds)?;
        self.realized_pnl_usdc = self.realized_pnl_usdc.checked_add(realized)?;
        self.fees_paid_usdc = self.fees_paid_usdc.checked_add(fee)?;
        position.realized_pnl_usdc = position.realized_pnl_usdc.checked_add(realized)?;
        position.size = position.size.checked_sub(quantity)?;
        if position.size.is_zero() {
            position.average_price = Fixed::ZERO;
        }
        Ok(())
    }

    pub fn available_cash_usdc(&self) -> Result<Fixed> {
        let reserved = self
            .orders
            .values()
            .filter(|order| is_open(order.state.status) && order.state.intent.side == Side::Buy)
            .try_fold(Fixed::ZERO, |total, order| {
                total.checked_add(
                    self.buy_reserve(order.state.intent.limit_price, order.remaining_size)?,
                )
            })?;
        let available = self.cash_usdc.checked_sub(reserved)?;
        if available < Fixed::ZERO {
            return Err(BotError::Execution(
                "paper_reserved_cash_invariant_broken".to_string(),
            ));
        }
        Ok(available)
    }

    fn available_position(&self, asset_id: &AssetId) -> Result<Fixed> {
        let held = self
            .positions
            .get(asset_id)
            .map(|position| position.size)
            .unwrap_or(Fixed::ZERO);
        let reserved = self
            .orders
            .values()
            .filter(|order| {
                is_open(order.state.status)
                    && order.state.intent.side == Side::Sell
                    && &order.state.intent.asset_id == asset_id
            })
            .try_fold(Fixed::ZERO, |total, order| {
                total.checked_add(order.remaining_size)
            })?;
        let available = held.checked_sub(reserved)?;
        if available < Fixed::ZERO {
            return Err(BotError::Execution(
                "paper_reserved_position_invariant_broken".to_string(),
            ));
        }
        Ok(available)
    }

    pub fn snapshot(&self, books: &BTreeMap<AssetId, BookState>) -> Result<PaperPortfolioSnapshot> {
        let mut market_value = Fixed::ZERO;
        let mut unrealized = Fixed::ZERO;
        for position in self.positions.values() {
            let mark = books
                .get(&position.asset_id)
                .and_then(|book| book.best_bid.or(book.last_trade_price))
                .unwrap_or(position.average_price);
            let value = mark.checked_mul(position.size)?;
            market_value = market_value.checked_add(value)?;
            unrealized = unrealized.checked_add(
                mark.checked_sub(position.average_price)?
                    .checked_mul(position.size)?,
            )?;
        }
        Ok(PaperPortfolioSnapshot {
            starting_cash_usdc: self.starting_cash_usdc,
            cash_usdc: self.cash_usdc,
            available_cash_usdc: self.available_cash_usdc()?,
            market_value_usdc: market_value,
            equity_usdc: self.cash_usdc.checked_add(market_value)?,
            realized_pnl_usdc: self.realized_pnl_usdc,
            unrealized_pnl_usdc: unrealized,
            fees_paid_usdc: self.fees_paid_usdc,
            positions: self.positions.values().cloned().collect(),
            open_orders: self
                .orders
                .values()
                .filter(|order| is_open(order.state.status))
                .cloned()
                .collect(),
        })
    }

    pub fn risk_state(
        &self,
        now_ms: u64,
        market_asset: &AssetId,
        books: &BTreeMap<AssetId, BookState>,
        prohibited_conduct_flag: bool,
    ) -> Result<RiskState> {
        let snapshot = self.snapshot(books)?;
        let market_position = self.positions.get(market_asset);
        let market_mark = books
            .get(market_asset)
            .and_then(|book| book.best_bid.or(book.last_trade_price))
            .unwrap_or(Fixed::ZERO);
        let position_market_exposure = market_position
            .map(|position| market_mark.checked_mul_ceil(position.size))
            .transpose()?
            .unwrap_or(Fixed::ZERO);
        let pending_market_exposure = self.pending_buy_notional(Some(market_asset))?;
        let market_exposure = position_market_exposure.checked_add(pending_market_exposure)?;
        let position_total_exposure =
            self.positions
                .values()
                .try_fold(Fixed::ZERO, |total, position| {
                    let mark = books
                        .get(&position.asset_id)
                        .and_then(|book| book.best_bid.or(book.last_trade_price))
                        .unwrap_or(position.average_price);
                    total.checked_add(mark.checked_mul_ceil(position.size)?)
                })?;
        let total_exposure =
            position_total_exposure.checked_add(self.pending_buy_notional(None)?)?;
        let daily_loss = if snapshot.equity_usdc < self.starting_cash_usdc {
            self.starting_cash_usdc.checked_sub(snapshot.equity_usdc)?
        } else {
            Fixed::ZERO
        };
        let own_resting_orders = self
            .orders
            .values()
            .filter(|order| is_open(order.state.status))
            .map(|order| OwnRestingOrder {
                asset_id: order.state.intent.asset_id.clone(),
                side: order.state.intent.side,
                price: order.state.intent.limit_price,
                size: order.remaining_size,
            })
            .collect();
        Ok(RiskState {
            now_ms,
            market_exposure_usdc: market_exposure,
            total_exposure_usdc: total_exposure,
            daily_loss_usdc: daily_loss,
            available_cash_usdc: Some(snapshot.available_cash_usdc),
            available_position_size: Some(self.available_position(market_asset)?),
            own_resting_orders,
            own_order_cache_certain: true,
            prohibited_conduct_flag,
            behavioral_pressure: None,
        })
    }

    pub fn orders(&self) -> impl Iterator<Item = &PaperOrder> {
        self.orders.values()
    }

    pub fn has_open_order(&self, asset_id: &AssetId, strategy_id: &str) -> bool {
        self.orders.values().any(|order| {
            is_open(order.state.status)
                && &order.state.intent.asset_id == asset_id
                && order.state.intent.strategy_id == strategy_id
        })
    }

    pub fn open_order_ids(&self) -> Vec<String> {
        self.orders
            .iter()
            .filter(|(_, order)| is_open(order.state.status))
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub fn nonzero_position_count(&self) -> usize {
        self.positions
            .values()
            .filter(|position| position.size > Fixed::ZERO)
            .count()
    }

    fn buy_reserve(&self, price: Fixed, size: Fixed) -> Result<Fixed> {
        let notional = price.checked_mul_ceil(size)?;
        notional.checked_add(bps_amount(notional, self.fee_bps)?)
    }

    fn pending_buy_notional(&self, asset_filter: Option<&AssetId>) -> Result<Fixed> {
        self.orders
            .values()
            .filter(|order| {
                is_open(order.state.status)
                    && order.state.intent.side == Side::Buy
                    && asset_filter.is_none_or(|asset| &order.state.intent.asset_id == asset)
            })
            .try_fold(Fixed::ZERO, |total, order| {
                total.checked_add(
                    order
                        .state
                        .intent
                        .limit_price
                        .checked_mul_ceil(order.remaining_size)?,
                )
            })
    }
}

fn order_crosses_book(order: &PaperOrder, book: &BookState) -> bool {
    let intent = &order.state.intent;
    match intent.side {
        Side::Buy => book.best_ask.is_some_and(|ask| ask <= intent.limit_price),
        Side::Sell => book.best_bid.is_some_and(|bid| bid >= intent.limit_price),
    }
}

fn participation_amount(liquidity: Fixed, fill_participation_bps: u64) -> Result<Fixed> {
    liquidity.checked_mul(Fixed::from_scaled(i128::from(fill_participation_bps) * 100))
}

fn bps_amount(value: Fixed, bps: u64) -> Result<Fixed> {
    if bps == 0 {
        return Ok(Fixed::ZERO);
    }
    value.checked_mul_ceil(Fixed::from_scaled(i128::from(bps) * 100))
}

fn is_open(status: OrderStatus) -> bool {
    matches!(
        status,
        OrderStatus::Submitted | OrderStatus::Acknowledged | OrderStatus::PartiallyFilled
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Level, TimeInForce};

    fn intent(side: Side) -> OrderIntent {
        OrderIntent {
            asset_id: AssetId::from("1"),
            side,
            limit_price: "0.5".parse().unwrap(),
            size: "4".parse().unwrap(),
            time_in_force: TimeInForce::Gtc,
            post_only: true,
            local_expires_at_ms: 2_000,
            wire_expiration_s: None,
            reason: "test".to_string(),
            strategy_id: "paper-test".to_string(),
            feature_snapshot_id: "book-1".to_string(),
        }
    }

    fn book(last_trade: Option<&str>) -> BookState {
        let mut book = BookState::empty("1", "0.01".parse().unwrap(), Fixed::ONE);
        book.bids.push(Level {
            price: "0.49".parse().unwrap(),
            size: "10".parse().unwrap(),
        });
        book.asks.push(Level {
            price: "0.51".parse().unwrap(),
            size: "10".parse().unwrap(),
        });
        book.best_bid = Some("0.49".parse().unwrap());
        book.best_ask = Some("0.51".parse().unwrap());
        book.last_trade_price = last_trade.map(|value| value.parse().unwrap());
        book.book_hash = Some(format!("book-{}", last_trade.unwrap_or("none")));
        book.exchange_timestamp_ms = if last_trade.is_some() { 1_200 } else { 1_100 };
        book.local_received_at_ms = book.exchange_timestamp_ms;
        book.tradeable = true;
        book
    }

    #[test]
    fn resting_order_only_fills_after_trade_through() {
        let mut engine = PaperEngine::new("100".parse().unwrap(), 0, 5_000).unwrap();
        let result = engine.submit(intent(Side::Buy), "c1", 1_000).unwrap();
        assert_eq!(result.status, OrderStatus::Acknowledged);
        assert!(engine.process_book(&book(None), 1_100).unwrap().is_empty());
        let fills = engine
            .process_trade(
                &AssetId::from("1"),
                "0.50".parse().unwrap(),
                Some("4".parse().unwrap()),
                Some(Side::Sell),
                1_200,
                1_200,
            )
            .unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].size.to_string(), "2");
        assert!(!fills[0].terminal);
        assert!(engine
            .process_trade(
                &AssetId::from("1"),
                "0.50".parse().unwrap(),
                Some("4".parse().unwrap()),
                Some(Side::Sell),
                1_200,
                1_201,
            )
            .unwrap()
            .is_empty());
    }

    #[test]
    fn completed_buy_updates_cash_position_and_fees() {
        let mut engine = PaperEngine::new("100".parse().unwrap(), 100, 10_000).unwrap();
        engine.submit(intent(Side::Buy), "c1", 1_000).unwrap();
        let fills = engine
            .process_trade(
                &AssetId::from("1"),
                "0.50".parse().unwrap(),
                Some("4".parse().unwrap()),
                Some(Side::Sell),
                1_100,
                1_100,
            )
            .unwrap();
        assert!(fills[0].terminal);
        let snapshot = engine.snapshot(&BTreeMap::new()).unwrap();
        assert_eq!(snapshot.positions[0].size.to_string(), "4");
        assert_eq!(snapshot.cash_usdc.to_string(), "97.98");
        assert_eq!(snapshot.fees_paid_usdc.to_string(), "0.02");
    }

    #[test]
    fn one_trade_participation_budget_is_shared_across_orders() {
        let mut engine = PaperEngine::new("100".parse().unwrap(), 0, 5_000).unwrap();
        engine.submit(intent(Side::Buy), "c1", 1_000).unwrap();
        engine.submit(intent(Side::Buy), "c2", 1_000).unwrap();
        let fills = engine
            .process_trade(
                &AssetId::from("1"),
                "0.50".parse().unwrap(),
                Some("4".parse().unwrap()),
                Some(Side::Sell),
                1_100,
                1_100,
            )
            .unwrap();
        assert_eq!(
            fills
                .iter()
                .try_fold(Fixed::ZERO, |total, fill| total.checked_add(fill.size))
                .unwrap()
                .to_string(),
            "2"
        );
    }

    #[test]
    fn trade_without_aggressor_side_does_not_invent_a_fill() {
        let mut engine = PaperEngine::new("100".parse().unwrap(), 0, 10_000).unwrap();
        engine.submit(intent(Side::Buy), "c1", 1_000).unwrap();
        assert!(engine
            .process_trade(
                &AssetId::from("1"),
                "0.50".parse().unwrap(),
                Some("4".parse().unwrap()),
                None,
                1_100,
                1_100,
            )
            .unwrap()
            .is_empty());
    }

    #[test]
    fn sell_requires_an_unreserved_position() {
        let mut engine = PaperEngine::new("100".parse().unwrap(), 0, 10_000).unwrap();
        let error = engine.submit(intent(Side::Sell), "c1", 1_000).unwrap_err();
        assert!(error.to_string().contains("position"));
    }

    #[test]
    fn ttl_expiration_cancels_without_fill() {
        let mut engine = PaperEngine::new("100".parse().unwrap(), 0, 10_000).unwrap();
        engine.submit(intent(Side::Buy), "c1", 1_000).unwrap();
        assert!(engine.process_book(&book(None), 2_000).unwrap().is_empty());
        assert_eq!(
            engine.orders().next().unwrap().state.status,
            OrderStatus::Cancelled
        );
    }

    #[test]
    fn open_buys_reserve_cash() {
        let mut engine = PaperEngine::new("3".parse().unwrap(), 0, 10_000).unwrap();
        engine.submit(intent(Side::Buy), "c1", 1_000).unwrap();
        assert_eq!(engine.available_cash_usdc().unwrap().to_string(), "1");
        assert!(engine.submit(intent(Side::Buy), "c2", 1_000).is_err());
    }

    #[test]
    fn open_buys_are_included_in_projected_exposure() {
        let mut engine = PaperEngine::new("100".parse().unwrap(), 0, 10_000).unwrap();
        engine.submit(intent(Side::Buy), "c1", 1_000).unwrap();
        let state = engine
            .risk_state(1_100, &AssetId::from("1"), &BTreeMap::new(), true)
            .unwrap();
        assert_eq!(state.market_exposure_usdc.to_string(), "2");
        assert_eq!(state.total_exposure_usdc.to_string(), "2");
        assert!(state.prohibited_conduct_flag);
    }

    #[test]
    fn fee_is_reserved_before_buy_can_rest() {
        let mut engine = PaperEngine::new("2".parse().unwrap(), 100, 10_000).unwrap();
        let error = engine.submit(intent(Side::Buy), "c1", 1_000).unwrap_err();
        assert!(error.to_string().contains("cash"));
    }

    #[test]
    fn flatten_failure_leaves_orders_and_accounting_unchanged() {
        let mut engine = PaperEngine::new("100".parse().unwrap(), 0, 10_000).unwrap();
        engine.submit(intent(Side::Buy), "open", 1_000).unwrap();
        for asset in ["1", "2"] {
            engine.positions.insert(
                AssetId::from(asset),
                PaperPosition {
                    asset_id: AssetId::from(asset),
                    size: "1".parse().unwrap(),
                    average_price: "0.4".parse().unwrap(),
                    realized_pnl_usdc: Fixed::ZERO,
                },
            );
        }
        let before = engine.clone();
        let mut books = BTreeMap::new();
        books.insert(AssetId::from("1"), book(None));

        let error = engine.flatten(&books, 1_500, 1_000, 500, 2).unwrap_err();

        assert!(error.to_string().contains("missing_book"));
        assert_eq!(engine, before);
    }

    #[test]
    fn flatten_rejects_stale_or_last_trade_only_books() {
        let mut engine = PaperEngine::new("100".parse().unwrap(), 0, 10_000).unwrap();
        engine.positions.insert(
            AssetId::from("1"),
            PaperPosition {
                asset_id: AssetId::from("1"),
                size: "2".parse().unwrap(),
                average_price: "0.4".parse().unwrap(),
                realized_pnl_usdc: Fixed::ZERO,
            },
        );
        let before = engine.clone();
        let stale = [(AssetId::from("1"), book(Some("0.5")))]
            .into_iter()
            .collect();
        assert!(engine.flatten(&stale, 2_000, 250, 500, 2).is_err());
        assert_eq!(engine, before);

        let mut exchange_stale = book(None);
        exchange_stale.exchange_timestamp_ms = 1_000;
        exchange_stale.local_received_at_ms = 1_190;
        let delayed = [(AssetId::from("1"), exchange_stale)].into_iter().collect();
        assert!(engine.flatten(&delayed, 1_200, 250, 100, 2).is_err());
        assert_eq!(engine, before);

        let mut last_trade_only = book(Some("0.5"));
        last_trade_only.bids.clear();
        last_trade_only.best_bid = None;
        let books = [(AssetId::from("1"), last_trade_only)]
            .into_iter()
            .collect();
        assert!(engine.flatten(&books, 1_200, 250, 500, 2).is_err());
        assert_eq!(engine, before);
    }

    #[test]
    fn flatten_consumes_only_visible_participating_bid_liquidity() {
        let mut engine = PaperEngine::new("100".parse().unwrap(), 0, 10_000).unwrap();
        engine.positions.insert(
            AssetId::from("1"),
            PaperPosition {
                asset_id: AssetId::from("1"),
                size: "100".parse().unwrap(),
                average_price: "0.4".parse().unwrap(),
                realized_pnl_usdc: Fixed::ZERO,
            },
        );
        let mut shallow = book(None);
        shallow.bids[0].size = "1".parse().unwrap();
        let books = [(AssetId::from("1"), shallow)].into_iter().collect();

        let fills = engine.flatten(&books, 1_100, 250, 500, 2).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].size.to_string(), "1");
        assert_eq!(engine.positions[&AssetId::from("1")].size.to_string(), "99");
        assert_eq!(engine.nonzero_position_count(), 1);
    }

    #[test]
    fn flatten_leaves_a_subminimum_residual_without_synthetic_order() {
        let mut engine = PaperEngine::new("100".parse().unwrap(), 0, 10_000).unwrap();
        engine.positions.insert(
            AssetId::from("1"),
            PaperPosition {
                asset_id: AssetId::from("1"),
                size: "0.5".parse().unwrap(),
                average_price: "0.4".parse().unwrap(),
                realized_pnl_usdc: Fixed::ZERO,
            },
        );
        let books = [(AssetId::from("1"), book(None))].into_iter().collect();

        let fills = engine.flatten(&books, 1_100, 250, 500, 2).unwrap();

        assert!(fills.is_empty());
        assert_eq!(
            engine.positions[&AssetId::from("1")].size.to_string(),
            "0.5"
        );
        assert_eq!(engine.orders().count(), 0);
    }

    #[test]
    fn flatten_aggregates_depth_into_one_valid_fak_and_keeps_execution_prices() {
        let mut engine = PaperEngine::new("100".parse().unwrap(), 0, 10_000).unwrap();
        engine.positions.insert(
            AssetId::from("1"),
            PaperPosition {
                asset_id: AssetId::from("1"),
                size: "1.2".parse().unwrap(),
                average_price: "0.4".parse().unwrap(),
                realized_pnl_usdc: Fixed::ZERO,
            },
        );
        let mut depth = book(None);
        depth.bids = vec![
            Level {
                price: "0.49".parse().unwrap(),
                size: "0.6".parse().unwrap(),
            },
            Level {
                price: "0.48".parse().unwrap(),
                size: "0.6".parse().unwrap(),
            },
        ];
        depth.best_bid = Some("0.49".parse().unwrap());
        let books = [(AssetId::from("1"), depth)].into_iter().collect();

        let fills = engine.flatten(&books, 1_100, 250, 500, 2).unwrap();

        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].client_order_id, fills[1].client_order_id);
        assert_eq!(fills[0].price.to_string(), "0.49");
        assert_eq!(fills[1].price.to_string(), "0.48");
        assert!(!fills[0].terminal);
        assert!(fills[1].terminal);
        let order = engine.orders().next().unwrap();
        assert_eq!(order.state.intent.size.to_string(), "1.2");
        assert_eq!(order.state.intent.limit_price.to_string(), "0.48");
        assert_eq!(engine.nonzero_position_count(), 0);
    }
}
