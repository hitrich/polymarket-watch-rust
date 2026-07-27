use crate::fixed::Fixed;
use crate::types::{BookState, OrderIntent, Side, TimeInForce};

pub trait Strategy {
    fn on_book(&mut self, book: &BookState, now_ms: u64) -> Option<OrderIntent>;
}

#[derive(Debug, Clone)]
pub struct ConservativeMaker {
    pub strategy_id: String,
    pub size: Fixed,
    pub min_spread: Fixed,
    pub ttl_ms: u64,
}

impl Strategy for ConservativeMaker {
    fn on_book(&mut self, book: &BookState, now_ms: u64) -> Option<OrderIntent> {
        let bid = book.best_bid?;
        let ask = book.best_ask?;
        let spread = ask.checked_sub(bid).ok()?;
        if spread < self.min_spread {
            return None;
        }
        let local_expires_at_ms = now_ms.checked_add(self.ttl_ms)?;
        let limit_price = bid.checked_add(book.tick_size).ok()?;
        Some(OrderIntent {
            asset_id: book.asset_id.clone(),
            side: Side::Buy,
            limit_price,
            size: self.size,
            time_in_force: TimeInForce::Gtc,
            post_only: true,
            local_expires_at_ms,
            wire_expiration_s: None,
            reason: "conservative_maker".to_string(),
            strategy_id: self.strategy_id.clone(),
            feature_snapshot_id: format!("book:{}", book.book_hash.clone().unwrap_or_default()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{AssetId, Level};

    #[test]
    fn strategy_emits_post_only_intent_only_on_wide_spread() {
        let mut book = BookState::empty(
            AssetId::from("a"),
            "0.001".parse().unwrap(),
            "1".parse().unwrap(),
        );
        book.bids.push(Level {
            price: "0.490".parse().unwrap(),
            size: "10".parse().unwrap(),
        });
        book.asks.push(Level {
            price: "0.510".parse().unwrap(),
            size: "10".parse().unwrap(),
        });
        book.best_bid = Some("0.490".parse().unwrap());
        book.best_ask = Some("0.510".parse().unwrap());
        book.book_hash = Some("h".to_string());
        let mut strategy = ConservativeMaker {
            strategy_id: "s".to_string(),
            size: "1".parse().unwrap(),
            min_spread: "0.010".parse().unwrap(),
            ttl_ms: 100,
        };
        let intent = strategy.on_book(&book, 10).unwrap();
        assert!(intent.post_only);
        assert_eq!(intent.limit_price.to_string(), "0.491");
    }

    #[test]
    fn strategy_drops_intent_when_expiry_overflows() {
        let mut book = BookState::empty(
            AssetId::from("a"),
            "0.001".parse().unwrap(),
            "1".parse().unwrap(),
        );
        book.best_bid = Some("0.490".parse().unwrap());
        book.best_ask = Some("0.510".parse().unwrap());
        let mut strategy = ConservativeMaker {
            strategy_id: "s".to_string(),
            size: "1".parse().unwrap(),
            min_spread: "0.010".parse().unwrap(),
            ttl_ms: 100,
        };
        assert!(strategy.on_book(&book, u64::MAX).is_none());
    }
}
