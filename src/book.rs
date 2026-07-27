use crate::fixed::Fixed;
use crate::types::{BookState, Level, Side};
use std::cmp::Reverse;

pub fn sort_and_refresh_top(book: &mut BookState) {
    book.bids.sort_by_key(|level| Reverse(level.price));
    book.asks.sort_by_key(|level| level.price);
    book.best_bid = book.bids.first().map(|v| v.price);
    book.best_ask = book.asks.first().map(|v| v.price);
}

pub fn upsert_level(levels: &mut Vec<Level>, price: Fixed, size: Fixed) {
    if size.is_zero() {
        levels.retain(|level| level.price != price);
        return;
    }
    if let Some(level) = levels.iter_mut().find(|level| level.price == price) {
        level.size = size;
    } else {
        levels.push(Level { price, size });
    }
}

pub fn apply_level_change(book: &mut BookState, side: Side, price: Fixed, size: Fixed) {
    match side {
        Side::Buy => upsert_level(&mut book.bids, price, size),
        Side::Sell => upsert_level(&mut book.asks, price, size),
    }
    sort_and_refresh_top(book);
}

pub fn mark_not_tradeable(book: &mut BookState) {
    book.tradeable = false;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BookState;

    #[test]
    fn size_zero_deletes_level() {
        let mut book = BookState::empty("asset", "0.001".parse().unwrap(), "5".parse().unwrap());
        apply_level_change(
            &mut book,
            Side::Buy,
            "0.5".parse().unwrap(),
            "10".parse().unwrap(),
        );
        assert_eq!(book.top_bid_size().to_string(), "10");
        apply_level_change(&mut book, Side::Buy, "0.5".parse().unwrap(), Fixed::ZERO);
        assert!(book.bids.is_empty());
    }
}
