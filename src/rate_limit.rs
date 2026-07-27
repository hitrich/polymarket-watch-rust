use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct SlidingWindowRateLimiter {
    limit: usize,
    window_ms: u64,
    accepted_at_ms: VecDeque<u64>,
}

impl SlidingWindowRateLimiter {
    pub fn per_second(limit: u64) -> Self {
        Self::new(limit, 1_000)
    }

    pub fn new(limit: u64, window_ms: u64) -> Self {
        Self {
            limit: usize::try_from(limit).unwrap_or(usize::MAX),
            window_ms,
            accepted_at_ms: VecDeque::new(),
        }
    }

    pub fn try_acquire(&mut self, now_ms: u64) -> bool {
        self.evict_expired(now_ms);
        if self.limit == 0 || self.window_ms == 0 || self.accepted_at_ms.len() >= self.limit {
            return false;
        }
        self.accepted_at_ms.push_back(now_ms);
        true
    }

    pub fn retry_after_ms(&mut self, now_ms: u64) -> u64 {
        self.evict_expired(now_ms);
        if self.accepted_at_ms.len() < self.limit {
            return 0;
        }
        self.accepted_at_ms
            .front()
            .and_then(|oldest| oldest.checked_add(self.window_ms))
            .and_then(|ready_at| ready_at.checked_sub(now_ms))
            .unwrap_or(0)
    }

    fn evict_expired(&mut self, now_ms: u64) {
        while self
            .accepted_at_ms
            .front()
            .is_some_and(|timestamp| now_ms.saturating_sub(*timestamp) >= self.window_ms)
        {
            self.accepted_at_ms.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enforces_a_sliding_window_and_recovers_at_boundary() {
        let mut limiter = SlidingWindowRateLimiter::per_second(2);
        assert!(limiter.try_acquire(100));
        assert!(limiter.try_acquire(200));
        assert!(!limiter.try_acquire(1_099));
        assert_eq!(limiter.retry_after_ms(1_099), 1);
        assert!(limiter.try_acquire(1_100));
    }

    #[test]
    fn backwards_clock_does_not_evict_recent_entries() {
        let mut limiter = SlidingWindowRateLimiter::per_second(1);
        assert!(limiter.try_acquire(1_000));
        assert!(!limiter.try_acquire(999));
    }
}
