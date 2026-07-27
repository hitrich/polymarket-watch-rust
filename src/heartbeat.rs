use crate::error::{BotError, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatState {
    pub live_enabled: bool,
    pub consecutive_failures: u32,
    pub max_failures: u32,
    pub degraded: bool,
}

impl Default for HeartbeatState {
    fn default() -> Self {
        Self {
            live_enabled: false,
            consecutive_failures: 0,
            max_failures: 3,
            degraded: true,
        }
    }
}

impl HeartbeatState {
    pub fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.degraded = !self.live_enabled;
    }

    pub fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        if self.consecutive_failures >= self.max_failures {
            self.degraded = true;
        }
    }

    pub fn require_healthy(&self) -> Result<()> {
        if !self.live_enabled {
            return Err(BotError::Readiness(
                "heartbeat_not_live_enabled".to_string(),
            ));
        }
        if self.degraded {
            return Err(BotError::Readiness("heartbeat_degraded".to_string()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_failures_degrade_live_mode() {
        let mut state = HeartbeatState {
            live_enabled: true,
            degraded: false,
            max_failures: 2,
            ..HeartbeatState::default()
        };
        state.record_failure();
        assert!(!state.degraded);
        state.record_failure();
        assert!(state.require_healthy().is_err());
    }

    #[test]
    fn default_heartbeat_is_not_healthy() {
        assert!(HeartbeatState::default().require_healthy().is_err());
    }
}
