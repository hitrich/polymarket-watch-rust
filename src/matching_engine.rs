use crate::types::OrderStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchingEngineMode {
    Normal,
    Restarting,
    PostOnlyRecovery,
    CancelOnly,
    PostOnly,
    ReadOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingEngineState {
    pub mode: MatchingEngineMode,
    pub retry_after_ms: Option<u64>,
    pub non_post_only_retry_allowed: bool,
}

impl Default for MatchingEngineState {
    fn default() -> Self {
        Self {
            mode: MatchingEngineMode::ReadOnly,
            retry_after_ms: None,
            non_post_only_retry_allowed: false,
        }
    }
}

impl MatchingEngineState {
    pub fn normal_after_verified_startup() -> Self {
        Self {
            mode: MatchingEngineMode::Normal,
            retry_after_ms: None,
            non_post_only_retry_allowed: false,
        }
    }
}

pub fn classify_http_status(status: u16, retry_after_ms: Option<u64>) -> MatchingEngineState {
    match status {
        200..=299 => MatchingEngineState::normal_after_verified_startup(),
        425 => MatchingEngineState {
            mode: MatchingEngineMode::Restarting,
            retry_after_ms,
            non_post_only_retry_allowed: false,
        },
        503 => MatchingEngineState {
            mode: MatchingEngineMode::ReadOnly,
            retry_after_ms,
            non_post_only_retry_allowed: false,
        },
        _ => MatchingEngineState {
            mode: MatchingEngineMode::ReadOnly,
            retry_after_ms,
            non_post_only_retry_allowed: false,
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchOrderEntry {
    pub success: bool,
    pub order_id: String,
    pub error_msg: String,
}

impl BatchOrderEntry {
    pub fn status(&self) -> OrderStatus {
        if self.success && !self.order_id.is_empty() && self.error_msg.is_empty() {
            OrderStatus::Acknowledged
        } else if self.success {
            OrderStatus::Unknown
        } else {
            OrderStatus::Rejected
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_425_enters_restart_no_blind_retry() {
        let state = classify_http_status(425, Some(1000));
        assert_eq!(state.mode, MatchingEngineMode::Restarting);
        assert!(!state.non_post_only_retry_allowed);
    }

    #[test]
    fn default_matching_engine_state_is_read_only() {
        assert_eq!(
            MatchingEngineState::default().mode,
            MatchingEngineMode::ReadOnly
        );
    }

    #[test]
    fn unknown_or_error_statuses_fail_closed() {
        assert_eq!(
            classify_http_status(401, None).mode,
            MatchingEngineMode::ReadOnly
        );
        assert_eq!(
            classify_http_status(429, Some(1000)).mode,
            MatchingEngineMode::ReadOnly
        );
        assert_eq!(
            classify_http_status(500, None).mode,
            MatchingEngineMode::ReadOnly
        );
        assert_eq!(
            classify_http_status(204, None).mode,
            MatchingEngineMode::Normal
        );
    }

    #[test]
    fn success_with_empty_order_id_is_unknown() {
        let entry = BatchOrderEntry {
            success: true,
            order_id: String::new(),
            error_msg: "post only mode".to_string(),
        };
        assert_eq!(entry.status(), OrderStatus::Unknown);
    }
}
