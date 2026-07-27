use crate::error::{BotError, Result};
use crate::fixed::Fixed;
use crate::journal::{replay_records, JournalEventKind, JournalRecord};
use crate::types::{AssetId, ConditionId, Side};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RemoteSnapshot {
    pub open_orders_loaded: bool,
    pub trades_loaded: bool,
    pub balances_loaded: bool,
    pub allowances_loaded: bool,
    pub open_order_ids: BTreeSet<String>,
    pub trade_order_ids: BTreeSet<String>,
    pub trades: BTreeMap<String, AuthoritativeTrade>,
    pub unresolved_gaps: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoritativeTrade {
    pub trade_id: String,
    pub condition_id: ConditionId,
    pub asset_id: AssetId,
    pub side: Side,
    pub price: Fixed,
    pub size: Fixed,
    pub fee_rate_bps: Fixed,
    pub status: String,
    pub order_ids: BTreeSet<String>,
    pub timestamp_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryReport {
    pub live_unlock_allowed: bool,
    pub reason: String,
    pub replayed_records: usize,
    pub in_flight_attempts: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredOpenOrder {
    pub client_order_id: String,
    pub submitted_at_ms: u64,
    pub asset_id: AssetId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnresolvedOrderAttempt {
    pub decision_id: String,
    pub client_order_id: String,
    pub exchange_order_id: String,
    pub strategy_id: String,
}

pub fn unresolved_order_attempts(records: &[JournalRecord]) -> Vec<UnresolvedOrderAttempt> {
    group_attempts(records)
        .into_iter()
        .filter(|(_, attempt)| !attempt.iter().any(|record| record.is_terminal()))
        .filter_map(|(client_order_id, attempt)| {
            let exchange_order_id = attempt
                .iter()
                .rev()
                .find_map(|record| record.exchange_order_id.clone())?;
            let identity = attempt.first()?;
            Some(UnresolvedOrderAttempt {
                decision_id: identity.decision_id.clone(),
                client_order_id: client_order_id.to_string(),
                exchange_order_id,
                strategy_id: identity.strategy_id.clone(),
            })
        })
        .collect()
}

pub fn recover_after_crash(
    journal_path: impl AsRef<Path>,
    remote: &RemoteSnapshot,
) -> Result<RecoveryReport> {
    let records = replay_records(journal_path)?;
    recover_after_crash_records(&records, remote)
}

pub fn recover_after_crash_records(
    records: &[JournalRecord],
    remote: &RemoteSnapshot,
) -> Result<RecoveryReport> {
    let attempts = group_attempts(records);
    let in_flight_attempts = attempts
        .values()
        .filter(|records| !records.iter().any(|record| record.is_terminal()))
        .count();
    if !remote.open_orders_loaded {
        return Ok(blocked(
            "open_orders_not_loaded",
            records.len(),
            in_flight_attempts,
        ));
    }
    if !remote.trades_loaded {
        return Ok(blocked(
            "trades_not_loaded",
            records.len(),
            in_flight_attempts,
        ));
    }
    if !remote.balances_loaded {
        return Ok(blocked(
            "balances_not_loaded",
            records.len(),
            in_flight_attempts,
        ));
    }
    if !remote.allowances_loaded {
        return Ok(blocked(
            "allowances_not_loaded",
            records.len(),
            in_flight_attempts,
        ));
    }
    if !remote.unresolved_gaps.is_empty() {
        return Ok(blocked(
            "unresolved_reconciliation_gaps",
            records.len(),
            in_flight_attempts,
        ));
    }
    let unresolved_attempts = attempts
        .values()
        .filter(|records| !records.iter().any(|record| record.is_terminal()))
        .filter(|records| !remote_contains_attempt(records, remote))
        .count();
    if unresolved_attempts > 0 {
        return Ok(blocked(
            "unresolved_in_flight_attempts",
            records.len(),
            in_flight_attempts,
        ));
    }
    let mapped_open_orders = attempts
        .values()
        .filter(|records| !records.iter().any(|record| record.is_terminal()))
        .flat_map(|records| {
            records
                .iter()
                .filter_map(|record| record.exchange_order_id.as_ref())
        })
        .filter(|order_id| remote.open_order_ids.contains(*order_id))
        .collect::<BTreeSet<_>>();
    if remote
        .open_order_ids
        .iter()
        .any(|order_id| !mapped_open_orders.contains(order_id))
    {
        return Ok(blocked(
            "unmapped_remote_open_orders",
            records.len(),
            in_flight_attempts,
        ));
    }
    Ok(RecoveryReport {
        live_unlock_allowed: true,
        reason: "reconciled".to_string(),
        replayed_records: records.len(),
        in_flight_attempts,
    })
}

pub fn recover_open_order_mappings(
    journal_path: impl AsRef<Path>,
    remote_open_order_ids: &BTreeSet<String>,
) -> Result<BTreeMap<String, RecoveredOpenOrder>> {
    let records = replay_records(journal_path)?;
    recover_open_order_mappings_records(&records, remote_open_order_ids)
}

pub fn recover_open_order_mappings_records(
    records: &[JournalRecord],
    remote_open_order_ids: &BTreeSet<String>,
) -> Result<BTreeMap<String, RecoveredOpenOrder>> {
    let attempts = group_attempts(records);
    let mut recovered = BTreeMap::new();
    for (client_order_id, attempt) in attempts {
        if attempt.iter().any(|record| record.is_terminal()) {
            continue;
        }
        let Some(exchange_order_id) = attempt
            .iter()
            .rev()
            .filter_map(|record| record.exchange_order_id.as_ref())
            .find(|order_id| remote_open_order_ids.contains(*order_id))
        else {
            continue;
        };
        let submitted_at_ms = attempt
            .iter()
            .find(|record| record.event_kind == JournalEventKind::WriteAhead)
            .map(|record| u64::try_from(record.wall_clock_timestamp_ms))
            .transpose()
            .map_err(|_| BotError::Journal("journal_timestamp_exceeds_u64".to_string()))?
            .ok_or_else(|| BotError::Journal("write_ahead_timestamp_missing".to_string()))?;
        let asset_id = attempt
            .iter()
            .find(|record| record.event_kind == JournalEventKind::WriteAhead)
            .map(|record| recover_write_ahead_asset(&record.payload))
            .transpose()?
            .ok_or_else(|| BotError::Journal("write_ahead_asset_missing".to_string()))?;
        recovered.insert(
            exchange_order_id.clone(),
            RecoveredOpenOrder {
                client_order_id: client_order_id.to_string(),
                submitted_at_ms,
                asset_id,
            },
        );
    }
    Ok(recovered)
}

fn recover_write_ahead_asset(payload: &str) -> Result<AssetId> {
    let encoded = payload
        .split('|')
        .find_map(|field| field.strip_prefix("asset_id_hex="))
        .ok_or_else(|| BotError::Journal("write_ahead_asset_missing".to_string()))?;
    if encoded.is_empty() || encoded.len() % 2 != 0 {
        return Err(BotError::Journal(
            "write_ahead_asset_hex_invalid".to_string(),
        ));
    }
    let bytes = encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_nibble(pair[0])?;
            let low = hex_nibble(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect::<Result<Vec<_>>>()?;
    let asset = String::from_utf8(bytes)
        .map_err(|_| BotError::Journal("write_ahead_asset_not_utf8".to_string()))?;
    if asset.is_empty() {
        return Err(BotError::Journal("write_ahead_asset_empty".to_string()));
    }
    Ok(AssetId::from(asset))
}

fn hex_nibble(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(BotError::Journal(
            "write_ahead_asset_hex_invalid".to_string(),
        )),
    }
}

fn group_attempts(records: &[JournalRecord]) -> BTreeMap<&str, Vec<&JournalRecord>> {
    let mut attempts = BTreeMap::<&str, Vec<&JournalRecord>>::new();
    for record in records {
        if record.event_kind == JournalEventKind::WriteAhead {
            attempts.entry(&record.client_order_id).or_default();
        }
        if let Some(attempt) = attempts.get_mut(record.client_order_id.as_str()) {
            attempt.push(record);
        }
    }
    attempts
}

fn remote_contains_attempt(records: &[&JournalRecord], remote: &RemoteSnapshot) -> bool {
    records
        .iter()
        .rev()
        .filter_map(|record| record.exchange_order_id.as_ref())
        .any(|order_id| {
            remote.open_order_ids.contains(order_id) || remote.trade_order_ids.contains(order_id)
        })
}

fn blocked(reason: &str, replayed_records: usize, in_flight_attempts: usize) -> RecoveryReport {
    RecoveryReport {
        live_unlock_allowed: false,
        reason: reason.to_string(),
        replayed_records,
        in_flight_attempts,
    }
}

pub fn require_recovered(report: &RecoveryReport) -> Result<()> {
    if report.live_unlock_allowed {
        Ok(())
    } else {
        Err(BotError::Readiness(report.reason.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{Journal, JournalStartup};

    #[test]
    fn recovery_requires_remote_state() {
        let path = std::env::temp_dir().join(format!(
            "polymarket_rs_recovery_{}.log",
            uuid::Uuid::new_v4()
        ));
        let _ = std::fs::remove_file(&path);
        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        journal
            .append_write_ahead("d", "c", "s", "accepted", "intent_v2|asset_id_hex=31")
            .unwrap();
        drop(journal);
        let report = recover_after_crash(&path, &RemoteSnapshot::default()).unwrap();
        assert!(!report.live_unlock_allowed);
        assert_eq!(report.reason, "open_orders_not_loaded");
        assert_eq!(report.replayed_records, 1);
        assert_eq!(report.in_flight_attempts, 1);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn recovery_requires_each_nonterminal_attempt_to_match_remote_state() {
        let path = std::env::temp_dir().join(format!(
            "polymarket_rs_recovery_remote_{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        journal
            .append_write_ahead("d", "c", "s", "accepted", "intent_v2|asset_id_hex=31")
            .unwrap();
        journal
            .append_lifecycle(
                JournalEventKind::Acknowledged,
                "d",
                "c",
                Some("exchange-1".to_string()),
                "s",
                "acknowledged",
                "live",
            )
            .unwrap();
        drop(journal);

        let loaded = RemoteSnapshot {
            open_orders_loaded: true,
            trades_loaded: true,
            balances_loaded: true,
            allowances_loaded: true,
            ..RemoteSnapshot::default()
        };
        let report = recover_after_crash(&path, &loaded).unwrap();
        assert!(!report.live_unlock_allowed);
        assert_eq!(report.reason, "unresolved_in_flight_attempts");

        let mut reconciled = loaded;
        reconciled.open_order_ids.insert("exchange-1".to_string());
        assert!(
            recover_after_crash(&path, &reconciled)
                .unwrap()
                .live_unlock_allowed
        );
        let mappings = recover_open_order_mappings(&path, &reconciled.open_order_ids).unwrap();
        assert_eq!(mappings["exchange-1"].client_order_id, "c");
        assert_eq!(mappings["exchange-1"].asset_id, AssetId::from("1"));

        reconciled.open_order_ids.insert("manual-order".to_string());
        let report = recover_after_crash(&path, &reconciled).unwrap();
        assert!(!report.live_unlock_allowed);
        assert_eq!(report.reason, "unmapped_remote_open_orders");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("jsonl.checkpoint"));
    }
}
