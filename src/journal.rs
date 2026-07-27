use crate::error::{BotError, Result};
use fs2::FileExt as _;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

pub const GENESIS_HASH: &str = "GENESIS";
pub const JOURNAL_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalEventKind {
    WriteAhead,
    PaperTransition,
    AccountMutation,
    Submitted,
    Acknowledged,
    Rejected,
    PartiallyFilled,
    Filled,
    CancelRequested,
    Cancelled,
    Reconciled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalRecord {
    pub schema_version: u32,
    pub event_kind: JournalEventKind,
    pub monotonic_sequence: u64,
    pub process_startup_id: String,
    pub wall_clock_timestamp_ms: u128,
    pub monotonic_timestamp_ms: u128,
    pub config_hash: String,
    pub binary_hash: String,
    pub previous_record_hash: String,
    pub record_hash: String,
    pub source_event_refs: Vec<String>,
    pub decision_id: String,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub strategy_id: String,
    pub risk_result: String,
    pub payload: String,
}

#[derive(Serialize)]
struct HashMaterial<'a> {
    schema_version: u32,
    event_kind: JournalEventKind,
    monotonic_sequence: u64,
    process_startup_id: &'a str,
    wall_clock_timestamp_ms: u128,
    monotonic_timestamp_ms: u128,
    config_hash: &'a str,
    binary_hash: &'a str,
    previous_record_hash: &'a str,
    source_event_refs: &'a [String],
    decision_id: &'a str,
    client_order_id: &'a str,
    exchange_order_id: &'a Option<String>,
    strategy_id: &'a str,
    risk_result: &'a str,
    payload: &'a str,
}

impl JournalRecord {
    fn new(
        sequence: u64,
        previous_hash: String,
        startup: &JournalStartup,
        entry: JournalEntry,
    ) -> Result<Self> {
        let mut record = Self {
            schema_version: JOURNAL_SCHEMA_VERSION,
            event_kind: entry.event_kind,
            monotonic_sequence: sequence,
            process_startup_id: startup.process_startup_id.clone(),
            wall_clock_timestamp_ms: now_ms(),
            monotonic_timestamp_ms: startup.started_at.elapsed().as_millis(),
            config_hash: startup.config_hash.clone(),
            binary_hash: startup.binary_hash.clone(),
            previous_record_hash: previous_hash,
            record_hash: String::new(),
            source_event_refs: entry.source_event_refs,
            decision_id: entry.decision_id,
            client_order_id: entry.client_order_id,
            exchange_order_id: entry.exchange_order_id,
            strategy_id: entry.strategy_id,
            risk_result: entry.risk_result,
            payload: entry.payload,
        };
        record.record_hash = record.compute_hash()?;
        Ok(record)
    }

    fn hash_material(&self) -> HashMaterial<'_> {
        HashMaterial {
            schema_version: self.schema_version,
            event_kind: self.event_kind,
            monotonic_sequence: self.monotonic_sequence,
            process_startup_id: &self.process_startup_id,
            wall_clock_timestamp_ms: self.wall_clock_timestamp_ms,
            monotonic_timestamp_ms: self.monotonic_timestamp_ms,
            config_hash: &self.config_hash,
            binary_hash: &self.binary_hash,
            previous_record_hash: &self.previous_record_hash,
            source_event_refs: &self.source_event_refs,
            decision_id: &self.decision_id,
            client_order_id: &self.client_order_id,
            exchange_order_id: &self.exchange_order_id,
            strategy_id: &self.strategy_id,
            risk_result: &self.risk_result,
            payload: &self.payload,
        }
    }

    pub fn compute_hash(&self) -> Result<String> {
        let material = serde_json::to_vec(&self.hash_material())
            .map_err(|error| BotError::Journal(format!("hash_material_serialize:{error}")))?;
        Ok(blake3::hash(&material).to_hex().to_string())
    }

    pub fn to_line(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|error| BotError::Journal(format!("record_serialize:{error}")))
    }

    pub fn from_line(line: &str) -> Result<Self> {
        let record: Self = serde_json::from_str(line)
            .map_err(|error| BotError::Journal(format!("record_parse:{error}")))?;
        if record.schema_version != JOURNAL_SCHEMA_VERSION {
            return Err(BotError::Journal(format!(
                "unsupported_schema_version:{}",
                record.schema_version
            )));
        }
        if record.process_startup_id.is_empty()
            || record.config_hash.is_empty()
            || record.binary_hash.is_empty()
            || record.client_order_id.is_empty()
        {
            return Err(BotError::Journal(
                "journal_record_missing_required_field".to_string(),
            ));
        }
        if record.record_hash != record.compute_hash()? {
            return Err(BotError::Journal("record_hash_mismatch".to_string()));
        }
        Ok(record)
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.event_kind,
            JournalEventKind::Rejected | JournalEventKind::Filled | JournalEventKind::Cancelled
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalEntry {
    pub event_kind: JournalEventKind,
    pub source_event_refs: Vec<String>,
    pub decision_id: String,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub strategy_id: String,
    pub risk_result: String,
    pub payload: String,
}

#[derive(Debug, Clone)]
pub struct JournalStartup {
    pub process_startup_id: String,
    pub config_hash: String,
    pub binary_hash: String,
    started_at: Instant,
}

impl JournalStartup {
    pub fn new(config_hash: impl Into<String>, binary_hash: impl Into<String>) -> Self {
        Self {
            process_startup_id: Uuid::new_v4().to_string(),
            config_hash: config_hash.into(),
            binary_hash: binary_hash.into(),
            started_at: Instant::now(),
        }
    }
}

#[derive(Debug)]
pub struct Journal {
    path: PathBuf,
    startup: JournalStartup,
    file: File,
    next_sequence: u64,
    last_hash: String,
    poisoned: Option<String>,
    checkpoint_warning: Option<String>,
    #[cfg(test)]
    fail_after_appends: Option<usize>,
    #[cfg(test)]
    fail_stage: Option<JournalFailStage>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JournalFailStage {
    AfterWriteBeforeSync,
    AfterSyncBeforeCheckpoint,
}

impl Journal {
    pub fn open(path: impl AsRef<Path>, startup: JournalStartup) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = open_journal_file(&path)?;
        file.try_lock_exclusive().map_err(|error| {
            BotError::Journal(format!("journal_already_locked_or_unavailable:{error}"))
        })?;
        let verification = verify_reader(&mut file, &path)?;
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            path,
            startup,
            file,
            next_sequence: verification.next_sequence,
            last_hash: verification.last_hash,
            poisoned: None,
            checkpoint_warning: None,
            #[cfg(test)]
            fail_after_appends: None,
            #[cfg(test)]
            fail_stage: None,
        })
    }

    pub fn write_checkpoint(&self) -> Result<()> {
        write_checkpoint(&self.path, self.next_sequence, &self.last_hash)
    }

    pub fn records_locked(&mut self) -> Result<Vec<JournalRecord>> {
        let result = (|| -> Result<Vec<JournalRecord>> {
            self.file.sync_data()?;
            verify_reader(&mut self.file, &self.path)?;
            self.file.seek(SeekFrom::Start(0))?;
            let reader = BufReader::new(self.file.try_clone()?);
            let mut records = Vec::new();
            for line in reader.lines() {
                let line = line?;
                if !line.trim().is_empty() {
                    records.push(JournalRecord::from_line(&line)?);
                }
            }
            Ok(records)
        })();
        let seek_result = self.file.seek(SeekFrom::End(0));
        match (result, seek_result) {
            (Ok(records), Ok(_)) => Ok(records),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error.into()),
        }
    }

    pub fn append_write_ahead(
        &mut self,
        decision_id: impl Into<String>,
        client_order_id: impl Into<String>,
        strategy_id: impl Into<String>,
        risk_result: impl Into<String>,
        payload: impl Into<String>,
    ) -> Result<JournalRecord> {
        self.append_write_ahead_with_sources(
            decision_id,
            client_order_id,
            strategy_id,
            risk_result,
            payload,
            Vec::new(),
        )
    }

    pub fn append_write_ahead_with_sources(
        &mut self,
        decision_id: impl Into<String>,
        client_order_id: impl Into<String>,
        strategy_id: impl Into<String>,
        risk_result: impl Into<String>,
        payload: impl Into<String>,
        source_event_refs: Vec<String>,
    ) -> Result<JournalRecord> {
        self.append_event(JournalEntry {
            event_kind: JournalEventKind::WriteAhead,
            source_event_refs,
            decision_id: decision_id.into(),
            client_order_id: client_order_id.into(),
            exchange_order_id: None,
            strategy_id: strategy_id.into(),
            risk_result: risk_result.into(),
            payload: payload.into(),
        })
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "lifecycle records keep every identity and audit field explicit at call sites"
    )]
    pub fn append_lifecycle(
        &mut self,
        event_kind: JournalEventKind,
        decision_id: impl Into<String>,
        client_order_id: impl Into<String>,
        exchange_order_id: Option<String>,
        strategy_id: impl Into<String>,
        result: impl Into<String>,
        payload: impl Into<String>,
    ) -> Result<JournalRecord> {
        if event_kind == JournalEventKind::WriteAhead {
            return Err(BotError::Journal(
                "use_append_write_ahead_for_pre_submit_records".to_string(),
            ));
        }
        self.append_event(JournalEntry {
            event_kind,
            source_event_refs: Vec::new(),
            decision_id: decision_id.into(),
            client_order_id: client_order_id.into(),
            exchange_order_id,
            strategy_id: strategy_id.into(),
            risk_result: result.into(),
            payload: payload.into(),
        })
    }

    fn append_event(&mut self, entry: JournalEntry) -> Result<JournalRecord> {
        if let Some(reason) = &self.poisoned {
            return Err(BotError::Journal(format!("journal_poisoned:{reason}")));
        }
        #[cfg(test)]
        if let Some(remaining) = self.fail_after_appends.take() {
            if remaining == 0 {
                let reason = "injected_append_failure".to_string();
                self.poisoned = Some(reason.clone());
                return Err(BotError::Journal(format!(
                    "journal_append_failed_and_poisoned:{reason}"
                )));
            }
            self.fail_after_appends = Some(remaining.saturating_sub(1));
        }

        let preflight = (|| -> Result<(JournalRecord, Vec<u8>, u64, u64)> {
            let next_sequence = self
                .next_sequence
                .checked_add(1)
                .ok_or_else(|| BotError::Journal("journal_sequence_exhausted".to_string()))?;
            let record = JournalRecord::new(
                self.next_sequence,
                self.last_hash.clone(),
                &self.startup,
                entry,
            )?;
            let mut bytes = record.to_line()?.into_bytes();
            bytes.push(b'\n');
            let old_len = self.file.metadata()?.len();
            Ok((record, bytes, old_len, next_sequence))
        })();
        let (record, bytes, old_len, next_sequence) = match preflight {
            Ok(value) => value,
            Err(error) => return self.poison_without_commit(error),
        };
        #[cfg(test)]
        let fail_stage = self.fail_stage.take();
        if let Err(error) = self.file.write_all(&bytes) {
            return self.rollback_uncommitted(old_len, error.into());
        }
        #[cfg(test)]
        if matches!(fail_stage, Some(JournalFailStage::AfterWriteBeforeSync)) {
            return self.rollback_uncommitted(
                old_len,
                BotError::Journal("injected_after_write_failure".to_string()),
            );
        }
        if let Err(error) = self.file.sync_data() {
            return self.rollback_uncommitted(old_len, error.into());
        }

        // The synced log record is the authoritative commit point. Checkpoints
        // accelerate startup but are allowed to lag a committed record.
        self.next_sequence = next_sequence;
        self.last_hash.clone_from(&record.record_hash);
        #[cfg(test)]
        let checkpoint_result = if matches!(
            fail_stage,
            Some(JournalFailStage::AfterSyncBeforeCheckpoint)
        ) {
            Err(BotError::Journal(
                "injected_checkpoint_after_commit_failure".to_string(),
            ))
        } else {
            self.write_checkpoint()
        };
        #[cfg(not(test))]
        let checkpoint_result = self.write_checkpoint();
        match checkpoint_result {
            Ok(()) => self.checkpoint_warning = None,
            Err(error) => self.checkpoint_warning = Some(error.to_string()),
        }
        Ok(record)
    }

    fn poison_without_commit<T>(&mut self, error: BotError) -> Result<T> {
        let reason = error.to_string();
        self.poisoned = Some(reason.clone());
        Err(BotError::Journal(format!(
            "journal_append_failed_and_poisoned:{reason}"
        )))
    }

    fn rollback_uncommitted<T>(&mut self, old_len: u64, error: BotError) -> Result<T> {
        let rollback = self
            .file
            .set_len(old_len)
            .and_then(|()| self.file.seek(SeekFrom::End(0)).map(|_| ()))
            .and_then(|()| self.file.sync_data());
        let reason = match rollback {
            Ok(()) => format!("{};append_rolled_back", error),
            Err(rollback_error) => format!("{};rollback_failed:{rollback_error}", error),
        };
        self.poisoned = Some(reason.clone());
        Err(BotError::Journal(format!(
            "journal_append_failed_and_poisoned:{reason}"
        )))
    }

    pub const fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    pub fn last_hash(&self) -> &str {
        &self.last_hash
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.is_some()
    }

    pub fn checkpoint_warning(&self) -> Option<&str> {
        self.checkpoint_warning.as_deref()
    }

    pub(crate) fn note_maintenance_warning(&mut self, warning: impl Into<String>) {
        self.checkpoint_warning = Some(warning.into());
    }

    pub(crate) fn should_compact(&self, max_records: u64, max_bytes: u64) -> Result<bool> {
        Ok(self.next_sequence >= max_records || self.file.metadata()?.len() >= max_bytes)
    }

    pub(crate) fn compact_paper_history(
        &mut self,
        decision_id: impl Into<String>,
        client_order_id: impl Into<String>,
        payload: impl Into<String>,
    ) -> Result<()> {
        if let Some(reason) = &self.poisoned {
            return Err(BotError::Journal(format!("journal_poisoned:{reason}")));
        }
        let prior_sequence = self.next_sequence;
        let prior_hash = self.last_hash.clone();
        let entry = JournalEntry {
            event_kind: JournalEventKind::PaperTransition,
            source_event_refs: vec![
                format!("compacted_records:{prior_sequence}"),
                format!("compacted_root:{prior_hash}"),
            ],
            decision_id: decision_id.into(),
            client_order_id: client_order_id.into(),
            exchange_order_id: None,
            strategy_id: "paper".to_string(),
            risk_result: "compacted_snapshot".to_string(),
            payload: payload.into(),
        };
        let record = JournalRecord::new(0, GENESIS_HASH.to_string(), &self.startup, entry)?;
        let mut bytes = record.to_line()?.into_bytes();
        bytes.push(b'\n');
        let temporary = self.path.with_extension(format!(
            "{}.compact-{}",
            self.path
                .extension()
                .and_then(|value| value.to_str())
                .unwrap_or("jsonl"),
            Uuid::new_v4()
        ));
        let result = (|| -> Result<()> {
            let mut replacement = secure_create_journal(&temporary)?;
            replacement.try_lock_exclusive().map_err(|error| {
                BotError::Journal(format!("journal_compaction_lock_failed:{error}"))
            })?;
            replacement.write_all(&bytes)?;
            replacement.sync_all()?;

            let checkpoint = checkpoint_path(&self.path);
            match std::fs::remove_file(&checkpoint) {
                Ok(()) => sync_parent(&checkpoint)?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            std::fs::rename(&temporary, &self.path)?;

            // Both the old append-only journal and this replacement contain the
            // same committed paper state. Once rename succeeds, any remaining
            // metadata/checkpoint error is a maintenance warning, not a false
            // report that the paper transition was uncommitted.
            let parent_result = sync_parent(&self.path);
            let old_file = std::mem::replace(&mut self.file, replacement);
            let _ = old_file.unlock();
            self.next_sequence = 1;
            self.last_hash.clone_from(&record.record_hash);
            self.checkpoint_warning = parent_result.err().map(|error| error.to_string());
            if let Err(error) = self.write_checkpoint() {
                self.checkpoint_warning = Some(error.to_string());
            }
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn fail_append_after_for_test(&mut self, successful_appends: usize) {
        self.fail_after_appends = Some(successful_appends);
    }

    #[cfg(test)]
    pub(crate) fn fail_stage_for_test(&mut self, stage: JournalFailStage) {
        self.fail_stage = Some(stage);
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalVerification {
    pub next_sequence: u64,
    pub last_hash: String,
}

pub fn verify_journal(path: impl AsRef<Path>) -> Result<JournalVerification> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(JournalVerification {
            next_sequence: 0,
            last_hash: GENESIS_HASH.to_string(),
        });
    }
    let mut file = OpenOptions::new().read(true).open(path)?;
    file.try_lock_shared()
        .map_err(|error| BotError::Journal(format!("journal_busy:{error}")))?;
    let result = verify_reader(&mut file, path);
    let _ = file.unlock();
    result
}

fn verify_reader(file: &mut File, path: &Path) -> Result<JournalVerification> {
    file.seek(SeekFrom::Start(0))?;
    let reader = BufReader::new(file.try_clone()?);
    let mut expected_previous = GENESIS_HASH.to_string();
    let mut expected_sequence = 0u64;
    let checkpoint = read_checkpoint(path)?;
    let mut checkpoint_matched = checkpoint
        .as_ref()
        .is_some_and(|value| value.next_sequence == 0);

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let record = JournalRecord::from_line(&line)?;
        if record.monotonic_sequence != expected_sequence {
            return Err(BotError::Journal("sequence_gap".to_string()));
        }
        if record.previous_record_hash != expected_previous {
            return Err(BotError::Journal("previous_hash_mismatch".to_string()));
        }
        expected_previous = record.record_hash;
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or_else(|| BotError::Journal("journal_sequence_exhausted".to_string()))?;
        if checkpoint.as_ref().is_some_and(|value| {
            value.next_sequence == expected_sequence && value.last_hash == expected_previous
        }) {
            checkpoint_matched = true;
        }
    }
    if let Some(checkpoint) = checkpoint {
        if checkpoint.next_sequence > expected_sequence || !checkpoint_matched {
            return Err(BotError::Journal("checkpoint_mismatch".to_string()));
        }
    }
    Ok(JournalVerification {
        next_sequence: expected_sequence,
        last_hash: expected_previous,
    })
}

pub fn replay_records(path: impl AsRef<Path>) -> Result<Vec<JournalRecord>> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut file = OpenOptions::new().read(true).open(path)?;
    file.try_lock_shared()
        .map_err(|error| BotError::Journal(format!("journal_busy:{error}")))?;
    verify_reader(&mut file, path)?;
    file.seek(SeekFrom::Start(0))?;
    let reader = BufReader::new(file.try_clone()?);
    let mut records = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if !line.trim().is_empty() {
            records.push(JournalRecord::from_line(&line)?);
        }
    }
    let _ = file.unlock();
    Ok(records)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct JournalCheckpoint {
    next_sequence: u64,
    last_hash: String,
}

pub fn write_checkpoint(path: impl AsRef<Path>, next_sequence: u64, last_hash: &str) -> Result<()> {
    let path = path.as_ref();
    let checkpoint_path = checkpoint_path(path);
    if let Some(parent) = checkpoint_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let temporary = checkpoint_path.with_extension(format!(
        "{}.tmp-{}",
        checkpoint_path
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("checkpoint"),
        Uuid::new_v4()
    ));
    let checkpoint = JournalCheckpoint {
        next_sequence,
        last_hash: last_hash.to_string(),
    };
    {
        let mut file = secure_create(&temporary)?;
        serde_json::to_writer(&mut file, &checkpoint)
            .map_err(|error| BotError::Journal(format!("checkpoint_serialize:{error}")))?;
        writeln!(file)?;
        file.sync_all()?;
    }
    std::fs::rename(&temporary, &checkpoint_path)?;
    sync_parent(&checkpoint_path)?;
    Ok(())
}

fn read_checkpoint(path: &Path) -> Result<Option<JournalCheckpoint>> {
    let path = checkpoint_path(path);
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(path)?;
    let checkpoint = serde_json::from_str(&raw)
        .map_err(|error| BotError::Journal(format!("checkpoint_parse:{error}")))?;
    Ok(Some(checkpoint))
}

fn checkpoint_path(path: &Path) -> PathBuf {
    let mut checkpoint = path.to_path_buf();
    let extension = checkpoint
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| format!("{value}.checkpoint"))
        .unwrap_or_else(|| "checkpoint".to_string());
    checkpoint.set_extension(extension);
    checkpoint
}

pub fn stable_hash_hex(input: &str) -> String {
    blake3::hash(input.as_bytes()).to_hex().to_string()
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis())
        .unwrap_or(0)
}

fn open_journal_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).read(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    harden_permissions(path)?;
    sync_parent(path)?;
    Ok(file)
}

fn secure_create(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    Ok(options.open(path)?)
}

fn secure_create_journal(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create_new(true).read(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    Ok(options.open(path)?)
}

fn harden_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("polymarket_rs_{name}_{}.jsonl", Uuid::new_v4()))
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(checkpoint_path(path));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn appends_lifecycle_and_verifies_blake3_chain() {
        let path = temp_path("journal");
        let startup = JournalStartup::new("config", "binary");
        let mut journal = Journal::open(&path, startup).unwrap();
        let first = journal
            .append_write_ahead("d1", "c1", "s1", "accepted", "payload")
            .unwrap();
        let second = journal
            .append_lifecycle(
                JournalEventKind::Acknowledged,
                "d1",
                "c1",
                Some("exchange-1".to_string()),
                "s1",
                "acknowledged",
                "live",
            )
            .unwrap();
        assert_eq!(second.previous_record_hash, first.record_hash);
        assert_eq!(second.record_hash.len(), 64);
        drop(journal);
        let verification = verify_journal(&path).unwrap();
        assert_eq!(verification.next_sequence, 2);
        assert_eq!(replay_records(&path).unwrap().len(), 2);
        cleanup(&path);
    }

    #[test]
    fn detects_tampering_and_torn_records() {
        let path = temp_path("tamper");
        std::fs::write(&path, "{\"schema_version\":2}\n").unwrap();
        assert!(verify_journal(&path).is_err());
        cleanup(&path);
    }

    #[test]
    fn exclusive_lock_rejects_a_second_writer() {
        let path = temp_path("lock");
        let first = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        let error = Journal::open(&path, JournalStartup::new("c", "b")).unwrap_err();
        assert!(error.to_string().contains("locked"));
        drop(first);
        assert!(Journal::open(&path, JournalStartup::new("c", "b")).is_ok());
        cleanup(&path);
    }

    #[test]
    fn stale_but_valid_checkpoint_is_accepted() {
        let path = temp_path("checkpoint");
        let mut journal = Journal::open(&path, JournalStartup::new("c", "b")).unwrap();
        let first = journal
            .append_write_ahead("d1", "c1", "s1", "accepted", "one")
            .unwrap();
        write_checkpoint(&path, 1, &first.record_hash).unwrap();
        journal
            .append_write_ahead("d2", "c2", "s1", "accepted", "two")
            .unwrap();
        write_checkpoint(&path, 1, &first.record_hash).unwrap();
        drop(journal);
        assert_eq!(verify_journal(&path).unwrap().next_sequence, 2);
        cleanup(&path);
    }
}
