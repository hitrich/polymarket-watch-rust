use crate::config::Settings;
use crate::error::{BotError, Result};
use crate::types::BotMode;
use fs2::FileExt as _;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;
use uuid::Uuid;

const COMPLIANCE_LATCH_SCHEMA_VERSION: u32 = 1;
const MAX_COMPLIANCE_REASONS: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComplianceLatchState {
    pub schema_version: u32,
    pub controlled_wallets: Vec<String>,
    pub prohibited_conduct_flag: bool,
    pub confidential_info_flag: bool,
    pub reasons: Vec<String>,
    pub updated_at_ms: u64,
}

#[derive(Debug)]
pub struct ComplianceGuard {
    path: PathBuf,
    _lock: File,
    state: ComplianceLatchState,
}

impl ComplianceGuard {
    pub fn open(
        path: impl AsRef<Path>,
        controlled_wallets: &[String],
        now_ms: u64,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        let lock_path = path.with_extension(format!(
            "{}.lock",
            path.extension()
                .and_then(|value| value.to_str())
                .unwrap_or("json")
        ));
        let lock = secure_open_lock(&lock_path)?;
        lock.try_lock_exclusive().map_err(|error| {
            BotError::Compliance(format!("compliance_latch_already_locked:{error}"))
        })?;
        let expected_wallets = normalized_wallets(controlled_wallets);
        let state = if path.exists() {
            harden_permissions(&path)?;
            let raw = std::fs::read_to_string(&path)?;
            let state: ComplianceLatchState = serde_json::from_str(&raw)
                .map_err(|error| BotError::Compliance(format!("compliance_latch_parse:{error}")))?;
            validate_latch_state(&state, &expected_wallets)?;
            state
        } else {
            let state = ComplianceLatchState {
                schema_version: COMPLIANCE_LATCH_SCHEMA_VERSION,
                controlled_wallets: expected_wallets,
                prohibited_conduct_flag: false,
                confidential_info_flag: false,
                reasons: Vec::new(),
                updated_at_ms: now_ms,
            };
            write_latch_state(&path, &state)?;
            state
        };
        Ok(Self {
            path,
            _lock: lock,
            state,
        })
    }

    pub fn state(&self) -> &ComplianceLatchState {
        &self.state
    }

    pub fn controls_wallet(&self, wallet: &str) -> bool {
        self.state
            .controlled_wallets
            .iter()
            .any(|controlled| controlled.eq_ignore_ascii_case(wallet))
    }

    pub fn latch_prohibited(&mut self, reason: impl Into<String>, now_ms: u64) -> Result<()> {
        self.latch(reason.into(), true, false, now_ms)
    }

    pub fn latch_confidential(&mut self, reason: impl Into<String>, now_ms: u64) -> Result<()> {
        self.latch(reason.into(), false, true, now_ms)
    }

    fn latch(
        &mut self,
        reason: String,
        prohibited: bool,
        confidential: bool,
        now_ms: u64,
    ) -> Result<()> {
        let mut staged = self.state.clone();
        staged.prohibited_conduct_flag |= prohibited;
        staged.confidential_info_flag |= confidential;
        if !staged.reasons.contains(&reason) {
            staged.reasons.push(reason);
            if staged.reasons.len() > MAX_COMPLIANCE_REASONS {
                let excess = staged.reasons.len() - MAX_COMPLIANCE_REASONS;
                staged.reasons.drain(0..excess);
            }
        }
        staged.updated_at_ms = now_ms;
        write_latch_state(&self.path, &staged)?;
        self.state = staged;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComplianceState {
    pub geoblock_checked: bool,
    pub geoblocked: bool,
    pub venue_allowed: bool,
    pub country_code: Option<String>,
    pub region_code: Option<String>,
    pub checked_at_ms: Option<u64>,
    pub prohibited_conduct_flag: bool,
    pub confidential_info_flag: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EligibilityStatus {
    Unverified,
    InvalidFuture,
    Stale,
    Blocked,
    VenueReview,
    Eligible,
}

#[derive(Debug, Deserialize)]
struct GeoblockResponse {
    blocked: bool,
    country: String,
    #[serde(default)]
    region: String,
}

pub async fn fetch_geoblock_state(url: &str, checked_at_ms: u64) -> Result<ComplianceState> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|error| BotError::Compliance(format!("geoblock_client:{error}")))?;
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|error| BotError::Compliance(format!("geoblock_request:{error}")))?
        .error_for_status()
        .map_err(|error| BotError::Compliance(format!("geoblock_status:{error}")))?
        .json::<GeoblockResponse>()
        .await
        .map_err(|error| BotError::Compliance(format!("geoblock_response:{error}")))?;
    let country = response.country.trim().to_ascii_uppercase();
    if !valid_country_code(&country) {
        return Err(BotError::Compliance(
            "geoblock_country_code_invalid".to_string(),
        ));
    }
    Ok(ComplianceState::from_geoblock_result(
        response.blocked,
        country,
        response.region,
        !response.blocked,
        checked_at_ms,
    ))
}

impl Default for ComplianceState {
    fn default() -> Self {
        Self {
            geoblock_checked: false,
            geoblocked: true,
            venue_allowed: false,
            country_code: None,
            region_code: None,
            checked_at_ms: None,
            prohibited_conduct_flag: false,
            confidential_info_flag: false,
        }
    }
}

impl ComplianceState {
    pub fn from_geoblock_result(
        blocked: bool,
        country_code: impl Into<String>,
        region_code: impl Into<String>,
        venue_allowed: bool,
        checked_at_ms: u64,
    ) -> Self {
        Self {
            geoblock_checked: true,
            geoblocked: blocked,
            venue_allowed,
            country_code: non_empty(country_code.into()),
            region_code: non_empty(region_code.into()),
            checked_at_ms: Some(checked_at_ms),
            prohibited_conduct_flag: false,
            confidential_info_flag: false,
        }
    }

    pub fn eligibility_status_at(&self, now_ms: u64, max_age_ms: u64) -> EligibilityStatus {
        if !self.has_geoblock_response() {
            EligibilityStatus::Unverified
        } else if self
            .checked_at_ms
            .is_some_and(|checked_at_ms| checked_at_ms > now_ms)
        {
            EligibilityStatus::InvalidFuture
        } else if !self.geography_is_fresh(now_ms, max_age_ms) {
            EligibilityStatus::Stale
        } else if self.geoblocked {
            EligibilityStatus::Blocked
        } else if !self.venue_allowed {
            EligibilityStatus::VenueReview
        } else {
            EligibilityStatus::Eligible
        }
    }

    pub fn has_geoblock_response(&self) -> bool {
        self.geoblock_checked
            && self.checked_at_ms.is_some_and(|timestamp| timestamp > 0)
            && self.country_code.as_deref().is_some_and(valid_country_code)
    }

    pub fn geography_is_fresh(&self, now_ms: u64, max_age_ms: u64) -> bool {
        let Some(checked_at_ms) = self.checked_at_ms else {
            return false;
        };
        self.has_geoblock_response()
            && max_age_ms > 0
            && checked_at_ms <= now_ms
            && now_ms - checked_at_ms <= max_age_ms
    }

    pub fn location_label(&self) -> String {
        match (&self.country_code, &self.region_code) {
            (Some(country), Some(region)) => format!("{country} · {region}"),
            (Some(country), None) => country.clone(),
            _ => "Location unavailable".to_string(),
        }
    }

    pub fn apply_persistent_latch(&mut self, latch: &ComplianceLatchState) {
        self.prohibited_conduct_flag = latch.prohibited_conduct_flag;
        self.confidential_info_flag = latch.confidential_info_flag;
    }
}

fn normalized_wallets(wallets: &[String]) -> Vec<String> {
    let mut wallets = wallets
        .iter()
        .map(|wallet| wallet.to_ascii_lowercase())
        .collect::<Vec<_>>();
    wallets.sort();
    wallets.dedup();
    wallets
}

fn validate_latch_state(state: &ComplianceLatchState, expected_wallets: &[String]) -> Result<()> {
    if state.schema_version != COMPLIANCE_LATCH_SCHEMA_VERSION {
        return Err(BotError::Compliance(
            "compliance_latch_schema_unsupported".to_string(),
        ));
    }
    if state.controlled_wallets != expected_wallets {
        return Err(BotError::Compliance(
            "controlled_wallet_declaration_changed_use_new_latch_file".to_string(),
        ));
    }
    if state.reasons.len() > MAX_COMPLIANCE_REASONS {
        return Err(BotError::Compliance(
            "compliance_latch_reason_limit_exceeded".to_string(),
        ));
    }
    Ok(())
}

fn write_latch_state(path: &Path, state: &ComplianceLatchState) -> Result<()> {
    let temporary = path.with_extension(format!(
        "{}.tmp-{}",
        path.extension()
            .and_then(|value| value.to_str())
            .unwrap_or("json"),
        Uuid::new_v4()
    ));
    let result = (|| -> Result<()> {
        let mut file = secure_create(&temporary)?;
        serde_json::to_writer(&mut file, state)
            .map_err(|error| BotError::Compliance(format!("compliance_latch_serialize:{error}")))?;
        writeln!(file)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        sync_parent(path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temporary);
    }
    result
}

fn secure_open_lock(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    harden_permissions(path)?;
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

fn non_empty(value: String) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn valid_country_code(value: &str) -> bool {
    value.len() == 2 && value.bytes().all(|byte| byte.is_ascii_uppercase())
}

pub fn check_compliance(settings: &Settings, state: &ComplianceState, now_ms: u64) -> Result<()> {
    if state.prohibited_conduct_flag {
        return Err(BotError::Compliance("prohibited_conduct".to_string()));
    }
    if state.confidential_info_flag {
        return Err(BotError::Compliance("confidential_info".to_string()));
    }
    if settings.mode != BotMode::Live {
        return Ok(());
    }
    if !state.has_geoblock_response() {
        return Err(BotError::Compliance("geoblock_not_checked".to_string()));
    }
    let checked_at_ms = state.checked_at_ms.unwrap_or_default();
    if checked_at_ms > now_ms {
        return Err(BotError::Compliance(
            "geoblock_timestamp_in_future".to_string(),
        ));
    }
    if !state.geography_is_fresh(now_ms, settings.max_geoblock_age_ms) {
        return Err(BotError::Compliance("geoblock_check_stale".to_string()));
    }
    if state.geoblocked {
        return Err(BotError::Compliance("geoblocked".to_string()));
    }
    if !state.venue_allowed {
        return Err(BotError::Compliance("venue_not_allowed".to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BotMode;

    #[test]
    fn live_mode_requires_geoblock_check() {
        let settings = Settings {
            mode: BotMode::Live,
            ..Settings::default()
        };
        let err = check_compliance(&settings, &ComplianceState::default(), 100).unwrap_err();
        assert!(err.to_string().contains("geoblock_not_checked"));
    }

    #[test]
    fn verified_eligible_location_passes_compliance() {
        let settings = Settings {
            mode: BotMode::Live,
            ..Settings::default()
        };
        let state = ComplianceState::from_geoblock_result(false, "CA", "BC", true, 90);
        assert_eq!(
            state.eligibility_status_at(100, settings.max_geoblock_age_ms),
            EligibilityStatus::Eligible
        );
        assert_eq!(state.location_label(), "CA · BC");
        assert!(check_compliance(&settings, &state, 100).is_ok());
    }

    #[test]
    fn checked_flag_without_response_metadata_is_not_verified() {
        let state = ComplianceState {
            geoblock_checked: true,
            geoblocked: false,
            venue_allowed: true,
            ..ComplianceState::default()
        };
        assert_eq!(
            state.eligibility_status_at(100, Settings::default().max_geoblock_age_ms),
            EligibilityStatus::Unverified
        );
    }

    #[test]
    fn prohibited_conduct_is_rejected_in_paper_mode_too() {
        let state = ComplianceState {
            prohibited_conduct_flag: true,
            ..ComplianceState::default()
        };
        let err = check_compliance(&Settings::default(), &state, 100).unwrap_err();
        assert!(err.to_string().contains("prohibited_conduct"));
    }

    #[test]
    fn geoblock_freshness_accepts_boundary_and_rejects_stale_response() {
        let settings = Settings {
            mode: BotMode::Live,
            ..Settings::default()
        };
        let checked_at_ms = 1_000;
        let state = ComplianceState::from_geoblock_result(false, "CA", "BC", true, checked_at_ms);
        assert!(check_compliance(
            &settings,
            &state,
            checked_at_ms + settings.max_geoblock_age_ms
        )
        .is_ok());
        let err = check_compliance(
            &settings,
            &state,
            checked_at_ms + settings.max_geoblock_age_ms + 1,
        )
        .unwrap_err();
        assert!(err.to_string().contains("geoblock_check_stale"));
        assert_eq!(
            state.eligibility_status_at(
                checked_at_ms + settings.max_geoblock_age_ms + 1,
                settings.max_geoblock_age_ms
            ),
            EligibilityStatus::Stale
        );
    }

    #[test]
    fn geoblock_timestamp_from_the_future_is_rejected() {
        let settings = Settings {
            mode: BotMode::Live,
            ..Settings::default()
        };
        let state = ComplianceState::from_geoblock_result(false, "CA", "BC", true, 101);
        let err = check_compliance(&settings, &state, 100).unwrap_err();
        assert!(err.to_string().contains("geoblock_timestamp_in_future"));
        assert_eq!(
            state.eligibility_status_at(100, settings.max_geoblock_age_ms),
            EligibilityStatus::InvalidFuture
        );
    }

    #[test]
    fn compliance_latch_is_durable_locked_and_bound_to_controlled_wallets() {
        let path = std::env::temp_dir().join(format!(
            "polymarket-compliance-latch-{}.json",
            uuid::Uuid::new_v4()
        ));
        let wallet = "0x1111111111111111111111111111111111111111".to_string();
        let mut guard = ComplianceGuard::open(&path, std::slice::from_ref(&wallet), 1).unwrap();
        assert!(ComplianceGuard::open(&path, std::slice::from_ref(&wallet), 1).is_err());
        guard.latch_confidential("confidential_source", 2).unwrap();
        assert!(guard.state().confidential_info_flag);
        drop(guard);

        let guard = ComplianceGuard::open(&path, std::slice::from_ref(&wallet), 3).unwrap();
        assert!(guard.state().confidential_info_flag);
        assert!(guard
            .state()
            .reasons
            .contains(&"confidential_source".to_string()));
        drop(guard);
        let other = "0x2222222222222222222222222222222222222222".to_string();
        assert!(ComplianceGuard::open(&path, &[other], 4).is_err());

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("json.lock"));
    }
}
