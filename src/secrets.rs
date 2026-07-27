use std::collections::BTreeMap;

use crate::types::WalletMode;

pub const COMMON_LIVE_RUNTIME_SECRETS: &[&str] = &[
    "PRIVATE_KEY",
    "POLYMARKET_API_KEY",
    "POLYMARKET_API_SECRET",
    "POLYMARKET_API_PASSPHRASE",
];

pub const DEPOSIT_WALLET_SECRET: &str = "DEPOSIT_WALLET_ADDRESS";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretKeyStatus {
    pub name: String,
    pub present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretStatus {
    pub keys: Vec<SecretKeyStatus>,
}

impl SecretStatus {
    pub fn from_environment(wallet_mode: WalletMode) -> Self {
        Self::from_lookup(wallet_mode, |key| std::env::var(key).ok())
    }

    pub fn from_lookup(
        wallet_mode: WalletMode,
        mut lookup: impl FnMut(&str) -> Option<String>,
    ) -> Self {
        let keys = required_secret_names(wallet_mode)
            .into_iter()
            .map(|name| SecretKeyStatus {
                name: name.to_string(),
                present: lookup(name)
                    .map(|value| !value.trim().is_empty())
                    .unwrap_or(false),
            })
            .collect();
        Self { keys }
    }

    pub fn missing_names(&self) -> Vec<String> {
        self.keys
            .iter()
            .filter(|key| !key.present)
            .map(|key| key.name.clone())
            .collect()
    }

    pub fn present_count(&self) -> usize {
        self.keys.iter().filter(|key| key.present).count()
    }

    pub fn total_count(&self) -> usize {
        self.keys.len()
    }

    pub fn as_display_map(&self) -> BTreeMap<String, &'static str> {
        self.keys
            .iter()
            .map(|key| {
                (
                    key.name.clone(),
                    if key.present { "configured" } else { "missing" },
                )
            })
            .collect()
    }
}

pub fn required_secret_names(wallet_mode: WalletMode) -> Vec<&'static str> {
    let mut names = COMMON_LIVE_RUNTIME_SECRETS.to_vec();
    if wallet_mode == WalletMode::DepositWallet {
        names.push(DEPOSIT_WALLET_SECRET);
    }
    names
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_missing_secret_names_without_exposing_values() {
        let status = SecretStatus::from_lookup(WalletMode::DepositWallet, |key| match key {
            "PRIVATE_KEY" => Some("0xabc".to_string()),
            "POLYMARKET_API_KEY" => Some("key".to_string()),
            _ => None,
        });
        assert_eq!(status.present_count(), 2);
        assert!(status
            .missing_names()
            .contains(&"POLYMARKET_API_SECRET".to_string()));
        assert!(!format!("{status:?}").contains("0xabc"));
    }

    #[test]
    fn wallet_modes_require_only_their_runtime_secrets() {
        for wallet_mode in [WalletMode::GnosisSafe, WalletMode::Proxy, WalletMode::Eoa] {
            let names = required_secret_names(wallet_mode);
            assert_eq!(names.len(), COMMON_LIVE_RUNTIME_SECRETS.len());
            assert!(!names.contains(&DEPOSIT_WALLET_SECRET));
        }

        let deposit_names = required_secret_names(WalletMode::DepositWallet);
        assert_eq!(deposit_names.len(), COMMON_LIVE_RUNTIME_SECRETS.len() + 1);
        assert!(deposit_names.contains(&DEPOSIT_WALLET_SECRET));
    }
}
