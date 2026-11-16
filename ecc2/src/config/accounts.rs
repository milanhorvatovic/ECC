use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AccountStrategy {
    #[default]
    LeastBusy,
    RoundRobin,
}

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountConfig {
    pub harness: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub max_parallel: Option<usize>,
}

impl fmt::Debug for AccountConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccountConfig")
            .field("harness", &self.harness)
            .field("env", &redacted_env(&self.env))
            .field("max_parallel", &self.max_parallel)
            .finish()
    }
}

pub(super) fn redacted_env(env: &BTreeMap<String, String>) -> BTreeMap<&str, &str> {
    env.keys().map(|key| (key.as_str(), "[REDACTED]")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn account_config_parses_builtin_custom_and_name_order() -> anyhow::Result<()> {
        let config: Config = toml::from_str(
            r#"
account_strategy = "round-robin"
[accounts.zeta]
harness = "claude"
max_parallel = 2
env = { CLAUDE_CONFIG_DIR = "~/.claude-accounts/work" }
[accounts.alpha]
harness = "custom"
env = {}
[harness_runners.custom]
program = "custom-agent"
"#,
        )?;
        config.validate_accounts()?;
        assert_eq!(config.account_strategy, AccountStrategy::RoundRobin);
        assert_eq!(
            config.accounts.keys().collect::<Vec<_>>(),
            vec!["alpha", "zeta"]
        );
        assert_eq!(config.accounts["zeta"].max_parallel, Some(2));
        assert_eq!(
            Config::default().account_strategy,
            AccountStrategy::LeastBusy
        );
        Ok(())
    }

    #[test]
    fn account_config_rejects_unknown_harness() -> anyhow::Result<()> {
        let config: Config = toml::from_str("[accounts.work]\nharness = 'typo'")?;
        assert!(config
            .validate_accounts()
            .unwrap_err()
            .to_string()
            .contains("accounts.work"));
        Ok(())
    }

    #[test]
    fn account_config_rejects_unknown_strategy() {
        let error = toml::from_str::<Config>("account_strategy = 'random'").unwrap_err();
        assert!(error.to_string().contains("account_strategy"));
    }

    #[test]
    fn account_config_rejects_empty_or_whitespace_env_key() -> anyhow::Result<()> {
        for key in ["", "  ", "\t"] {
            let mut config = Config::default();
            config.accounts.insert(
                "work".into(),
                AccountConfig {
                    harness: "claude".into(),
                    env: BTreeMap::from([(key.into(), "secret".into())]),
                    max_parallel: None,
                },
            );
            assert!(config
                .validate_accounts()
                .unwrap_err()
                .to_string()
                .contains("accounts.work"));
        }
        Ok(())
    }

    #[test]
    fn account_config_debug_redacts_account_and_runner_env() {
        let mut config = Config::default();
        let env = BTreeMap::from([("KEY".into(), "secret-debug-marker".into())]);
        config.accounts.insert(
            "work".into(),
            AccountConfig {
                harness: "claude".into(),
                env: env.clone(),
                max_parallel: None,
            },
        );
        config.harness_runners.insert(
            "claude".into(),
            crate::config::HarnessRunnerConfig {
                env,
                ..Default::default()
            },
        );
        assert!(!format!("{config:?}").contains("secret-debug-marker"));
    }

    #[test]
    fn account_config_loading_validates_entries() -> anyhow::Result<()> {
        let directory = Config::config_root();
        let file = directory.join("account-validation.toml");
        for (content, entry) in [
            ("[accounts.work]\nharness = 'missing'", "accounts.work"),
            (
                "[accounts.work]\nharness = 'claude'\nenv = { '' = 'secret' }",
                "accounts.work",
            ),
            ("account_strategy = 'random'", "account_strategy"),
        ] {
            std::fs::write(&file, content)?;
            let error = Config::load_from_paths(std::slice::from_ref(&file), &[]).unwrap_err();
            assert!(format!("{error:#}").contains(entry));
        }
        Ok(())
    }
}
