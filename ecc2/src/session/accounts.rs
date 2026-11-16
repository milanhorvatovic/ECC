#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AccountStrategy;

    #[test]
    fn account_least_busy_uses_counts_and_input_order() {
        let accounts = [
            AccountLoad {
                name: "first",
                active: 2,
                max_parallel: None,
            },
            AccountLoad {
                name: "second",
                active: 1,
                max_parallel: None,
            },
        ];
        assert_eq!(
            select_account(&accounts, AccountStrategy::LeastBusy, None),
            Some("second")
        );
        assert_eq!(
            select_account(&accounts[..1], AccountStrategy::LeastBusy, None),
            Some("first")
        );
        let tied = [
            accounts[1],
            AccountLoad {
                name: "third",
                ..accounts[1]
            },
        ];
        assert_eq!(
            select_account(&tied, AccountStrategy::LeastBusy, None),
            Some("second")
        );
    }

    #[test]
    fn account_round_robin_wraps_and_skips_full_accounts() {
        let accounts = [
            AccountLoad {
                name: "a",
                active: 0,
                max_parallel: None,
            },
            AccountLoad {
                name: "b",
                active: 1,
                max_parallel: Some(1),
            },
            AccountLoad {
                name: "c",
                active: 0,
                max_parallel: Some(1),
            },
        ];
        assert_eq!(
            select_account(&accounts, AccountStrategy::RoundRobin, None),
            Some("a")
        );
        assert_eq!(
            select_account(&accounts, AccountStrategy::RoundRobin, Some("a")),
            Some("c")
        );
        assert_eq!(
            select_account(&accounts, AccountStrategy::RoundRobin, Some("c")),
            Some("a")
        );
        assert_eq!(
            select_account(&accounts, AccountStrategy::RoundRobin, Some("removed")),
            Some("a")
        );
    }

    #[test]
    fn account_full_or_empty_pool_returns_none_for_both_strategies() {
        let accounts = [AccountLoad {
            name: "full",
            active: 1,
            max_parallel: Some(1),
        }];
        for strategy in [AccountStrategy::LeastBusy, AccountStrategy::RoundRobin] {
            assert_eq!(select_account(&accounts, strategy, None), None);
            assert_eq!(select_account(&[], strategy, None), None);
        }
    }
}
use crate::config::AccountStrategy;

#[derive(Clone, Copy)]
pub struct AccountLoad<'a> {
    pub name: &'a str,
    pub active: usize,
    pub max_parallel: Option<usize>,
}

pub fn select_account<'a>(
    eligible: &[AccountLoad<'a>],
    strategy: AccountStrategy,
    last_assigned: Option<&str>,
) -> Option<&'a str> {
    let has_capacity = |account: &&AccountLoad<'a>| {
        account
            .max_parallel
            .is_none_or(|limit| account.active < limit)
    };
    match strategy {
        AccountStrategy::LeastBusy => eligible
            .iter()
            .filter(has_capacity)
            .min_by_key(|account| account.active)
            .map(|account| account.name),
        AccountStrategy::RoundRobin => {
            let start = last_assigned
                .and_then(|last| eligible.iter().position(|account| account.name == last))
                .map_or(0, |index| index + 1);
            eligible
                .iter()
                .cycle()
                .skip(start)
                .take(eligible.len())
                .find(has_capacity)
                .map(|account| account.name)
        }
    }
}
