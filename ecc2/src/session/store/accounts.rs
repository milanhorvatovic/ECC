use super::*;
use crate::session::accounts::{AccountLoad, select_account};

impl StateStore {
    pub fn session_account(&self, id: &str) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row("SELECT account FROM sessions WHERE id = ?1", [id], |row| {
                row.get(0)
            })?)
    }

    pub fn session_account_pin(&self, id: &str) -> Result<Option<String>> {
        Ok(self.conn.query_row(
            "SELECT account_pin FROM sessions WHERE id = ?1",
            [id],
            |row| row.get(0),
        )?)
    }

    // NULL means no account wait. A boolean preserves the requested worktree
    // until explicit resume; the daemon reads only pending_worktree_queue.
    pub fn account_wait_worktree(&self, id: &str) -> Result<Option<bool>> {
        Ok(self.conn.query_row(
            "SELECT account_wait_worktree FROM sessions WHERE id = ?1",
            [id],
            |row| row.get(0),
        )?)
    }

    pub fn prepare_account_resume(&self, cfg: &Config, id: &str, harness: &str) -> Result<()> {
        let stored_pin = self.session_account_pin(id)?;
        let pin = cfg.requested_account.as_deref().or(stored_pin.as_deref());
        cfg.validate_account_pin(harness, pin)?;
        self.conn.execute(
            "UPDATE sessions SET account = NULL, account_pin = ?2 WHERE id = ?1",
            rusqlite::params![id, pin],
        )?;
        Ok(())
    }

    pub fn assign_session_account(
        &self,
        cfg: &Config,
        id: &str,
        harness: &str,
        requested: Option<&str>,
        use_worktree: bool,
    ) -> Result<bool> {
        let tx = rusqlite::Transaction::new_unchecked(
            &self.conn,
            rusqlite::TransactionBehavior::Immediate,
        )?;
        let stored_pin = self.session_account_pin(id)?;
        let pin = requested.or(stored_pin.as_deref());
        cfg.validate_account_pin(harness, pin)?;
        let harness = HarnessKind::canonical_agent_type(harness);
        let mut loads = Vec::new();
        for (name, account) in &cfg.accounts {
            if HarnessKind::canonical_agent_type(&account.harness) != harness
                || pin.is_some_and(|pin| pin != name)
            {
                continue;
            }
            let active: i64 = tx.query_row(
                "SELECT COUNT(*) FROM sessions WHERE account = ?1 AND id != ?2 AND state IN ('pending', 'running')",
                rusqlite::params![name, id], |row| row.get(0),
            )?;
            loads.push(AccountLoad {
                name,
                active: active as usize,
                max_parallel: account.max_parallel,
            });
        }
        if loads.is_empty() && pin.is_none() {
            tx.execute(
                "UPDATE sessions SET account = NULL, account_wait_worktree = NULL WHERE id = ?1",
                [id],
            )?;
            tx.commit()?;
            return Ok(true);
        }
        let cursor: Option<String> = tx
            .query_row(
                "SELECT account FROM account_pool_cursors WHERE harness = ?1",
                [&harness],
                |row| row.get(0),
            )
            .optional()?;
        let selected = select_account(&loads, cfg.account_strategy, cursor.as_deref());
        tx.execute(
            "UPDATE sessions SET account = ?2, account_pin = ?3, account_wait_worktree = ?4 WHERE id = ?1",
            rusqlite::params![id, selected, pin, selected.is_none().then_some(use_worktree)],
        )?;
        if pin.is_none() && cfg.account_strategy == crate::config::AccountStrategy::RoundRobin {
            if let Some(name) = selected {
                tx.execute("INSERT INTO account_pool_cursors (harness, account) VALUES (?1, ?2) ON CONFLICT(harness) DO UPDATE SET account = excluded.account", rusqlite::params![harness, name])?;
            }
        }
        tx.commit()?;
        Ok(selected.is_some())
    }
}
