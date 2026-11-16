fn account_pool_config(root: &Path) -> Config {
    let mut config = build_config(root);
    for name in ["a", "b"] {
        config.accounts.insert(
            name.into(),
            crate::config::AccountConfig {
                harness: "claude".into(),
                env: BTreeMap::from([("CLAUDE_CONFIG_DIR".into(), format!("placeholder-{name}"))]),
                max_parallel: Some(1),
            },
        );
    }
    config
}

fn account_record(db: &StateStore, cfg: &Config, id: &str, root: &Path) -> Result<()> {
    let mut session = build_session(id, SessionState::Pending, Utc::now());
    session.working_dir = root.to_path_buf();
    db.insert_session(&session)?;
    db.assign_session_account(cfg, id, "claude", cfg.requested_account.as_deref(), false)?;
    Ok(())
}

#[test]
fn account_least_busy_counts_pending_and_running_and_skips_full() -> Result<()> {
    let root = TestDir::new("accounts-counts")?;
    let cfg = account_pool_config(root.path());
    let db = StateStore::open(&cfg.db_path)?;
    account_record(&db, &cfg, "first", root.path())?;
    assert_eq!(db.session_account("first")?.as_deref(), Some("a"));
    db.update_state("first", &SessionState::Running)?;
    account_record(&db, &cfg, "second", root.path())?;
    assert_eq!(db.session_account("second")?.as_deref(), Some("b"));
    account_record(&db, &cfg, "third", root.path())?;
    assert_eq!(db.session_account("third")?, None);
    assert_eq!(
        db.get_session("third")?.unwrap().state,
        SessionState::Pending
    );
    assert_eq!(
        db.list_sessions()?
            .iter()
            .filter(|session| db.account_wait_worktree(&session.id).unwrap().is_some())
            .count(),
        1
    );
    Ok(())
}

#[test]
fn account_round_robin_cursor_survives_store_reopen() -> Result<()> {
    let root = TestDir::new("accounts-round-robin")?;
    let mut cfg = account_pool_config(root.path());
    cfg.account_strategy = crate::config::AccountStrategy::RoundRobin;
    let db = StateStore::open(&cfg.db_path)?;
    account_record(&db, &cfg, "first", root.path())?;
    db.update_state("first", &SessionState::Running)?;
    db.update_state("first", &SessionState::Completed)?;
    drop(db);
    let db = StateStore::open(&cfg.db_path)?;
    account_record(&db, &cfg, "second", root.path())?;
    assert_eq!(db.session_account("second")?.as_deref(), Some("b"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_all_full_start_is_pending_without_spawning() -> Result<()> {
    let root = TestDir::new("accounts-start-full")?;
    let mut cfg = account_pool_config(root.path());
    let db = StateStore::open(&cfg.db_path)?;
    account_record(&db, &cfg, "first", root.path())?;
    account_record(&db, &cfg, "second", root.path())?;
    cfg.requested_account = Some("b".into());
    let id = queue_session_in_dir_with_runner_program(
        &db,
        &cfg,
        "wait for account",
        "claude",
        false,
        root.path(),
        Path::new("/missing-runner-must-not-be-spawned"),
        None,
        None,
        SessionGrouping::default(),
    )
    .await?;
    assert_eq!(db.get_session(&id)?.unwrap().state, SessionState::Pending);
    assert_eq!(db.session_account(&id)?, None);
    assert_eq!(db.account_wait_worktree(&id)?, Some(false));
    assert_eq!(db.session_account_pin(&id)?.as_deref(), Some("b"));
    Ok(())
}

#[test]
fn account_pin_succeeds_and_rejects_unknown_or_mismatched_harness() -> Result<()> {
    let root = TestDir::new("accounts-pins")?;
    let mut cfg = account_pool_config(root.path());
    cfg.requested_account = Some("b".into());
    let db = StateStore::open(&cfg.db_path)?;
    account_record(&db, &cfg, "pinned", root.path())?;
    assert_eq!(db.session_account("pinned")?.as_deref(), Some("b"));
    let error = cfg
        .validate_account_pin("claude", Some("missing"))
        .unwrap_err()
        .to_string();
    assert!(error.contains("missing") && error.contains("claude"));
    let error = cfg
        .validate_account_pin("codex", Some("b"))
        .unwrap_err()
        .to_string();
    assert!(error.contains("b") && error.contains("claude") && error.contains("codex"));
    Ok(())
}

#[test]
fn account_old_schema_database_with_row_opens_with_empty_account() -> Result<()> {
    let root = TestDir::new("accounts-old-schema")?;
    let database_path = root.path().join("legacy.db");
    let connection = rusqlite::Connection::open(&database_path)?;
    connection.execute_batch(
        "CREATE TABLE sessions (
            id TEXT PRIMARY KEY,
            task TEXT NOT NULL,
            project TEXT NOT NULL DEFAULT '',
            task_group TEXT NOT NULL DEFAULT '',
            agent_type TEXT NOT NULL,
            harness TEXT NOT NULL DEFAULT 'unknown',
            detected_harnesses_json TEXT NOT NULL DEFAULT '[]',
            working_dir TEXT NOT NULL DEFAULT '.',
            state TEXT NOT NULL DEFAULT 'pending',
            pid INTEGER,
            worktree_path TEXT,
            worktree_branch TEXT,
            worktree_base TEXT,
            input_tokens INTEGER DEFAULT 0,
            output_tokens INTEGER DEFAULT 0,
            tokens_used INTEGER DEFAULT 0,
            tool_calls INTEGER DEFAULT 0,
            files_changed INTEGER DEFAULT 0,
            duration_secs INTEGER DEFAULT 0,
            cost_usd REAL DEFAULT 0.0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_heartbeat_at TEXT NOT NULL
        );
        INSERT INTO sessions (id, task, agent_type, created_at, updated_at, last_heartbeat_at)
        VALUES ('legacy', 'old task', 'claude', '2026-01-01T00:00:00Z',
                '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');",
    )?;
    drop(connection);
    let db = StateStore::open(&database_path)?;
    assert_eq!(db.get_session("legacy")?.unwrap().task, "old task");
    assert_eq!(db.session_account("legacy")?, None);
    assert!(
        db.list_sessions()?
            .iter()
            .all(|session| db.account_wait_worktree(&session.id).unwrap().is_none())
    );
    Ok(())
}

#[test]
fn account_assignments_are_atomic_between_connections() -> Result<()> {
    let root = TestDir::new("accounts-concurrency")?;
    let mut cfg = account_pool_config(root.path());
    cfg.accounts.remove("b");
    let db = StateStore::open(&cfg.db_path)?;
    for id in ["first", "second"] {
        db.insert_session(&build_session(id, SessionState::Pending, Utc::now()))?;
    }
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let handles = ["first", "second"].map(|id| {
        let cfg = cfg.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || -> Result<bool> {
            let db = StateStore::open(&cfg.db_path)?;
            barrier.wait();
            db.assign_session_account(&cfg, id, "claude", None, false)
        })
    });
    let reserved = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(reserved.iter().filter(|reserved| **reserved).count(), 1);
    assert_eq!(
        db.list_sessions()?
            .iter()
            .filter(|session| db.account_wait_worktree(&session.id).unwrap().is_some())
            .count(),
        1
    );
    Ok(())
}

#[test]
fn account_without_config_adds_no_environment_or_wait_queue() -> Result<()> {
    let root = TestDir::new("accounts-none")?;
    let cfg = build_config(root.path());
    let db = StateStore::open(&cfg.db_path)?;
    account_record(&db, &cfg, "plain", root.path())?;
    assert_eq!(db.session_account("plain")?, None);
    assert!(
        db.list_sessions()?
            .iter()
            .all(|session| db.account_wait_worktree(&session.id).unwrap().is_none())
    );
    for harness in ["claude", "codex", "opencode", "gemini"] {
        let command = build_agent_command(
            &cfg,
            harness,
            Path::new(harness),
            "task",
            "plain",
            root.path(),
            None,
        );
        let env = command.as_std().get_envs().collect::<Vec<_>>();
        for key in [
            "CLAUDE_CONFIG_DIR",
            "CODEX_HOME",
            "OPENCODE_CONFIG_DIR",
            "GEMINI_CLI_HOME",
        ] {
            assert!(!env.iter().any(|(name, _)| *name == key));
        }
    }
    let status = get_status(&db, &cfg, "plain")?.to_string();
    assert!(status.contains("Account: -"));
    println!("{status}");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_assignment_applies_env_and_secrets_never_reach_sinks() -> Result<()> {
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    #[derive(Clone)]
    struct LogWriter(Arc<Mutex<Vec<u8>>>);
    impl Write for LogWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let root = TestDir::new("accounts-secrets")?;
    let mut cfg = account_pool_config(root.path());
    let marker = "account-env-secret-marker-4f068e";
    cfg.accounts
        .get_mut("a")
        .unwrap()
        .env
        .insert("CLAUDE_CONFIG_DIR".into(), marker.into());
    let script = root.path().join("runner.sh");
    fs::write(
        &script,
        "if [ \"$CLAUDE_CONFIG_DIR\" = \"$EXPECTED_ACCOUNT\" ]; then printf 'account-env-ok\\n'; else exit 7; fi\nprintf '%s\\n' \"$CLAUDE_CONFIG_DIR\"\nprintf '%s\\n' \"$CLAUDE_CONFIG_DIR\" >&2\n",
    )?;
    cfg.harness_runners.insert(
        "claude".into(),
        crate::config::HarnessRunnerConfig {
            program: "/bin/sh".into(),
            base_args: vec![script.to_string_lossy().into_owned()],
            env: BTreeMap::from([
                ("CLAUDE_CONFIG_DIR".into(), "runner-must-lose".into()),
                ("EXPECTED_ACCOUNT".into(), marker.into()),
            ]),
            ..Default::default()
        },
    );
    let logs = Arc::new(Mutex::new(Vec::new()));
    let writer = LogWriter(logs.clone());
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    tracing::debug!(config = ?cfg, "test resolved config");
    let db = StateStore::open(&cfg.db_path)?;
    let id = queue_session_in_dir_with_runner_program(
        &db,
        &cfg,
        "test env",
        "claude",
        false,
        root.path(),
        Path::new("/usr/bin/true"),
        None,
        None,
        SessionGrouping::default(),
    )
    .await?;
    assert_eq!(db.session_account(&id)?.as_deref(), Some("a"));
    run_session(&cfg, &id, "test env", "claude", root.path()).await?;
    assert_eq!(db.get_session(&id)?.unwrap().state, SessionState::Completed);
    let output = db
        .get_output_lines(&id, 100)?
        .into_iter()
        .map(|line| line.text)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        output.contains("account-env-ok"),
        "the child must see account env overriding runner env"
    );
    assert!(!output.contains(marker));
    let status = get_status(&db, &cfg, &id)?.to_string();
    println!("{status}");
    assert!(status.contains("Account: a"));
    assert!(!status.contains(marker));
    let log_output = String::from_utf8(logs.lock().unwrap().clone())?;
    assert!(
        log_output.contains("test resolved config"),
        "log capture must not be vacuous"
    );
    assert!(!log_output.contains(marker));
    cfg.accounts
        .get_mut("b")
        .unwrap()
        .env
        .insert("EXPECTED_ACCOUNT".into(), "placeholder-b".into());
    cfg.requested_account = Some("b".into());
    let second = queue_session_in_dir_with_runner_program(
        &db,
        &cfg,
        "second account",
        "claude",
        false,
        root.path(),
        Path::new("/usr/bin/true"),
        None,
        None,
        SessionGrouping::default(),
    )
    .await?;
    run_session(&cfg, &second, "second account", "claude", root.path()).await?;
    assert_eq!(db.session_account(&second)?.as_deref(), Some("b"));
    assert_eq!(
        db.get_session(&second)?.unwrap().state,
        SessionState::Completed
    );
    assert!(
        db.get_output_lines(&second, 100)?
            .iter()
            .any(|line| line.text == "account-env-ok")
    );
    let database = fs::read(&cfg.db_path)?;
    assert!(
        !database
            .windows(marker.len())
            .any(|bytes| bytes == marker.as_bytes())
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_rebalance_crosses_accounts_and_preserves_assigned_running_and_pending()
-> Result<()> {
    for all_teams in [false, true] {
        for state in [SessionState::Running, SessionState::Pending] {
            let root = TestDir::new("accounts-rebalance")?;
            let mut cfg = account_pool_config(root.path());
            cfg.max_parallel_sessions = 2;
            cfg.accounts.get_mut("a").unwrap().max_parallel = None;
            let db = StateStore::open(&cfg.db_path)?;
            let mut pin_a = cfg.clone();
            pin_a.requested_account = Some("a".into());
            account_record(&db, &pin_a, "lead", root.path())?;
            account_record(&db, &pin_a, "busy", root.path())?;
            db.update_state("busy", &state)?;
            let mut pin_b = cfg.clone();
            pin_b.requested_account = Some("b".into());
            account_record(&db, &pin_b, "idle", root.path())?;
            db.update_state("idle", &SessionState::Running)?;
            db.update_state("idle", &SessionState::Idle)?;
            db.send_native_assignment_notice("lead", "busy", "{\"task\":\"owned task\",\"context\":\"Assigned by lead [claude] | cwd /repo | spawned new delegate\"}")?;
            db.send_message(
                "lead",
                "busy",
                "{\"task\":\"unassigned intake\",\"context\":\"Delegated from lead\"}",
                "task_handoff",
            )?;
            db.send_message(
                "lead",
                "idle",
                "{\"task\":\"old task\",\"context\":\"Delegated from lead\"}",
                "task_handoff",
            )?;
            db.mark_messages_read("idle")?;
            let moved = if all_teams {
                rebalance_all_teams(&db, &cfg, "claude", false, 10)
                    .await?
                    .into_iter()
                    .flat_map(|outcome| outcome.rerouted)
                    .collect::<Vec<_>>()
            } else {
                rebalance_team_backlog(&db, &cfg, "lead", "claude", false, 10).await?
            };
            assert_eq!(moved.len(), 1);
            assert_eq!(moved[0].session_id, "idle");
            assert_eq!(db.session_account("busy")?.as_deref(), Some("a"));
            assert_eq!(db.session_account("idle")?.as_deref(), Some("b"));
            let messages = db.list_messages_for_session("idle", 100)?;
            assert!(
                messages
                    .iter()
                    .any(|message| message.content.contains("unassigned intake"))
            );
            assert!(
                !messages
                    .iter()
                    .any(|message| message.content.contains("owned task"))
            );
            assert_eq!(db.unread_task_handoff_count("busy")?, 1);
            assert_eq!(db.unread_message_counts()?.get("busy"), Some(&1));
        }
    }
    Ok(())
}
