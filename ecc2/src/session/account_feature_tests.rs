async fn review_profile_pin(kind: &str) -> Result<()> {
    let root = TestDir::new("accounts-effective-profile")?;
    let mut cfg = account_pool_config(root.path());
    cfg.accounts.get_mut("b").unwrap().harness = "codex".into();
    cfg.agent_profiles.insert(
        "coder".into(),
        crate::config::AgentProfileConfig {
            agent: Some("codex".into()),
            ..Default::default()
        },
    );
    let db = StateStore::open(&cfg.db_path)?;
    account_record(&db, &cfg, "lead", root.path())?;
    match kind {
        "default" => cfg.default_agent_profile = Some("coder".into()),
        "inherited" => db.upsert_session_profile("lead", &cfg.resolve_agent_profile("coder")?)?,
        _ => {}
    }
    cfg.requested_account = Some("b".into());
    let result = assign_session_in_dir_with_runner_program(
        &db,
        &cfg,
        "lead",
        "code",
        "claude",
        false,
        root.path(),
        Path::new("/usr/bin/true"),
        (kind == "explicit").then_some("coder"),
        SessionGrouping::default(),
    )
    .await;
    assert!(
        result.is_ok(),
        "effective codex profile must accept codex pin: {:?}",
        result.as_ref().err()
    );
    let id = result?.session_id;
    assert_eq!(db.get_session(&id)?.unwrap().agent_type, "codex");
    assert_eq!(db.session_account(&id)?.as_deref(), Some("b"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_review_explicit_profile_pin() -> Result<()> {
    review_profile_pin("explicit").await
}
#[tokio::test(flavor = "current_thread")]
async fn account_review_default_profile_pin() -> Result<()> {
    review_profile_pin("default").await
}
#[tokio::test(flavor = "current_thread")]
async fn account_review_inherited_profile_pin() -> Result<()> {
    review_profile_pin("inherited").await
}

#[tokio::test(flavor = "current_thread")]
async fn account_review_pin_keeps_team_capacity_count() -> Result<()> {
    let root = TestDir::new("accounts-team-cap")?;
    let mut cfg = account_pool_config(root.path());
    cfg.max_parallel_sessions = 1;
    let db = StateStore::open(&cfg.db_path)?;
    account_record(&db, &cfg, "lead", root.path())?;
    cfg.requested_account = Some("b".into());
    account_record(&db, &cfg, "busy", root.path())?;
    db.update_state("busy", &SessionState::Running)?;
    db.send_native_assignment_notice("lead", "busy", "owned task")?;
    cfg.requested_account = Some("a".into());
    let preview = preview_assignment_for_task(&db, &cfg, "lead", "next", "claude")?;
    assert_eq!(preview.action, AssignmentAction::DeferredSaturated);
    let outcome = assign_session_in_dir_with_runner_program(
        &db,
        &cfg,
        "lead",
        "next",
        "claude",
        false,
        root.path(),
        Path::new("/usr/bin/true"),
        None,
        SessionGrouping::default(),
    )
    .await?;
    assert_eq!(outcome.action, AssignmentAction::DeferredSaturated);
    cfg.requested_account = None;
    assert_eq!(
        direct_delegate_sessions(&db, &cfg, &resolve_session(&db, "lead")?, "claude")?.len(),
        1
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_review_redaction_visibly_replaces_nonempty_values() -> Result<()> {
    use crate::session::runtime::capture_command_output_redacted;
    for (id, secret, raw, expected) in [
        (
            "long",
            "long-account-secret-value",
            "before long-account-secret-value after",
            "before [REDACTED] after",
        ),
        (
            "short",
            "1",
            "build 2021 finished in 10s, exit 1",
            "build 202[REDACTED] finished in [REDACTED]0s, exit [REDACTED]",
        ),
    ] {
        let root = TestDir::new("accounts-output-redaction")?;
        let cfg = build_config(root.path());
        let db = StateStore::open(&cfg.db_path)?;
        db.insert_session(&build_session(id, SessionState::Pending, Utc::now()))?;
        let output = SessionOutputStore::default();
        let mut events = output.subscribe();
        let mut command = Command::new("/usr/bin/printf");
        command.arg("%s\n").arg(raw);
        capture_command_output_redacted(
            cfg.db_path,
            id.into(),
            command,
            output,
            StdDuration::from_secs(1),
            vec![secret.into()],
        )
        .await?;
        assert_eq!(db.get_output_lines(id, 10)?[0].text, expected);
        assert_eq!(events.try_recv()?.line.text, expected);
    }
    Ok(())
}
#[test]
fn account_review_debug_preserves_every_nonsecret_field() {
    let runner = crate::config::HarnessRunnerConfig {
        program: "agent-program".into(),
        base_args: vec!["argument".into()],
        env: BTreeMap::from([("VISIBLE_ENV_KEY".into(), "hidden-env-value".into())]),
        ..Default::default()
    };
    let debug = format!("{runner:?}");
    for field in [
        "program",
        "base_args",
        "project_markers",
        "cwd_flag",
        "session_name_flag",
        "task_flag",
        "model_flag",
        "add_dir_flag",
        "include_directories_flag",
        "allowed_tools_flag",
        "disallowed_tools_flag",
        "permission_mode_flag",
        "max_budget_usd_flag",
        "append_system_prompt_flag",
        "inline_system_prompt_for_task",
        "env",
        "VISIBLE_ENV_KEY",
        "agent-program",
        "argument",
    ] {
        assert!(debug.contains(field), "Debug lost {field}: {debug}");
    }
    assert!(!debug.contains("hidden-env-value"));
    let account = crate::config::AccountConfig {
        harness: "claude".into(),
        env: runner.env,
        max_parallel: Some(2),
    };
    let debug = format!("{account:?}");
    for field in [
        "harness",
        "claude",
        "max_parallel",
        "Some(2)",
        "VISIBLE_ENV_KEY",
    ] {
        assert!(debug.contains(field), "Account Debug lost {field}: {debug}");
    }
    assert!(!debug.contains("hidden-env-value"));
}

#[tokio::test(flavor = "current_thread")]
async fn account_review_rebalance_keeps_full_team_count_across_accounts() -> Result<()> {
    let root = TestDir::new("accounts-rebalance-cap")?;
    let mut cfg = account_pool_config(root.path());
    cfg.max_parallel_sessions = 2;
    for account in cfg.accounts.values_mut() {
        account.max_parallel = None;
    }
    let db = StateStore::open(&cfg.db_path)?;
    account_record(&db, &cfg, "lead", root.path())?;
    for (id, pin) in [("busy-a", "a"), ("busy-b", "b")] {
        cfg.requested_account = Some(pin.into());
        account_record(&db, &cfg, id, root.path())?;
        db.update_state(id, &SessionState::Running)?;
        db.send_native_assignment_notice("lead", id, "owned task")?;
    }
    db.send_message("lead", "busy-b", "independent intake", "task_handoff")?;
    let moved = rebalance_team_backlog(&db, &cfg, "lead", "claude", false, 10).await?;
    assert!(moved.is_empty());
    cfg.requested_account = None;
    assert_eq!(
        direct_delegate_sessions(&db, &cfg, &resolve_session(&db, "lead")?, "claude")?.len(),
        2
    );
    assert_eq!(db.unread_task_handoff_count("busy-b")?, 2);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_revision_explicit_start_pin_survives_until_resume() -> Result<()> {
    let root = TestDir::new("accounts-real-pin")?;
    let mut cfg = account_pool_config(root.path());
    cfg.requested_account = Some("b".into());
    let db = StateStore::open(&cfg.db_path)?;
    account_record(&db, &cfg, "pinned", root.path())?;
    db.update_state("pinned", &SessionState::Running)?;
    db.update_state("pinned", &SessionState::Failed)?;
    cfg.requested_account = None;
    cfg.accounts.remove("b");
    let error = resume_session_with_program(&db, &cfg, "pinned", Some(Path::new("/usr/bin/true")))
        .await
        .unwrap_err();
    assert!(error.to_string().contains('b') && error.to_string().contains("claude"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_revision_redaction_covers_short_long_and_empty_values() -> Result<()> {
    use crate::session::runtime::capture_command_output_redacted;
    for value in ["/vault", "long-account-marker-value", "", "\n/vault\n\n"] {
        let root = TestDir::new("accounts-all-values")?;
        let cfg = build_config(root.path());
        let db = StateStore::open(&cfg.db_path)?;
        db.insert_session(&build_session("output", SessionState::Pending, Utc::now()))?;
        let output = SessionOutputStore::default();
        let mut events = output.subscribe();
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("printf '%s\\n' \"$ACCOUNT_DIR\"; printf 'runner-flag=%s\\n' \"$RUNNER_FLAG\"")
            .env("ACCOUNT_DIR", value)
            .env("RUNNER_FLAG", "1");
        capture_command_output_redacted(
            cfg.db_path,
            "output".into(),
            command,
            output,
            StdDuration::from_secs(1),
            vec![value.into()],
        )
        .await?;
        let stored = db
            .get_output_lines("output", 20)?
            .into_iter()
            .map(|line| line.text)
            .collect::<Vec<_>>();
        assert!(stored.iter().any(|line| line == "runner-flag=1"));
        for line in &stored {
            for secret in value.lines().filter(|part| !part.is_empty()) {
                assert!(
                    !line.contains(secret),
                    "stored account value leaked: {line}"
                );
            }
        }
        let mut broadcast = Vec::new();
        while let Ok(event) = events.try_recv() {
            broadcast.push(event.line.text);
        }
        assert_eq!(stored, broadcast);
        if !value.is_empty() {
            assert!(stored.iter().any(|line| line == "[REDACTED]"));
        }
    }
    Ok(())
}
#[tokio::test(flavor = "current_thread")]
async fn account_revision_zero_cap_keeps_base_no_account_fallback() -> Result<()> {
    let root = TestDir::new("accounts-base-fallback")?;
    let mut cfg = build_config(root.path());
    cfg.max_parallel_sessions = 0;
    let db = StateStore::open(&cfg.db_path)?;
    account_record(&db, &cfg, "lead", root.path())?;
    assert_eq!(
        preview_assignment_for_task(&db, &cfg, "lead", "task", "claude")?.action,
        AssignmentAction::Spawned
    );
    let outcome = assign_session_in_dir_with_runner_program(
        &db,
        &cfg,
        "lead",
        "task",
        "claude",
        false,
        root.path(),
        Path::new("/usr/bin/true"),
        None,
        SessionGrouping::default(),
    )
    .await?;
    assert_eq!(outcome.action, AssignmentAction::Spawned);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_serial_no_accounts_profile_reuses_base_delegate() -> Result<()> {
    let root = TestDir::new("accounts-profile-routing")?;
    let mut cfg = build_config(root.path());
    cfg.agent_profiles.insert(
        "coder".into(),
        crate::config::AgentProfileConfig {
            agent: Some("codex".into()),
            ..Default::default()
        },
    );
    cfg.default_agent_profile = Some("coder".into());
    let db = StateStore::open(&cfg.db_path)?;
    for (id, state) in [
        ("lead", SessionState::Running),
        ("idle", SessionState::Idle),
    ] {
        let mut session = build_session(id, state, Utc::now());
        session.working_dir = root.path().into();
        db.insert_session(&session)?;
    }
    db.send_message("lead", "idle", "old task", "task_handoff")?;
    for message in db.unread_task_handoffs_for_session("idle", 10)? {
        db.mark_message_read(message.id)?;
    }
    let preview = preview_assignment_for_task(&db, &cfg, "lead", "next", "claude")?;
    let outcome = assign_session_in_dir_with_runner_program(
        &db,
        &cfg,
        "lead",
        "next",
        "claude",
        false,
        root.path(),
        Path::new("/usr/bin/true"),
        None,
        SessionGrouping::default(),
    )
    .await?;
    println!(
        "preview={:?}; assigned={}",
        preview.session_id, outcome.session_id
    );
    assert_eq!(outcome.session_id, "idle");
    assert_eq!(preview.session_id.as_deref(), Some("idle"));
    Ok(())
}

#[test]
fn account_serial_least_busy_ignores_inactive_sessions_without_capacity_limits() -> Result<()> {
    let root = TestDir::new("accounts-inactive-count")?;
    let mut cfg = account_pool_config(root.path());
    for account in cfg.accounts.values_mut() {
        account.max_parallel = None;
    }
    let db = StateStore::open(&cfg.db_path)?;
    cfg.requested_account = Some("a".into());
    for (id, state) in [
        ("idle", SessionState::Idle),
        ("idle-2", SessionState::Idle),
        ("idle-3", SessionState::Idle),
        ("done", SessionState::Completed),
        ("done-2", SessionState::Completed),
        ("done-3", SessionState::Completed),
        ("stopped", SessionState::Stopped),
        ("stopped-2", SessionState::Stopped),
        ("stopped-3", SessionState::Stopped),
    ] {
        account_record(&db, &cfg, id, root.path())?;
        db.update_state(id, &SessionState::Running)?;
        db.update_state(id, &state)?;
    }
    cfg.requested_account = Some("b".into());
    account_record(&db, &cfg, "pending", root.path())?;
    account_record(&db, &cfg, "running", root.path())?;
    db.update_state("running", &SessionState::Running)?;
    cfg.requested_account = None;
    account_record(&db, &cfg, "choice", root.path())?;
    assert_eq!(db.session_account("choice")?.as_deref(), Some("a"));
    Ok(())
}

#[test]
fn account_ninth_round_robin_preserves_full_cursor_position() -> Result<()> {
    let root = TestDir::new("round-robin-full-cursor")?;
    let mut cfg = account_pool_config(root.path());
    cfg.account_strategy = crate::config::AccountStrategy::RoundRobin;
    cfg.accounts.insert("c".into(), cfg.accounts["a"].clone());
    let db = StateStore::open(&cfg.db_path)?;
    account_record(&db, &cfg, "first", root.path())?;
    assert_eq!(db.session_account("first")?.as_deref(), Some("a"));
    db.update_state("first", &SessionState::Stopped)?;
    account_record(&db, &cfg, "second", root.path())?;
    assert_eq!(db.session_account("second")?.as_deref(), Some("b"));
    drop(db);
    let db = StateStore::open(&cfg.db_path)?;
    account_record(&db, &cfg, "third", root.path())?;
    println!(
        "round-robin full cursor b; third={:?}",
        db.session_account("third")?
    );
    assert_eq!(db.session_account("third")?.as_deref(), Some("c"));
    db.update_state("third", &SessionState::Stopped)?;
    account_record(&db, &cfg, "fourth", root.path())?;
    assert_eq!(db.session_account("fourth")?.as_deref(), Some("a"));
    Ok(())
}

#[test]
fn account_ninth_round_robin_all_full_patterns_keep_ring_order() -> Result<()> {
    let names = ["a", "b", "c", "d"];
    let mut checked = 0;
    for full_mask in 0..16 {
        let root = TestDir::new("round-robin-capacity-ring")?;
        let mut cfg = account_pool_config(root.path());
        cfg.account_strategy = crate::config::AccountStrategy::RoundRobin;
        for name in ["c", "d"] {
            cfg.accounts.insert(name.into(), cfg.accounts["a"].clone());
        }
        let db = StateStore::open(&cfg.db_path)?;
        for (index, name) in names.iter().enumerate() {
            if full_mask & (1 << index) != 0 {
                let mut pinned = cfg.clone();
                pinned.requested_account = Some((*name).into());
                account_record(&db, &pinned, &format!("held-{name}"), root.path())?;
            }
        }
        let sql = rusqlite::Connection::open(&cfg.db_path)?;
        for cursor in 0..names.len() {
            sql.execute("INSERT INTO account_pool_cursors (harness, account) VALUES ('claude', ?1) ON CONFLICT(harness) DO UPDATE SET account = excluded.account", [names[cursor]])?;
            let id = format!("candidate-{cursor}");
            account_record(&db, &cfg, &id, root.path())?;
            let expected = (1..=names.len())
                .map(|step| (cursor + step) % names.len())
                .find(|index| full_mask & (1 << index) == 0)
                .map(|index| names[index]);
            let actual = db.session_account(&id)?;
            assert_eq!(
                actual.as_deref(),
                expected,
                "mask={full_mask}, cursor={cursor}"
            );
            db.update_state(&id, &SessionState::Stopped)?;
            checked += 1;
        }
    }
    println!("round-robin production ring cases checked={checked}");
    Ok(())
}

#[test]
fn account_tenth_pins_leave_unpinned_round_robin_cursor_unchanged() -> Result<()> {
    for persisted in [false, true] {
        let root = TestDir::new("round-robin-independent-pin")?;
        let mut cfg = account_pool_config(root.path());
        cfg.account_strategy = crate::config::AccountStrategy::RoundRobin;
        cfg.accounts.insert("c".into(), cfg.accounts["a"].clone());
        for account in cfg.accounts.values_mut() {
            account.max_parallel = None;
        }
        let db = StateStore::open(&cfg.db_path)?;
        account_record(&db, &cfg, "u1", root.path())?;
        if persisted {
            let mut plain = cfg.clone();
            plain.accounts.clear();
            account_record(&db, &plain, "p1", root.path())?;
            db.assign_session_account(&cfg, "p1", "claude", Some("c"), false)?;
            db.assign_session_account(&cfg, "p1", "claude", None, false)?;
        } else {
            let mut pinned = cfg.clone();
            pinned.requested_account = Some("c".into());
            account_record(&db, &pinned, "p1", root.path())?;
        }
        drop(db);
        let db = StateStore::open(&cfg.db_path)?;
        account_record(&db, &cfg, "u2", root.path())?;
        println!(
            "round-robin pin persisted={persisted}; u1={:?}; p1={:?}; u2={:?}",
            db.session_account("u1")?,
            db.session_account("p1")?,
            db.session_account("u2")?
        );
        assert_eq!(db.session_account("u1")?.as_deref(), Some("a"));
        assert_eq!(db.session_account("p1")?.as_deref(), Some("c"));
        assert_eq!(db.session_account("u2")?.as_deref(), Some("b"));
    }
    Ok(())
}

