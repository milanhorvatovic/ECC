#[tokio::test(flavor = "current_thread")]
async fn account_uncovered_guard_failed_worktree_matches_no_table() -> Result<()> {
    for profile_agent in [None, Some("codex")] {
        let mut rows = Vec::new();
        for has_accounts in [false, true] {
            let root = TestDir::new("uncovered-worktree-failure")?;
            let mut cfg = account_pool_config(root.path());
            if !has_accounts {
                cfg.accounts.clear();
            }
            let db = StateStore::open(&cfg.db_path)?;
            let profile = profile_agent.map(|agent: &str| SessionAgentProfile {
                agent: Some(agent.into()),
                ..Default::default()
            });
            let error = queue_session_with_resolved_profile_and_runner_program(
                &db,
                &cfg,
                "task",
                if profile.is_some() { "claude" } else { "codex" },
                true,
                root.path(),
                Path::new("/missing-must-not-spawn"),
                profile,
                SessionGrouping::default(),
            )
            .await
            .unwrap_err();
            assert!(format!("{error:#}").contains("git"), "{error:#}");
            rows.push(db.list_sessions()?.len());
        }
        assert_eq!(rows, [0, 0], "profile override={profile_agent:?}");
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_uncovered_guard_successful_worktree_uses_effective_harness() -> Result<()> {
    for (requested, profile_agent, expected_account) in [
        ("codex", None, None),
        ("claude", Some("codex"), None),
        ("claude", None, Some("a")),
        ("codex", Some(" Claude-Code "), Some("a")),
    ] {
        let root = TestDir::new("uncovered-worktree-success")?;
        init_git_repo(root.path())?;
        let mut cfg = account_pool_config(root.path());
        cfg.accounts.get_mut("a").unwrap().harness = " Claude-Code ".into();
        let db = StateStore::open(&cfg.db_path)?;
        let runner = root.path().join("runner.sh");
        fs::write(
            &runner,
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$0.log.tmp\" && mv \"$0.log.tmp\" \"$0.log\"\n",
        )?;
        fs::set_permissions(&runner, fs::Permissions::from_mode(0o755))?;
        let profile = profile_agent.map(|agent: &str| SessionAgentProfile {
            agent: Some(agent.into()),
            ..Default::default()
        });
        let id = queue_session_with_resolved_profile_and_runner_program(
            &db,
            &cfg,
            "task",
            requested,
            true,
            root.path(),
            &runner,
            profile,
            SessionGrouping::default(),
        )
        .await?;
        assert_eq!(db.session_account(&id)?.as_deref(), expected_account);
        assert_eq!(db.account_wait_worktree(&id)?, None);
        assert_eq!(account_wait_message(&db, &id)?, None);
        assert!(db.get_session(&id)?.unwrap().worktree.is_some());
        assert!(wait_for_file(&runner.with_extension("sh.log"))?.contains(&id));
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_uncovered_guard_resume_leaves_account_columns_untouched() -> Result<()> {
    for has_accounts in [false, true] {
        let root = TestDir::new("uncovered-resume-guard")?;
        let mut cfg = account_pool_config(root.path());
        if !has_accounts {
            cfg.accounts.clear();
        }
        let db = StateStore::open(&cfg.db_path)?;
        let mut session = build_session("resume", SessionState::Stopped, Utc::now());
        session.agent_type = "codex".into();
        session.working_dir = root.path().into();
        db.insert_session(&session)?;
        // An uncovered resume must not depend on writes to account columns.
        rusqlite::Connection::open(&cfg.db_path)?.execute_batch(
            "CREATE TRIGGER no_account_reassignment BEFORE UPDATE OF account, account_pin ON sessions
             BEGIN SELECT RAISE(ABORT, 'unexpected account reassignment'); END;",
        )?;
        let runner = root.path().join("runner.sh");
        fs::write(
            &runner,
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$0.log.tmp\" && mv \"$0.log.tmp\" \"$0.log\"\n",
        )?;
        fs::set_permissions(&runner, fs::Permissions::from_mode(0o755))?;
        let resumed = resume_session_with_program(&db, &cfg, &session.id, Some(&runner)).await?;
        assert_eq!(resumed, session.id);
        assert_eq!(db.get_session(&resumed)?.unwrap().state, SessionState::Pending);
        assert_eq!(db.session_account(&resumed)?, None);
        assert_eq!(db.session_account_pin(&resumed)?, None);
        assert_eq!(db.account_wait_worktree(&resumed)?, None);
        assert!(wait_for_file(&runner.with_extension("sh.log"))?.contains(&resumed));
    }
    Ok(())
}

async fn account_uncovered_harness_matches_no_table(entry: &str) -> Result<()> {
    let root = TestDir::new("uncovered-harness")?;
    let mut observed = Vec::new();
    for has_accounts in [false, true] {
        let mut cfg = account_pool_config(root.path());
        if !has_accounts {
            cfg.accounts.clear();
        }
        cfg.db_path = root.path().join(format!("state-{has_accounts}.db"));
        let mut base = cfg.clone();
        base.accounts.clear();
        let expected = build_agent_command(
            &base,
            "codex",
            Path::new("codex"),
            "task",
            "session",
            root.path(),
            None,
        );
        let actual = build_agent_command(
            &cfg,
            "codex",
            Path::new("codex"),
            "task",
            "session",
            root.path(),
            None,
        );
        assert_eq!(
            actual.as_std().get_program(),
            expected.as_std().get_program()
        );
        assert_eq!(
            actual.as_std().get_args().collect::<Vec<_>>(),
            expected.as_std().get_args().collect::<Vec<_>>()
        );
        assert_eq!(
            actual.as_std().get_envs().collect::<Vec<_>>(),
            expected.as_std().get_envs().collect::<Vec<_>>()
        );
        cfg.harness_runners.insert(
            "codex".into(),
            crate::config::HarnessRunnerConfig {
                program: "/bin/sh".into(),
                base_args: vec![
                    "-c".into(),
                    "printf '%s\\n' \"$0\" \"$CLAUDE_CONFIG_DIR\" \"$CODEX_HOME\"".into(),
                ],
                env: BTreeMap::from([
                    ("CLAUDE_CONFIG_DIR".into(), "runner-claude".into()),
                    ("CODEX_HOME".into(), "runner-codex".into()),
                ]),
                ..Default::default()
            },
        );
        let db = StateStore::open(&cfg.db_path)?;
        let runner = root.path().join(format!("runner-{has_accounts}.sh"));
        fs::write(
            &runner,
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$0.log.tmp\" && mv \"$0.log.tmp\" \"$0.log\"\n",
        )?;
        fs::set_permissions(&runner, fs::Permissions::from_mode(0o755))?;
        let id = if entry == "start" {
            queue_session_in_dir_with_runner_program(
                &db,
                &cfg,
                "fallback task",
                "codex",
                false,
                root.path(),
                &runner,
                None,
                None,
                SessionGrouping::default(),
            )
            .await?
        } else {
            let mut session = build_session("resume", SessionState::Stopped, Utc::now());
            session.agent_type = "codex".into();
            session.task = "fallback task".into();
            session.working_dir = root.path().into();
            db.insert_session(&session)?;
            if entry == "waiting-resume" {
                rusqlite::Connection::open(&cfg.db_path)?.execute(
                    "UPDATE sessions SET account_wait_worktree = 0 WHERE id = ?1",
                    [&session.id],
                )?;
            }
            resume_session_with_program(&db, &cfg, &session.id, Some(&runner)).await?
        };
        assert_eq!(db.session_account(&id)?, None);
        assert_eq!(db.account_wait_worktree(&id)?, None, "entry={entry}");
        assert_eq!(account_wait_message(&db, &id)?, None);
        let runner_args = wait_for_file(&runner.with_extension("sh.log"))?.replace(&id, "session");
        run_session(&cfg, &id, "fallback task", "codex", root.path()).await?;
        assert_eq!(db.get_session(&id)?.unwrap().state, SessionState::Completed);
        assert_eq!(db.session_account(&id)?, None);
        assert_eq!(db.account_wait_worktree(&id)?, None);
        let output = db
            .get_output_lines(&id, 20)?
            .into_iter()
            .map(|line| line.text)
            .collect::<Vec<_>>();
        assert_eq!(output, ["fallback task", "runner-claude", "runner-codex"]);
        observed.push((runner_args, output));
    }
    assert_eq!(observed[0], observed[1]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_uncovered_harness_start_matches_no_table() -> Result<()> {
    account_uncovered_harness_matches_no_table("start").await
}

#[tokio::test(flavor = "current_thread")]
async fn account_uncovered_harness_resume_matches_no_table() -> Result<()> {
    account_uncovered_harness_matches_no_table("resume").await
}

#[tokio::test(flavor = "current_thread")]
async fn account_uncovered_harness_waiting_resume_matches_no_table() -> Result<()> {
    account_uncovered_harness_matches_no_table("waiting-resume").await
}

#[tokio::test(flavor = "current_thread")]
async fn account_uncovered_harness_keeps_matching_capacity_and_pins() -> Result<()> {
    for configured_harness in ["claude", " Claude-Code "] {
        let root = TestDir::new("covered-harness")?;
        let mut cfg = account_pool_config(root.path());
        for account in cfg.accounts.values_mut() {
            account.harness = configured_harness.into();
        }
        let db = StateStore::open(&cfg.db_path)?;
        for expected in [Some("a"), Some("b"), None] {
            let id = queue_session_in_dir_with_runner_program(
                &db,
                &cfg,
                "task",
                "claude",
                false,
                root.path(),
                Path::new("/usr/bin/true"),
                None,
                None,
                SessionGrouping::default(),
            )
            .await?;
            assert_eq!(db.session_account(&id)?.as_deref(), expected);
            assert_eq!(
                db.account_wait_worktree(&id)?,
                expected.is_none().then_some(false)
            );
        }
        for pin in ["a", "missing"] {
            cfg.requested_account = Some(pin.into());
            let error = queue_session_in_dir_with_runner_program(
                &db,
                &cfg,
                "task",
                "codex",
                false,
                root.path(),
                Path::new("/missing-must-not-spawn"),
                None,
                None,
                SessionGrouping::default(),
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(error.contains(pin) && error.contains("codex"));
            if pin == "a" {
                assert!(error.contains(configured_harness));
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_manual_full_start_reports_waiting_without_process_or_worktree() -> Result<()> {
    let root = TestDir::new("manual-account-full")?;
    init_git_repo(root.path())?;
    let cfg = account_pool_config(root.path());
    let db = StateStore::open(&cfg.db_path)?;
    account_record(&db, &cfg, "a-full", root.path())?;
    account_record(&db, &cfg, "b-full", root.path())?;
    let id = queue_session_in_dir_with_runner_program(
        &db,
        &cfg,
        "wait",
        "claude",
        true,
        root.path(),
        Path::new("/missing-must-not-spawn"),
        None,
        None,
        SessionGrouping::default(),
    )
    .await?;
    let session = db.get_session(&id)?.unwrap();
    let status = get_status(&db, &cfg, &id)?.to_string();
    println!(
        "full manual start: state={:?}; worktree={}; status={status}",
        session.state,
        session.worktree.is_some()
    );
    assert_eq!(session.state, SessionState::Pending);
    assert_eq!(session.pid, None);
    assert!(session.worktree.is_none());
    assert!(!cfg.worktree_root.join(&id).exists());
    assert!(status.contains("Waiting for an account"));
    assert!(
        account_wait_message(&db, &id)?
            .unwrap()
            .contains("waiting for an account")
    );
    let result =
        resume_session_with_program(&db, &cfg, &id, Some(Path::new("/missing-must-not-spawn")))
            .await;
    assert!(result.is_ok());
    assert!(
        get_status(&db, &cfg, &id)?
            .to_string()
            .contains("Waiting for an account")
    );
    assert!(db.pending_worktree_queue(10)?.is_empty());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_manual_unpinned_wait_selects_free_account_on_resume() -> Result<()> {
    let root = TestDir::new("manual-unpinned-resume")?;
    let cfg = account_pool_config(root.path());
    let db = StateStore::open(&cfg.db_path)?;
    account_record(&db, &cfg, "a-full", root.path())?;
    account_record(&db, &cfg, "b-full", root.path())?;
    let id = queue_session_in_dir_with_runner_program(
        &db,
        &cfg,
        "wait",
        "claude",
        false,
        root.path(),
        Path::new("/missing-must-not-spawn"),
        None,
        None,
        SessionGrouping::default(),
    )
    .await?;
    assert_eq!(db.session_account_pin(&id)?, None);
    db.update_state("b-full", &SessionState::Stopped)?;
    let (runner, log) = write_fake_claude(root.path())?;
    assert_eq!(
        resume_session_with_program(&db, &cfg, &id, Some(&runner)).await?,
        id
    );
    assert!(wait_for_file(&log)?.contains("run-session"));
    assert_eq!(db.session_account(&id)?.as_deref(), Some("b"));
    assert_eq!(db.session_account_pin(&id)?, None);
    assert_eq!(account_wait_message(&db, &id)?, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_manual_marker_broadcast_is_redacted() -> Result<()> {
    let root = TestDir::new("manual-marker-broadcast")?;
    let cfg = build_config(root.path());
    let db = StateStore::open(&cfg.db_path)?;
    db.insert_session(&build_session("marker", SessionState::Pending, Utc::now()))?;
    let marker = "account-env-secret-marker-4f068e";
    let output = SessionOutputStore::default();
    let mut events = output.subscribe();
    let mut command = Command::new("/usr/bin/printf");
    command.arg("%s\n").arg(marker);
    super::super::runtime::capture_command_output_redacted(
        cfg.db_path,
        "marker".into(),
        command,
        output.clone(),
        StdDuration::from_secs(1),
        vec![marker.into()],
    )
    .await?;
    assert_eq!(db.get_output_lines("marker", 10)?[0].text, "[REDACTED]");
    assert_eq!(output.lines("marker")[0].text, "[REDACTED]");
    assert_eq!(events.try_recv()?.line.text, "[REDACTED]");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_manual_wait_requires_resume_even_after_capacity_frees() -> Result<()> {
    let root = TestDir::new("manual-account-resume")?;
    init_git_repo(root.path())?;
    let mut cfg = account_pool_config(root.path());
    let db = StateStore::open(&cfg.db_path)?;
    account_record(&db, &cfg, "a-full", root.path())?;
    account_record(&db, &cfg, "b-full", root.path())?;
    cfg.requested_account = Some("b".into());
    let id = queue_session_in_dir_with_runner_program(
        &db,
        &cfg,
        "wait",
        "claude",
        true,
        root.path(),
        Path::new("/missing-must-not-spawn"),
        None,
        None,
        SessionGrouping::default(),
    )
    .await?;
    db.update_state("b-full", &SessionState::Stopped)?;
    let starts =
        activate_pending_worktree_sessions_with(&db, &cfg, |_, _, _, _, _| async { Ok(()) })
            .await?;
    println!("after capacity frees: automatic starts={starts:?}");
    assert!(starts.is_empty(), "account waits require explicit resume");
    assert!(db.get_session(&id)?.unwrap().worktree.is_none());
    drop(db);
    let db = StateStore::open(&cfg.db_path)?;
    cfg.requested_account = None;
    let (runner, log) = write_fake_claude(root.path())?;
    assert_eq!(
        resume_session_with_program(&db, &cfg, &id, Some(&runner)).await?,
        id
    );
    assert!(wait_for_file(&log)?.contains("run-session"));
    assert_eq!(db.session_account(&id)?.as_deref(), Some("b"));
    assert!(db.get_session(&id)?.unwrap().worktree.is_some());
    assert!(
        !get_status(&db, &cfg, &id)?
            .to_string()
            .contains("Waiting for an account")
    );
    assert!(account_wait_message(&db, &id)?.is_none());
    println!("manual resume selected b and launched preserved worktree intent");
    Ok(())
}

#[test]
fn account_manual_schema_contains_no_retry_machinery() -> Result<()> {
    let root = TestDir::new("manual-account-schema")?;
    let cfg = account_pool_config(root.path());
    let _db = StateStore::open(&cfg.db_path)?;
    let sql = rusqlite::Connection::open(&cfg.db_path)?;
    let mut statement =
        sql.prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")?;
    let tables = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let obsolete = tables
        .iter()
        .filter(|name| {
            name.as_str() == "account_wait_queue"
                || name.starts_with("session_activation_")
                || name.starts_with("session_launch_")
        })
        .collect::<Vec<_>>();
    println!("obsolete tables: {obsolete:?}");
    assert!(obsolete.is_empty());
    let mut statement = sql.prepare("PRAGMA table_info(sessions)")?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    assert!(!columns.iter().any(|name| name == "worktree_slot_released"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_revision_auto_assignment_is_not_a_resume_pin() -> Result<()> {
    for remove_accounts in [false, true] {
        let root = TestDir::new("accounts-auto-resume")?;
        let mut cfg = account_pool_config(root.path());
        cfg.harness_runners.insert(
            "claude".into(),
            crate::config::HarnessRunnerConfig {
                program: "/usr/bin/true".into(),
                ..Default::default()
            },
        );
        let db = StateStore::open(&cfg.db_path)?;
        account_record(&db, &cfg, "old", root.path())?;
        assert_eq!(db.session_account("old")?.as_deref(), Some("a"));
        db.update_state("old", &SessionState::Failed)?;
        if remove_accounts {
            cfg.accounts.clear();
        } else {
            account_record(&db, &cfg, "busy", root.path())?;
        }
        assert!(
            resume_session_with_program(&db, &cfg, "old", Some(Path::new("/usr/bin/true")))
                .await
                .is_ok()
        );
        run_session(&cfg, "old", "task", "claude", root.path()).await?;
        assert_eq!(db.session_account_pin("old")?, None);
        assert_eq!(
            db.session_account("old")?.as_deref(),
            if remove_accounts { None } else { Some("b") }
        );
        assert!(db.account_wait_worktree("old")?.is_none());
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_seventh_adopted_handoff_keeps_persisted_pin() -> Result<()> {
    let root = TestDir::new("adopted-pin")?;
    let mut cfg = account_pool_config(root.path());
    cfg.requested_account = Some("a".into());
    let db = StateStore::open(&cfg.db_path)?;
    account_record(&db, &cfg, "handoff", root.path())?;
    cfg.requested_account = Some("b".into());
    cfg.harness_runners.insert(
        "claude".into(),
        crate::config::HarnessRunnerConfig {
            program: "/usr/bin/true".into(),
            ..Default::default()
        },
    );
    run_session(&cfg, "handoff", "task", "claude", root.path()).await?;
    assert_eq!(
        db.get_session("handoff")?.unwrap().state,
        SessionState::Completed
    );
    assert_eq!(db.session_account("handoff")?.as_deref(), Some("a"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_final_running_run_session_cannot_repin() -> Result<()> {
    let root = TestDir::new("running-account")?;
    let mut cfg = account_pool_config(root.path());
    let db = StateStore::open(&cfg.db_path)?;
    account_record(&db, &cfg, "running", root.path())?;
    db.update_pid("running", Some(std::process::id()))?;
    db.update_state("running", &SessionState::Running)?;
    cfg.requested_account = Some("b".into());
    run_session(&cfg, "running", "task", "claude", root.path()).await?;
    assert!(
        resume_session_with_program(
            &db,
            &cfg,
            "running",
            Some(Path::new("/missing-must-not-spawn"))
        )
        .await
        .is_err()
    );
    assert_eq!(db.session_account("running")?.as_deref(), Some("a"));
    assert_eq!(db.session_account_pin("running")?, None);
    assert_eq!(
        db.get_session("running")?.unwrap().pid,
        Some(std::process::id())
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_manual_changed_harness_is_reselected_before_spawn() -> Result<()> {
    let root = TestDir::new("changed-account-harness")?;
    let mut cfg = account_pool_config(root.path());
    let db = StateStore::open(&cfg.db_path)?;
    account_record(&db, &cfg, "queued", root.path())?;
    assert_eq!(db.session_account("queued")?.as_deref(), Some("a"));
    cfg.accounts.get_mut("a").unwrap().harness = "codex".into();
    cfg.harness_runners.insert(
        "claude".into(),
        crate::config::HarnessRunnerConfig {
            program: "/bin/sh".into(),
            base_args: vec![
                "-c".into(),
                "test \"$CLAUDE_CONFIG_DIR\" = placeholder-b".into(),
            ],
            ..Default::default()
        },
    );
    run_session(&cfg, "queued", "task", "claude", root.path()).await?;
    assert_eq!(db.session_account("queued")?.as_deref(), Some("b"));
    assert_eq!(
        db.get_session("queued")?.unwrap().state,
        SessionState::Completed
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_manual_resume_after_pool_removal_and_worktree_capacity_wait() -> Result<()> {
    let root = TestDir::new("manual-resume-worktree-capacity")?;
    init_git_repo(root.path())?;
    let mut cfg = account_pool_config(root.path());
    cfg.max_parallel_worktrees = 1;
    let db = StateStore::open(&cfg.db_path)?;
    let existing = lifecycle_existing_worktree(&db, &cfg, root.path())?;
    account_record(&db, &cfg, "a-full", root.path())?;
    account_record(&db, &cfg, "b-full", root.path())?;
    let id = queue_session_in_dir_with_runner_program(
        &db,
        &cfg,
        "wait",
        "claude",
        true,
        root.path(),
        Path::new("/missing-must-not-spawn"),
        None,
        None,
        SessionGrouping::default(),
    )
    .await?;
    cfg.accounts.clear();
    assert!(
        resume_session_with_program(&db, &cfg, &id, Some(Path::new("/missing-must-not-spawn")))
            .await
            .is_ok()
    );
    assert_eq!(db.account_wait_worktree(&id)?, None);
    assert_eq!(db.pending_worktree_queue(10)?[0].session_id, id);
    assert!(db.get_session(&id)?.unwrap().worktree.is_none());
    assert!(
        activate_pending_worktree_sessions_with(&db, &cfg, |_, _, _, _, _| async { Ok(()) })
            .await?
            .is_empty()
    );
    db.clear_worktree_to_dir(&existing.id, root.path())?;
    assert_eq!(
        activate_pending_worktree_sessions_with(&db, &cfg, |_, _, _, _, _| async { Ok(()) })
            .await?,
        vec![id.clone()]
    );
    assert!(db.pending_worktree_queue(10)?.is_empty());
    assert!(db.get_session(&id)?.unwrap().worktree.is_some());
    assert!(existing.working_dir.join("valuable.txt").exists());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_lifecycle_capture_error_preserves_spawned_edits_and_worktrees() -> Result<()> {
    for entry in ["existing", "new", "activation"] {
        let root = TestDir::new("spawned-edits")?;
        init_git_repo(root.path())?;
        let mut cfg = account_pool_config(root.path());
        let script = root.path().join("agent.sh");
        fs::write(
            &script,
            "printf 'agent edit' > valuable.txt\nprintf '\\377\\n'\n",
        )?;
        cfg.harness_runners.insert(
            "claude".into(),
            crate::config::HarnessRunnerConfig {
                program: "/bin/sh".into(),
                base_args: vec![script.to_string_lossy().into_owned()],
                ..Default::default()
            },
        );
        let db = StateStore::open(&cfg.db_path)?;
        let id = if entry == "existing" {
            let session = lifecycle_existing_worktree(&db, &cfg, root.path())?;
            db.update_state_and_pid(&session.id, &SessionState::Pending, None)?;
            session.id
        } else {
            if entry == "activation" {
                cfg.max_parallel_worktrees = 0;
            }
            let id = queue_session_in_dir_with_runner_program(
                &db,
                &cfg,
                "edit",
                "claude",
                true,
                root.path(),
                Path::new("/usr/bin/true"),
                None,
                None,
                SessionGrouping::default(),
            )
            .await?;
            cfg.max_parallel_worktrees = 1;
            id
        };
        if entry == "activation" {
            assert_eq!(
                activate_pending_worktree_sessions(&db, &cfg).await?,
                vec![id.clone()]
            );
            final_wait_for_terminal(&db, &id).await?;
        } else {
            let session = db.get_session(&id)?.unwrap();
            assert!(
                run_session(&cfg, &id, &session.task, "claude", &session.working_dir)
                    .await
                    .is_err()
            );
        }
        let after = db.get_session(&id)?.unwrap();
        assert_eq!(after.state, SessionState::Failed);
        let tree = after.worktree.as_ref().unwrap();
        assert!(tree.path.exists());
        assert_eq!(
            fs::read_to_string(tree.path.join("valuable.txt"))?,
            "agent edit"
        );
        println!("spawned entry={entry}: Failed, worktree and agent edit preserved");
    }
    Ok(())
}
