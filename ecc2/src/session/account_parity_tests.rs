include!("account_base_reference.rs");

fn lifecycle_existing_worktree(db: &StateStore, cfg: &Config, root: &Path) -> Result<Session> {
    let mut session = build_session("existing", SessionState::Stopped, Utc::now());
    let worktree = worktree::create_for_session_in_repo(&session.id, cfg, root)?;
    fs::write(worktree.path.join("valuable.txt"), "uncommitted agent work")?;
    session.working_dir = worktree.path.clone();
    session.worktree = Some(worktree);
    db.insert_session(&session)?;
    Ok(session)
}

#[tokio::test(flavor = "current_thread")]
async fn account_lifecycle_failed_resume_keeps_existing_worktree() -> Result<()> {
    for accounts in [false, true] {
        let root = TestDir::new("lifecycle-resume")?;
        init_git_repo(root.path())?;
        let cfg = if accounts {
            account_pool_config(root.path())
        } else {
            build_config(root.path())
        };
        let db = StateStore::open(&cfg.db_path)?;
        let session = lifecycle_existing_worktree(&db, &cfg, root.path())?;
        let result = resume_session_with_program(
            &db,
            &cfg,
            &session.id,
            Some(Path::new("/missing-runner-probe")),
        )
        .await;
        let after = db.get_session(&session.id)?.unwrap();
        println!(
            "accounts={accounts}; error={}; valuable={}; attached={}",
            result.is_err(),
            session.working_dir.join("valuable.txt").exists(),
            after.worktree.is_some()
        );
        assert!(result.is_err());
        assert!(session.working_dir.join("valuable.txt").exists());
        assert_eq!(
            serde_json::to_value(&after.worktree)?,
            serde_json::to_value(&session.worktree)?
        );
        assert_eq!(after.working_dir, session.working_dir);
        assert_eq!(after.state, SessionState::Pending);
        assert_eq!(db.session_account(&session.id)?, None);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_lifecycle_removed_profile_routes_at_all_entry_points() -> Result<()> {
    let mut failures = Vec::new();
    for entry in ["preview", "assign", "drain", "rebalance-zero", "rebalance"] {
        let result = lifecycle_removed_profile_entry(entry).await;
        println!("entry={entry}; result={result:?}");
        if let Err(error) = result {
            failures.push(format!("{entry}: {error}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("; "));
    Ok(())
}

async fn lifecycle_removed_profile_entry(entry: &str) -> Result<()> {
    let root = TestDir::new("lifecycle-routing")?;
    let mut cfg = build_config(root.path());
    cfg.agent_profiles.insert(
        "retired".into(),
        crate::config::AgentProfileConfig {
            agent: Some("codex".into()),
            ..Default::default()
        },
    );
    let db = StateStore::open(&cfg.db_path)?;
    for (id, state) in [
        ("lead", SessionState::Running),
        ("idle", SessionState::Idle),
        ("busy", SessionState::Running),
    ] {
        let mut session = build_session(id, state, Utc::now());
        session.working_dir = root.path().into();
        db.insert_session(&session)?;
    }
    db.upsert_session_profile("lead", &cfg.resolve_agent_profile("retired")?)?;
    cfg.agent_profiles.remove("retired");
    for id in ["idle", "busy"] {
        db.send_native_assignment_notice("lead", id, "owned task")?;
        db.mark_messages_read(id)?;
    }
    println!("routing entry={entry}");
    match entry {
        "preview" => assert_eq!(
            preview_assignment_for_task(&db, &cfg, "lead", "next", "claude")?
                .session_id
                .as_deref(),
            Some("idle")
        ),
        "assign" => assert_eq!(
            assign_session_in_dir_with_runner_program(
                &db,
                &cfg,
                "lead",
                "next",
                "claude",
                false,
                root.path(),
                Path::new("/missing-must-not-spawn"),
                None,
                SessionGrouping::default()
            )
            .await?
            .session_id,
            "idle"
        ),
        "drain" => {
            db.send_message("outside", "lead", "next", "task_handoff")?;
            assert_eq!(
                drain_inbox(&db, &cfg, "lead", "claude", false, 1).await?[0].session_id,
                "idle"
            );
        }
        "rebalance-zero" => assert!(
            rebalance_team_backlog(&db, &cfg, "lead", "claude", false, 0)
                .await?
                .is_empty()
        ),
        _ => {
            for task in ["intake one", "intake two"] {
                db.send_message("lead", "busy", task, "task_handoff")?;
            }
            let outcomes = rebalance_team_backlog(&db, &cfg, "lead", "claude", false, 1).await?;
            assert_eq!(outcomes[0].session_id, "idle");
        }
    }
    Ok(())
}

#[test]
fn account_lifecycle_no_accounts_counts_every_base_worktree() -> Result<()> {
    let root = TestDir::new("lifecycle-base-slots")?;
    let cfg = build_config(root.path());
    let db = StateStore::open(&cfg.db_path)?;
    for (index, state) in [
        SessionState::Pending,
        SessionState::Running,
        SessionState::Idle,
        SessionState::Stopped,
        SessionState::Failed,
        SessionState::Completed,
        SessionState::Stale,
    ]
    .into_iter()
    .enumerate()
    {
        let mut session = build_session(&format!("base-{index}"), state, Utc::now());
        session.worktree = Some(super::super::WorktreeInfo {
            path: root.path().join(&session.id),
            branch: session.id.clone(),
            base_branch: "main".into(),
        });
        db.insert_session(&session)?;
        let base_count = db
            .list_sessions()?
            .iter()
            .filter(|session| session.worktree.is_some())
            .count();
        assert_eq!(attached_worktree_count(&db)?, base_count);
        assert!(
            db.list_sessions()?
                .iter()
                .all(|session| db.account_wait_worktree(&session.id).unwrap().is_none())
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_lifecycle_no_accounts_worktree_failure_leaves_no_row() -> Result<()> {
    let root = TestDir::new("lifecycle-start-record")?;
    let cfg = build_config(root.path());
    let db = StateStore::open(&cfg.db_path)?;
    let result = queue_session_in_dir_with_runner_program(
        &db,
        &cfg,
        "task",
        "claude",
        true,
        root.path(),
        Path::new("/usr/bin/true"),
        None,
        None,
        SessionGrouping::default(),
    )
    .await;
    println!(
        "start error={}; rows={}",
        result.is_err(),
        db.list_sessions()?.len()
    );
    assert!(result.is_err());
    assert!(db.list_sessions()?.is_empty());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn account_lifecycle_no_accounts_failure_records_match_base() -> Result<()> {
    for use_worktree in [false, true] {
        let root = TestDir::new("lifecycle-base-start-failure")?;
        init_git_repo(root.path())?;
        let cfg = build_config(root.path());
        let db = StateStore::open(&cfg.db_path)?;
        let result = queue_session_in_dir_with_runner_program(
            &db,
            &cfg,
            "task",
            "claude",
            use_worktree,
            root.path(),
            Path::new("/missing-runner-probe"),
            None,
            None,
            SessionGrouping::default(),
        )
        .await;
        assert!(result.is_err());
        let sessions = db.list_sessions()?;
        assert_eq!(sessions.len(), 1);
        let failed = &sessions[0];
        assert_eq!(failed.state, SessionState::Failed);
        assert_eq!(failed.pid, None);
        assert_eq!(failed.worktree.is_some(), use_worktree);
        assert!(db.get_output_lines(&failed.id, 10)?.is_empty());
        if use_worktree {
            assert!(!failed.worktree.as_ref().unwrap().path.exists());
        }
    }
    for state in [
        SessionState::Pending,
        SessionState::Idle,
        SessionState::Stopped,
        SessionState::Failed,
        SessionState::Stale,
    ] {
        let root = TestDir::new("lifecycle-base-resume-record")?;
        let cfg = build_config(root.path());
        let db = StateStore::open(&cfg.db_path)?;
        let before = build_session("resume", state, Utc::now());
        db.insert_session(&before)?;
        assert!(
            resume_session_with_program(
                &db,
                &cfg,
                "resume",
                Some(Path::new("/missing-runner-probe"))
            )
            .await
            .is_err()
        );
        let after = db.get_session("resume")?.unwrap();
        assert_eq!(after.state, SessionState::Pending);
        assert_eq!(after.pid, None);
        assert_eq!(after.working_dir, before.working_dir);
        assert!(db.get_output_lines("resume", 10)?.is_empty());
    }
    let root = TestDir::new("lifecycle-base-runtime-record")?;
    let mut cfg = build_config(root.path());
    let db = StateStore::open(&cfg.db_path)?;
    let mut pending = build_session("runtime", SessionState::Pending, Utc::now());
    pending.working_dir = root.path().into();
    db.insert_session(&pending)?;
    cfg.harness_runners.insert(
        "claude".into(),
        crate::config::HarnessRunnerConfig {
            program: " ".into(),
            ..Default::default()
        },
    );
    assert!(
        run_session(&cfg, "runtime", "task", "claude", root.path())
            .await
            .is_err()
    );
    assert_eq!(
        db.get_session("runtime")?.unwrap().state,
        SessionState::Pending
    );
    cfg.harness_runners.get_mut("claude").unwrap().program = "/missing-agent-probe".into();
    assert!(
        run_session(&cfg, "runtime", "task", "claude", root.path())
            .await
            .is_err()
    );
    assert_eq!(
        db.get_session("runtime")?.unwrap().state,
        SessionState::Failed
    );
    assert!(db.get_output_lines("runtime", 10)?.is_empty());
    Ok(())
}

async fn final_wait_for_terminal(db: &StateStore, id: &str) -> Result<Session> {
    for _ in 0..1000 {
        let session = db.get_session(id)?.unwrap();
        if matches!(
            session.state,
            SessionState::Failed | SessionState::Completed
        ) {
            return Ok(session);
        }
        tokio::time::sleep(StdDuration::from_millis(5)).await;
    }
    anyhow::bail!("session {id} did not finish")
}

fn final_base_record(db: &StateStore, root: &Path, outcome: bool) -> Result<serde_json::Value> {
    let mut rows = Vec::new();
    for session in db.list_sessions()? {
        let normalize = |value: &Path| {
            value
                .strip_prefix(root)
                .unwrap_or(value)
                .to_string_lossy()
                .replace(&session.id, "<session>")
        };
        let worktree = session.worktree.as_ref().map(|tree| {
            (
                normalize(&tree.path),
                tree.branch.replace(&session.id, "<session>"),
                tree.base_branch.clone(),
                tree.path.exists(),
            )
        });
        let output = db
            .get_output_lines(&session.id, 100)?
            .into_iter()
            .map(|line| format!("{:?}:{}", line.stream, line.text))
            .collect::<Vec<_>>();
        rows.push(serde_json::json!({"task": session.task, "agent": session.agent_type, "state": session.state.to_string(), "pid": session.pid, "directory": normalize(&session.working_dir), "worktree": worktree, "output": output, "metrics": session.metrics, "group": session.task_group, "project_is_default": session.project == default_project_label(root)}));
    }
    Ok(serde_json::json!({"ok": outcome, "rows": rows}))
}

#[tokio::test(flavor = "current_thread")]
async fn account_final_no_accounts_four_paths_match_base_revision() -> Result<()> {
    for entry in ["start", "resume", "run", "activation"] {
        for failure in [false, true] {
            let mut records = Vec::new();
            for reference in [true, false] {
                let root = TestDir::new("final-base-comparison")?;
                init_git_repo(root.path())?;
                let mut cfg = build_config(root.path());
                cfg.harness_runners.insert(
                    "claude".into(),
                    crate::config::HarnessRunnerConfig {
                        program: if failure {
                            "/missing-agent-probe".into()
                        } else {
                            "/bin/sh".into()
                        },
                        base_args: vec!["-c".into(), "printf runtime-ok".into()],
                        ..Default::default()
                    },
                );
                let db = StateStore::open(&cfg.db_path)?;
                let runner = if failure {
                    Path::new("/missing-runner-probe")
                } else {
                    Path::new("/usr/bin/true")
                };
                let ok = match entry {
                    "start" => {
                        if reference {
                            reference_queue(
                                &db,
                                &cfg,
                                "task",
                                "claude",
                                true,
                                root.path(),
                                runner,
                                None,
                                SessionGrouping::default(),
                            )
                            .await
                            .is_ok()
                        } else {
                            queue_session_in_dir_with_runner_program(
                                &db,
                                &cfg,
                                "task",
                                "claude",
                                true,
                                root.path(),
                                runner,
                                None,
                                None,
                                SessionGrouping::default(),
                            )
                            .await
                            .is_ok()
                        }
                    }
                    "resume" => {
                        let session = lifecycle_existing_worktree(&db, &cfg, root.path())?;
                        if reference {
                            reference_resume(&db, &cfg, &session.id, Some(runner))
                                .await
                                .is_ok()
                        } else {
                            resume_session_with_program(&db, &cfg, &session.id, Some(runner))
                                .await
                                .is_ok()
                        }
                    }
                    "run" => {
                        let session = lifecycle_existing_worktree(&db, &cfg, root.path())?;
                        db.update_state_and_pid(&session.id, &SessionState::Pending, None)?;
                        if reference {
                            reference_run(
                                &cfg,
                                &session.id,
                                &session.task,
                                "claude",
                                &session.working_dir,
                            )
                            .await
                            .is_ok()
                        } else {
                            run_session(
                                &cfg,
                                &session.id,
                                &session.task,
                                "claude",
                                &session.working_dir,
                            )
                            .await
                            .is_ok()
                        }
                    }
                    _ => {
                        let mut pending =
                            build_session("queued", SessionState::Pending, Utc::now());
                        pending.working_dir = root.path().into();
                        db.insert_session(&pending)?;
                        db.enqueue_pending_worktree(&pending.id, root.path())?;
                        let result = if reference {
                            reference_activate(
                                &db,
                                &cfg,
                                |cfg, id, task, harness, cwd| async move {
                                    tokio::spawn(async move {
                                        let _ =
                                            reference_run(&cfg, &id, &task, &harness, &cwd).await;
                                    });
                                    Ok(())
                                },
                            )
                            .await
                        } else {
                            activate_pending_worktree_sessions(&db, &cfg).await
                        };
                        assert_eq!(result.as_ref().unwrap(), &vec!["queued".to_string()]);
                        final_wait_for_terminal(&db, "queued").await?;
                        result.is_ok()
                    }
                };
                records.push(final_base_record(&db, root.path(), ok)?);
            }
            println!(
                "base comparison entry={entry}; failure={failure}; equal={}",
                records[0] == records[1]
            );
            assert_eq!(records[0], records[1], "entry={entry}, failure={failure}");
        }
    }
    Ok(())
}

fn seventh_source_hash(source: &str) -> u64 {
    source.bytes().fold(14695981039346656037, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(1099511628211)
    })
}

#[cfg(unix)]
#[test]
fn account_seventh_daemon_recovery_matches_base() -> Result<()> {
    let root = TestDir::new("daemon-base-recovery")?;
    let cfg = build_config(root.path());
    let db = StateStore::open(&cfg.db_path)?;
    let mut dead = StdCommand::new("/usr/bin/true").spawn()?;
    let dead_pid = dead.id();
    dead.wait()?;
    for (id, pid) in [
        ("live", Some(std::process::id())),
        ("dead", Some(dead_pid)),
        ("missing", None),
    ] {
        let mut session = build_session(id, SessionState::Running, Utc::now());
        session.pid = pid;
        session.working_dir = root.path().into();
        db.insert_session(&session)?;
    }
    super::super::daemon::resume_crashed_sessions(&db)?;
    assert_eq!(
        db.get_session("live")?.unwrap().state,
        SessionState::Running
    );
    for id in ["dead", "missing"] {
        let session = db.get_session(id)?.unwrap();
        assert_eq!(session.state, SessionState::Failed);
        assert_eq!(session.pid, None);
    }
    let source = include_str!("daemon.rs");
    let segment = source.split_once("#[cfg(unix)]\n").unwrap().1;
    let segment = format!(
        "#[cfg(unix)]\n{}",
        segment.split_once("#[cfg(test)]\nmod tests").unwrap().0
    );
    let hash = seventh_source_hash(&segment);
    println!(
        "daemon Unix recovery: live=Running; dead=Failed; missing=Failed; base source hash={hash}"
    );
    assert_eq!(
        hash, 8016752923906464359,
        "daemon PID probes must match the base on every target"
    );
    Ok(())
}

#[test]
fn account_seventh_original_activation_test_matches_base() {
    let source = include_str!("manager.rs");
    let begin = source.find("    #[tokio::test(flavor = \"current_thread\")]\n    async fn activate_pending_worktree_sessions_starts_queued_session_when_slot_opens").unwrap();
    let end = source[begin + 20..].find("    #[tokio::test").unwrap() + begin + 20;
    let hash = seventh_source_hash(&source[begin..end]);
    println!("original activation test base source hash={hash}");
    assert_eq!(
        hash, 7582903013464608411,
        "original activation test must match the base byte for byte"
    );
}
