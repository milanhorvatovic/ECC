// Keep the baseline signature and body so the differential check shares no launch decisions.
#[allow(clippy::too_many_arguments, clippy::needless_borrow)]
async fn reference_queue(
    db: &StateStore,
    cfg: &Config,
    task: &str,
    agent_type: &str,
    use_worktree: bool,
    repo_root: &Path,
    runner_program: &Path,
    profile: Option<SessionAgentProfile>,
    grouping: SessionGrouping,
) -> Result<String> {
    let effective_agent_type = profile
        .as_ref()
        .and_then(|profile| profile.agent.as_deref())
        .unwrap_or(agent_type);
    let session = reference_build(
        db,
        task,
        &effective_agent_type,
        use_worktree,
        cfg,
        repo_root,
        grouping,
    )?;
    db.insert_session(&session)?;
    if let Some(profile) = profile.as_ref() {
        db.upsert_session_profile(&session.id, profile)?;
    }

    if use_worktree && session.worktree.is_none() {
        db.enqueue_pending_worktree(&session.id, repo_root)?;
        return Ok(session.id);
    }

    let working_dir = session
        .worktree
        .as_ref()
        .map(|worktree| worktree.path.as_path())
        .unwrap_or(repo_root);

    match reference_spawn(
        task,
        &session.id,
        &session.agent_type,
        working_dir,
        runner_program,
    )
    .await
    {
        Ok(()) => Ok(session.id),
        Err(error) => {
            db.update_state(&session.id, &SessionState::Failed)?;

            if let Some(worktree) = session.worktree.as_ref() {
                let _ = crate::worktree::remove(worktree);
            }

            Err(error.context(format!("Failed to queue session {}", session.id)))
        }
    }
}

async fn reference_resume(
    db: &StateStore,
    _cfg: &Config,
    id: &str,
    runner_executable_override: Option<&Path>,
) -> Result<String> {
    let session = resolve_session(db, id)?;

    if session.state == SessionState::Completed {
        anyhow::bail!("Completed sessions cannot be resumed: {}", session.id);
    }

    if session.state == SessionState::Running {
        anyhow::bail!("Session is already running: {}", session.id);
    }

    db.update_state_and_pid(&session.id, &SessionState::Pending, None)?;
    if let Some(worktree) = session.worktree.as_ref() {
        if let Err(error) = worktree::sync_shared_dependency_dirs(worktree) {
            tracing::warn!(
                "Shared dependency cache sync warning for resumed session {}: {error}",
                session.id
            );
        }
    }
    let runner_executable = match runner_executable_override {
        Some(program) => program.to_path_buf(),
        None => std::env::current_exe().context("Failed to resolve ECC executable path")?,
    };
    reference_spawn(
        &session.task,
        &session.id,
        &session.agent_type,
        &session.working_dir,
        &runner_executable,
    )
    .await
    .with_context(|| format!("Failed to resume session {}", session.id))?;
    Ok(session.id)
}

async fn reference_run(
    cfg: &Config,
    session_id: &str,
    task: &str,
    agent_type: &str,
    working_dir: &Path,
) -> Result<()> {
    let db = StateStore::open(&cfg.db_path)?;
    let session = resolve_session(&db, session_id)?;

    if session.state != SessionState::Pending {
        tracing::info!(
            "Skipping run_session for {} because state is {}",
            session_id,
            session.state
        );
        return Ok(());
    }

    let agent_program = agent_program(cfg, agent_type)?;
    let profile = db.get_session_profile(session_id)?;
    let command = build_agent_command(
        cfg,
        agent_type,
        &agent_program,
        task,
        session_id,
        working_dir,
        profile.as_ref(),
    );
    super::super::runtime::capture_command_output(
        cfg.db_path.clone(),
        session_id.to_string(),
        command,
        SessionOutputStore::default(),
        std::time::Duration::from_secs(cfg.heartbeat_interval_secs),
    )
    .await?;
    Ok(())
}

async fn reference_activate<F, Fut>(db: &StateStore, cfg: &Config, spawn: F) -> Result<Vec<String>>
where
    F: Fn(Config, String, String, String, PathBuf) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut available_slots = cfg
        .max_parallel_worktrees
        .saturating_sub(reference_count(db)?);
    if available_slots == 0 {
        return Ok(Vec::new());
    }

    let mut started = Vec::new();
    for request in db.pending_worktree_queue(available_slots)? {
        let Some(session) = db.get_session(&request.session_id)? else {
            db.dequeue_pending_worktree(&request.session_id)?;
            continue;
        };

        if session.worktree.is_some()
            || session.pid.is_some()
            || session.state != SessionState::Pending
        {
            db.dequeue_pending_worktree(&session.id)?;
            continue;
        }

        let worktree =
            match worktree::create_for_session_in_repo(&session.id, cfg, &request.repo_root) {
                Ok(worktree) => worktree,
                Err(error) => {
                    db.dequeue_pending_worktree(&session.id)?;
                    db.update_state(&session.id, &SessionState::Failed)?;
                    tracing::warn!(
                        "Failed to create queued worktree for session {}: {error}",
                        session.id
                    );
                    continue;
                }
            };

        if let Err(error) = db.attach_worktree(&session.id, &worktree) {
            let _ = worktree::remove(&worktree);
            db.dequeue_pending_worktree(&session.id)?;
            db.update_state(&session.id, &SessionState::Failed)?;
            return Err(error.context(format!(
                "Failed to attach queued worktree for session {}",
                session.id
            )));
        }

        if let Err(error) = spawn(
            cfg.clone(),
            session.id.clone(),
            session.task.clone(),
            session.agent_type.clone(),
            worktree.path.clone(),
        )
        .await
        {
            let _ = worktree::remove(&worktree);
            let _ = db.clear_worktree_to_dir(&session.id, &request.repo_root);
            db.dequeue_pending_worktree(&session.id)?;
            db.update_state(&session.id, &SessionState::Failed)?;
            tracing::warn!(
                "Failed to start queued worktree session {}: {error}",
                session.id
            );
            continue;
        }

        db.dequeue_pending_worktree(&session.id)?;
        started.push(session.id);
        available_slots = available_slots.saturating_sub(1);
        if available_slots == 0 {
            break;
        }
    }

    Ok(started)
}

fn reference_build(
    db: &StateStore,
    task: &str,
    agent_type: &str,
    use_worktree: bool,
    cfg: &Config,
    repo_root: &Path,
    grouping: SessionGrouping,
) -> Result<Session> {
    let canonical_agent_type =
        SessionHarnessInfo::resolve_requested_agent_type(cfg, agent_type, repo_root);
    let id = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let now = chrono::Utc::now();

    let worktree = if use_worktree && reference_count(db)? < cfg.max_parallel_worktrees {
        Some(worktree::create_for_session_in_repo(&id, cfg, repo_root)?)
    } else {
        None
    };
    let working_dir = worktree
        .as_ref()
        .map(|worktree| worktree.path.clone())
        .unwrap_or_else(|| repo_root.to_path_buf());
    let project = grouping
        .project
        .as_deref()
        .and_then(normalize_group_label)
        .unwrap_or_else(|| default_project_label(repo_root));
    let task_group = grouping
        .task_group
        .as_deref()
        .and_then(normalize_group_label)
        .unwrap_or_else(|| default_task_group_label(task));

    Ok(Session {
        id,
        task: task.to_string(),
        project,
        task_group,
        agent_type: canonical_agent_type,
        working_dir,
        state: SessionState::Pending,
        pid: None,
        worktree,
        created_at: now,
        updated_at: now,
        last_heartbeat_at: now,
        metrics: SessionMetrics::default(),
    })
}

fn reference_count(db: &StateStore) -> Result<usize> {
    Ok(db
        .list_sessions()?
        .iter()
        .filter(|session| session.worktree.is_some())
        .count())
}

async fn reference_spawn(
    task: &str,
    id: &str,
    agent: &str,
    cwd: &Path,
    runner: &Path,
) -> Result<()> {
    spawn_session_runner_for_program(task, id, agent, cwd, runner).await
}
