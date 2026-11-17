use super::*;
use crate::config::ResolvedOrchestrationTemplate;
use crate::session::store::templates::TemplateStep;

pub(super) async fn launch(
    db: &StateStore,
    cfg: &Config,
    template: &ResolvedOrchestrationTemplate,
    source: Option<&Session>,
    task: Option<&str>,
    repo_root: &Path,
    runner_program: &Path,
) -> Result<TemplateLaunchOutcome> {
    let ready = template
        .steps
        .iter()
        .filter(|step| step.after.is_empty())
        .count();
    let live = db
        .list_sessions()?
        .iter()
        .filter(|session| {
            matches!(
                session.state,
                SessionState::Pending
                    | SessionState::Running
                    | SessionState::Idle
                    | SessionState::Stale
            )
        })
        .count();
    let available = cfg.max_parallel_sessions.saturating_sub(live);
    if ready > available {
        anyhow::bail!(
            "template {} requires {ready} session slots but only {available} available",
            template.template_name
        );
    }
    let launch_id = uuid::Uuid::new_v4().to_string();
    let mut steps = Vec::new();
    for step in &template.steps {
        let profile_name = step.profile.as_deref().or_else(|| {
            step.agent
                .is_none()
                .then_some(cfg.default_agent_profile.as_deref())
                .flatten()
        });
        let profile = profile_name
            .map(|name| cfg.resolve_agent_profile(name))
            .transpose()?;
        let agent = effective_launch_agent(
            cfg,
            step.agent.as_deref().unwrap_or(&cfg.default_agent),
            profile.as_ref(),
            repo_root,
        );
        cfg.validate_account_pin(&agent, cfg.requested_account.as_deref())?;
        let grouping = SessionGrouping {
            project: step
                .project
                .clone()
                .or_else(|| source.map(|session| session.project.clone())),
            task_group: step
                .task_group
                .clone()
                .or_else(|| {
                    source
                        .map(|session| session.task_group.clone())
                        .or_else(|| task.map(default_task_group_label))
                        .or_else(|| Some(template.template_name.replace(['_', '-'], " ")))
                }),
        };
        let session = build_session_record(db, &step.task, &agent, false, cfg, repo_root, grouping)?;
        steps.push(TemplateStep {
            launch_id: launch_id.clone(),
            name: step.name.clone(),
            session,
            profile,
            after: Vec::new(),
            use_worktree: step.worktree,
            anchor_id: String::new(),
            template_name: template.template_name.clone(),
            account_pin: cfg.requested_account.clone(),
        });
    }
    let root_index = template
        .steps
        .iter()
        .position(|step| step.after.is_empty())
        .context("template has no root step")?;
    let anchor = source
        .map(|session| session.id.clone())
        .unwrap_or_else(|| steps[root_index].session.id.clone());
    for (index, resolved) in template.steps.iter().enumerate() {
        let after = resolved
            .after
            .iter()
            .map(|name| {
                let dependency = template
                    .steps
                    .iter()
                    .position(|step| step.name == *name)
                    .context("resolved template dependency disappeared")?;
                Ok(steps[dependency].session.id.clone())
            })
            .collect::<Result<Vec<_>>>()?;
        steps[index].after = after;
        steps[index].anchor_id = anchor.clone();
    }
    db.insert_template_steps(&steps)?;
    activate_with_runner(db, cfg, runner_program).await?;
    Ok(TemplateLaunchOutcome {
        template_name: template.template_name.clone(),
        step_count: steps.len(),
        anchor_session_id: Some(anchor),
        created: steps
            .into_iter()
            .map(|step| TemplateLaunchStepOutcome {
                step_name: step.name,
                session_id: step.session.id,
                task: step.session.task,
            })
            .collect(),
    })
}

pub async fn activate_ready_template_steps(db: &StateStore, cfg: &Config) -> Result<Vec<String>> {
    let runner = std::env::current_exe().context("Failed to resolve ECC executable path")?;
    activate_with_runner(db, cfg, &runner).await
}

pub(super) async fn activate_with_runner(
    db: &StateStore,
    cfg: &Config,
    runner: &Path,
) -> Result<Vec<String>> {
    let mut started = Vec::new();
    for step in db.template_steps()? {
        let result = activate_step(db, cfg, runner, &step).await;
        match result {
            Ok(true) => started.push(step.session.id.clone()),
            Ok(false) => {}
            Err(error) => {
                let reason = format!("step {:?} could not start: {error:#}", step.name);
                db.block_template_step(&step.session.id, &reason)?;
            }
        }
    }
    Ok(started)
}

async fn activate_step(
    db: &StateStore,
    cfg: &Config,
    runner: &Path,
    step: &TemplateStep,
) -> Result<bool> {
    let claimed = db.claim_ready_template_step(cfg, &step.session.id)?;
    let Some(session) = db.get_session(&step.session.id)? else {
        return Ok(claimed);
    };
    if session.state != SessionState::Pending
        || db.template_blocked_reason(&session.id)?.is_some()
        || db.account_wait_worktree(&session.id)?.is_some()
        || (step.use_worktree
            && session.worktree.is_none()
            && attached_worktree_count(db)? >= cfg.max_parallel_worktrees)
    {
        return Ok(claimed);
    }
    // Runners arbitrate in SQLite before starting a harness. A Pending row
    // can be redelivered after a daemon crash without duplicating agent work.
    spawn_session_runner_for_program(
        &session.task,
        &session.id,
        &session.agent_type,
        &session.working_dir,
        runner,
    )
    .await?;
    Ok(claimed)
}

pub(super) fn prepare_runtime(db: &StateStore, cfg: &Config, step: &TemplateStep) -> Result<bool> {
    let id = &step.session.id;
    if !db.claim_template_runtime(id)? {
        return Ok(false);
    }
    let result = (|| -> Result<bool> {
        let session = db
            .get_session(id)?
            .context("template session disappeared")?;
        let pin = db.session_account_pin(id)?;
        if !db.assign_session_account(
            cfg,
            id,
            &session.agent_type,
            pin.as_deref(),
            step.use_worktree,
        )? {
            db.release_template_runtime(id)?;
            return Ok(false);
        }
        if step.use_worktree && session.worktree.is_none() {
            if attached_worktree_count(db)? >= cfg.max_parallel_worktrees {
                db.release_template_runtime(id)?;
                return Ok(false);
            }
            let worktree =
                worktree::create_for_session_in_repo(id, cfg, &step.session.working_dir)?;
            db.attach_worktree(id, &worktree)?;
        }
        Ok(true)
    })();
    if result.is_err() {
        db.update_state_and_pid(id, &SessionState::Failed, None)?;
    }
    result
}
