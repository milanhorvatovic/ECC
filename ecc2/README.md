# ECC 2.0 Alpha

`ecc2/` is the current Rust-based ECC 2.0 control-plane scaffold.

It is usable as an alpha for local experimentation, but it is **not** the finished ECC 2.0 product yet.

## What Exists Today

- terminal UI dashboard
- session store backed by SQLite
- session start / stop / resume flows
- background daemon mode
- observability and risk-scoring primitives
- worktree-aware session scaffolding
- basic multi-session state and output tracking

Dashboard output is hydrated from SQLite at startup and after recovery, then
synchronized with a monotonic database cursor. Because session runners are
separate processes, the database remains the cross-process source of truth
while steady-state refreshes read only the rows appended since the previous
dashboard tick.

## What This Is For

ECC 2.0 is the layer above individual harness installs.

The goal is:

- manage many agent sessions from one surface
- keep session state, output, and risk visible
- add orchestration, worktree management, and review controls
- support Claude Code first without blocking future harness interoperability

## Current Status

This directory should be treated as:

- real code
- alpha quality
- valid to build and test locally
- not yet a public GA release

Open issue clusters for the broader roadmap live in the main repo issue tracker under the `ecc-2.0` label.

## Run It

From the repo root:

```bash
cd ecc2
cargo run
```

Useful commands:

```bash
# Launch the dashboard
cargo run -- dashboard

# Start a new session
cargo run -- start --task "audit the repo and propose fixes" --agent claude --worktree

# List sessions
cargo run -- sessions

# Inspect a session
cargo run -- status latest

# Stop a session
cargo run -- stop <session-id>

# Resume a failed/stopped session
cargo run -- resume <session-id>

# Run the daemon loop
cargo run -- daemon
```

## Account Pools

Configure subscription accounts in `ecc2.toml`. Each account names a built-in
harness (except `unknown`) or a configured `harness_runners` entry. For example:

```toml
account_strategy = "least-busy"

[accounts.personal]
harness = "claude"
max_parallel = 2
env = { CLAUDE_CONFIG_DIR = "~/.claude-accounts/personal" }

[accounts.work]
harness = "claude"
max_parallel = 3
env = { CLAUDE_CONFIG_DIR = "~/.claude-accounts/work" }

[accounts.code]
harness = "codex"
env = { CODEX_HOME = "~/.codex-accounts/work" }
```

Paths above are placeholders. Replace them with absolute paths: ecc2 passes
environment values literally, without shell expansion. It never reads or copies
files from account configuration directories. Account environment overrides
runner environment when both set the same key. Config `Debug` output hides
environment values, and captured stdout and stderr replace every non-empty
environment value of the assigned account with `[REDACTED]` before storing or
broadcasting output. Empty
values and empty lines of multiline values are skipped. Account `env` is for
per-account values such as configuration paths: even a short common value such
as `1` replaces matching output text. Constant flags shared by every account
belong in `[harness_runners.<name>].env`; runner environment values are not added
to account redaction patterns. Do not put credentials in tasks,
account names, or other session metadata.

`account_strategy` defaults to `least-busy`, which counts Pending and Running
sessions per account. Ties use **account name order**, not file order, because
the current TOML merge uses sorted maps. `round-robin` cycles in account name
order; its last assignment is saved per harness in SQLite and survives restarts.
Both strategies skip accounts at `max_parallel`; omit that field for no account
limit. When every eligible account is full, the session stays Pending without
spawning an agent or creating a worktree. `ecc status <session-id>` shows it
waiting for an account. Run `ecc resume <session-id>` after capacity becomes
available; if the pool is still full, resume reports that it is still waiting.
Account waits survive restarts and require explicit resume. The daemon continues
to service the ordinary worktree queue. Existing worktrees and agent edits stay
on disk after an agent has spawned. See the [manual account tests](src/session/account_manual_tests.rs)
and [base behavior comparisons](src/session/account_parity_tests.rs).
Configurations without accounts keep their usual launch environment and behavior.
A harness with no configured account runs as it would without accounts, unless an account is pinned.

Pin a session with `ecc start --task "review changes" --agent claude --account work`.
`--account` is accepted by `start`, `delegate`, `template`, `assign`, `drain-inbox`,
`auto-dispatch`, `coordinate-backlog`, `maintain-coordination`, `rebalance-all`,
`rebalance-team`, `resume`, `run-session`, `schedule run-due`, `remote run`,
`daemon`, and the dashboard (also the default command). Other commands, including
`status`, reject it. Unknown accounts and harness mismatches are errors. Explicit
pins persist with the session; `resume --account` replaces the pin, while an
automatically selected account is selected again on resume. A pin persists
across restarts; the session waits for that account rather than using another
free account. Removing that account, or changing its harness, makes resume
return an error naming the account. If all accounts are removed, explicit resume
starts unpinned account waits with the normal harness environment.
`ecc status <session-id>` displays an `Account:`
field, using `-` for sessions with no account.

`rebalance-team` and `rebalance-all` can move unassigned intake handoffs between
delegates on different accounts of the same harness. A handoff already assigned
to a running or queued delegate stays with that delegate; running work never
changes accounts.

## Sequential Templates

A template step can name its prerequisites with `after`. Names refer to the
resolved names in the same template, including interpolated names and the default
`step 1`, `step 2`, and so on. Unknown, self-referencing, ambiguous, and cyclic
dependencies, including duplicate prerequisites, are rejected with the template
and step named in the error.
Literal dependency graphs are also checked when configuration loads; graphs
with variable names are checked after interpolation at launch.

```toml
[orchestration_templates.audit]
worktree = false

[[orchestration_templates.audit.steps]]
name = "inspect"
agent = "claude"
task = "Inspect /path/to/repository and describe its main risks."

[[orchestration_templates.audit.steps]]
name = "review"
agent = "codex"
after = ["inspect"]
task = "Review the reported risks against /path/to/repository."
```

The paths are placeholders. Run `ecc template audit` with the daemon running.
Only root steps queue immediately. Each later step queues after every named
prerequisite reaches Completed. Each step keeps its own agent, profile, allowed
tools, permission mode, and harness account pool. Templates with no `after`
continue to queue every step at launch with the existing whole-template slot
check and anchor handoffs.

Each named prerequisite contributes its captured text output to the later
step's task. The source is the output already kept in SQLite, subject to the
output ring's retention. Each contribution keeps the last 16 KiB of UTF-8 text
(`TEMPLATE_RESULT_MAX_BYTES` in `src/session/store/templates.rs`), with framing
outside that limit, cut forward to a UTF-8 boundary, with a visible note when
earlier output was truncated by the byte limit or the output ring is full.
It is labelled with the earlier step's name and
explicitly marked as untrusted data that must not override the step's own task,
tools, or permissions. Result framing and account redaction are covered by the
[sequential regression tests](src/session/template_regression_tests.rs).
Capture redacts only the assigned account's environment values,
as described under Account Pools. Result passing redacts every configured
account's environment values across all harnesses before truncation. The passed
block, including its label, boundary, and joins, is redacted at the result
boundary. Values supplied in template configuration and launch variables retain
the same storage and display behavior as templates without `after`; redaction
does not change a step's own task, effective profile, or permissions. This passes
text; it does not base a later worktree on an earlier step's branch.

If a step ends Failed or Stopped, its direct and transitive dependents never
start. The blocked record persists even if that earlier session is resumed.
`ecc status <step-session-id>` identifies the failed or stopped prerequisite;
it also shows the prerequisites of a step that is still waiting.
The [blocked-step regression tests](src/session/template_regression_tests.rs)
cover resume refusal and listing and deletion of steps that never queued.
Run `ecc stop <step-session-id>` to cancel a waiting step before it starts.
For an unblocked waiting step, status then identifies it as Stopped, and its
dependents stay blocked. Stopping an already blocked step preserves the
prerequisite named in its status.

Deleting a prerequisite or handoff anchor is refused while an unstarted,
unblocked step still needs it, including the launch source or root anchor used
by later descendants. The refusal names a waiting step. Deletion is available
again after all consumers have started or become blocked.

The CLI and manager launch check count only root steps against free session
slots. A later step that is ready while all slots are occupied waits for a slot;
the daemon checks readiness each heartbeat and respects `max_parallel_sessions`.
The dashboard retains its whole-template capacity pre-check. Worktree capacity
and account limits still apply; a full account pool requires explicit resume,
as described under Account Pools.

Waiting definitions and claimed Pending sessions persist in SQLite. Claiming a
step and creating its session share one transaction. A separate database claim
before a runner starts the harness prevents duplicate execution when a Pending
session is delivered again. Restarting the daemon therefore resumes waiting
chains, including prerequisites that completed while it was offline, without
creating a second session for a step. Existing crashed-session handling still
marks a lost running process Failed; it does not retry the step. The final
prerequisite check and harness spawn share a database write lock with failure
propagation. A step's activation error is recorded in its status while other
steps continue through the activation pass.

## Bounded Harness Evaluation

ECC2 now has an operator-driven configuration registry and promotion gate. Candidate JSON is canonicalized and addressed by its SHA-256 digest, with immutable trace/evidence references. Evaluation uses the same explicit unique seeds for candidate and active baseline through a pluggable Rust trait. The CLI exposes only a deterministic local recorded-measurements evaluator; it makes no network or process calls.

```bash
cargo run -- harness-eval record --config candidate.json --trace-ref trace://run-1 --evidence-ref evidence://review-1
cargo run -- harness-eval activate-initial <sha256> --evidence-ref evidence://baseline-approval
cargo run -- harness-eval run --candidate <sha256> --baseline <sha256> --seed 1 --seed 2 --measurements measurements.json --evidence-ref evidence://evaluation-1 --min-samples 2 --min-mean-delta 0.05 --min-win-rate 0.5
cargo run -- harness-eval audit
```

`measurements.json` contains `{"evaluator":"recorded-v1","scores":{"<candidate>":{"1":0.9},"<baseline>":{"1":0.7}},"health":{"<candidate>":true}}` (with every requested seed present). Promotion requires minimum paired samples, arithmetic-mean delta, and per-seed win rate. SQLite transactions update the active pointer and append audit evidence atomically; a failed or errored candidate-keyed recorded health assertion restores the prior pointer and records rollback evidence. Database triggers reject update/deletion of candidate, evaluation, and audit rows.

Limitations: this performs one bounded deterministic comparison. It does not autonomously rewrite prompts or `ecc2.toml`, train/fine-tune a model, implement or claim reinforcement learning, call a network service, or run shell-command evaluators. It does not alter running sessions. Evidence references and scores are operator assertions, not authenticated truth. Arithmetic gates do not establish statistical significance. The active pointer is registry state only; it is not automatic deployment into a harness runtime.

## Validate

```bash
cd ecc2
cargo test
```

## What Is Still Missing

The alpha is missing the higher-level operator surface that defines ECC 2.0:

- richer multi-agent orchestration
- explicit agent-to-agent delegation and summaries
- visual worktree / diff review surface
- stronger external harness compatibility
- deeper memory and roadmap-aware planning layers
- release packaging and installer story

## Repo Rule

Do not market `ecc2/` as done just because the scaffold builds.

The right framing is:

- ECC 2.0 alpha exists
- it is usable for internal/operator testing
- it is not the complete release yet
