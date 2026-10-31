# Working mode for M1

**Status:** Active
**Created:** 2026-10-31
**Reads first:** `2026-10-30-architecture-brief.md`, then
`2026-10-31-m1-implementation-plan.md`, then this file.

How the M1 tasks get built: one orchestrating agent session, one subagent
per task in its own worktree and branch, one pull request per task, a
human merge. This file is the hand-off between sessions; a new session
should be able to start from it with no other context.

## 1. Before the first code PR

Done once by the repository owner; the session checks, never assumes.

1. Repository secret `GITLEAKS_CONFIG_TOML` holds the gitleaks config. The
   `secret-scan` job fails closed without it, so no PR is green until it
   exists.
2. The agent environment exports `GITLEAKS_CONFIG_TOML` or its `_B64`
   form. First command of any session, from the checkout:
   `hack/secret-scan/require-config.sh` must exit 0 and print nothing.
   Subagents inherit the same environment, and git hooks live in the
   shared `.git` directory, so every worktree is covered once the variable
   is present.
3. A ruleset on `main`: pull request required; required checks `ci-ok`
   (once `ci.yml` exists, task T1) and `secret-scan`; squash merge with the
   default message set to pull request title only, so a PR body never
   becomes a commit message.
4. `mise install` has been run in the checkout at least once (installs the
   hooks). In a worktree, hooks are already shared.

## 2. Per task

For each row of the plan's work breakdown (section 15):

1. The orchestrator spawns one subagent in its own worktree with the task
   row, its acceptance check, and pointers to the brief and plan sections
   it touches. The brief is the only context the subagent has; it must be
   self-contained.
2. Branch name `m1/tNN-<slug>` from current `main`. The subagent writes
   code and tests, runs `make check`, pushes, opens the PR with a body that
   states what the acceptance check proved, and reports back.
3. The orchestrator reviews every PR itself and runs an independent
   review pass on anything touching `internal/reconcile`,
   `internal/transport` or `internal/mount`. Review comments go back to the
   same subagent, which keeps its context; a replacement subagent is a last
   resort.
4. Nothing merges with `ci-ok` red, and nothing is retried to make it
   green. A flaky test is a bug and gets its own fix.
5. The repository owner merges through task T6. After that, the owner may
   delegate merging to the orchestrator on green CI plus review; that is an
   explicit instruction, not a default.

## 3. Waves

At most three subagents at a time, in the plan's dependency order:

| wave | tasks | why |
|---|---|---|
| 1 | T1 | everything rebases on the scaffold and `ci.yml` |
| 2 | T2, T3, T5 | independent of each other |
| 3 | T4 | needs T3 |
| 4 | T6, T8 | T6 needs T3, T4, T5; T8 needs T2, T4 |
| 5 | T7 | needs T6 |
| 6 | T9 | needs T7, T8 |
| 7 | T10 | needs T9 |
| 8 | T11, T12 | last |

A wave starts when every PR of the previous wave it depends on is merged;
subagents branch from `main`, never from each other.

## 4. Models

- Orchestrator: the strongest model available. Its work is review and
  correctness judgment on a storage driver, where a wrong call is data
  loss, not a failed test.
- Implementation subagents: a mid-tier model; the tasks are well specified
  and the acceptance checks are mechanical.
- Documentation and mechanical tasks (T12, bumps): the smallest model.

## 5. Rules that apply to every branch and PR

- Nothing environment-specific enters the repository: no hostnames,
  addresses, device names, personal paths or references to prior projects.
  The secret-scan hooks enforce the private list; the rule is wider than
  the list.
- Pull request titles and bodies are scanned before they are posted (the
  pre-publish hook in `.claude/settings.json` does this for agents), and
  say what changed and what was verified, nothing about where it runs.
- Commit messages: imperative subject, a body that says why, no model or
  tool identifiers beyond the trailer lines the environment adds.
- Design changes go to `docs/design/` in the same PR as the code that
  needs them; the brief's decision log is updated, never rewritten.
- `make check` before every push, on the task's own branch.

## 6. Starting the next session

1. Confirm the config: `hack/secret-scan/require-config.sh`.
2. Read the three design files.
3. Check which PRs are open and which tasks are merged (`git log main`),
   and pick up at the first incomplete wave.
4. If the first design PR is not merged yet, nothing else starts.
