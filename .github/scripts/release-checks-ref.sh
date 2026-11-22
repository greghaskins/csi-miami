#!/usr/bin/env bash
# release-checks-ref: the gate that every greghaskins/release-checks commit
# this repository calls is one full SHA on release-checks' main, and that no
# job holding a write token or a secret runs code from outside .github/.
#
# A commit SHA from a fork of release-checks looks like any other 40 hex
# digits, and GitHub resolves it; only this check, run here and not inside
# release-checks, tells them apart. It runs the PR's own copy of itself, so a
# PR could weaken it; such a PR changes .github/, which CODEOWNERS covers.
#
# Fails unless:
#   rule 1: every release-checks reference under .github/workflows/ is a full
#           SHA, and all name the same SHA;
#   rule 2: GitHub's compare of release-checks main...<sha> is `behind` or
#           `identical` (the SHA is on main); an API error fails;
#   rule 3: no job holding a write token or a secret uses mise-action, a
#           local action outside ./.github/, or an action or reusable
#           workflow not pinned to a full SHA (docker:// by sha256 digest);
#   rule 4: every such job that runs ko sets KO_CONFIG_PATH under .github/.
#
# A job holds a write token when its permissions (or, if it sets none, the
# workflow's) grant any `write`, or when neither sets permissions (GitHub's
# default may grant write). It holds a secret when it names one other than
# GITHUB_TOKEN, or passes `secrets: inherit`. `run:` lines are left to review.
#
# Usage: release-checks-ref.sh [repo root]   (default: the current directory)
# Needs: yq (mikefarah v4), jq, gh (with GH_TOKEN) from the runner image.
set -euo pipefail

RC_REPO=greghaskins/release-checks
cd "${1:-.}"
wf_dir=.github/workflows
fails=0
fail() { # rule, message
  echo "not ok: rule $1: $2"
  [ -z "${GITHUB_STEP_SUMMARY:-}" ] || echo "- release-checks-ref rule $1: $2" >> "$GITHUB_STEP_SUMMARY"
  fails=1
}

shopt -s nullglob
files=("$wf_dir"/*.yml "$wf_dir"/*.yaml)
[ "${#files[@]}" -gt 0 ] || { echo "no workflows under ${wf_dir}"; exit 0; }

# One JSON document per workflow: {file, wf}.
docs="$(for f in "${files[@]}"; do
  yq -o=json '.' "$f" | jq -c --arg f "$f" '{file: $f, wf: .}'
done)"

sha_re='^[0-9a-f]{40}$'

# --- rules 1 and 2: release-checks references --------------------------------
# `uses: greghaskins/release-checks/...@ref` (jobs and steps), and checkouts
# with `repository: greghaskins/release-checks`.
refs="$(jq -r --arg rc "$RC_REPO" '
  .file as $f | .wf.jobs // {} | to_entries[] | .key as $j | .value as $job
  | ( [$job.uses // empty] + [($job.steps // [])[] | .uses // empty]
      | .[] | select(startswith($rc + "/") or startswith($rc + "@"))
      | "\($f) \($j) \(split("@") | if length > 1 then .[-1] else "" end)" ),
    ( ($job.steps // [])[] | select((.with.repository // "") == $rc)
      | "\($f) \($j) \((.with.ref // "") | tostring)" )
' <<<"$docs")"

shas=()
while read -r f j ref; do
  [ -n "${f:-}" ] || continue
  if [[ "${ref:-}" =~ $sha_re ]]; then
    shas+=("$ref")
  else
    fail 1 "${f} job ${j}: ${RC_REPO} at '${ref:-}', not a full commit SHA"
  fi
done <<<"$refs"

uniq_shas="$(printf '%s\n' "${shas[@]+"${shas[@]}"}" | sed '/^$/d' | sort -u)"
count="$(grep -c . <<<"$uniq_shas" || true)"
if [ "$count" -gt 1 ]; then
  fail 1 "more than one ${RC_REPO} SHA: $(tr '\n' ' ' <<<"$uniq_shas")- pin one"
fi
if [ "$count" -eq 0 ]; then
  echo "ok: rules 1 and 2: no ${RC_REPO} reference"
else
  while read -r sha; do
    if ! status="$(gh api "repos/${RC_REPO}/compare/main...${sha}" --jq .status 2>&1)"; then
      fail 2 "cannot compare ${RC_REPO} main...${sha}: ${status}"
    elif [ "$status" = behind ] || [ "$status" = identical ]; then
      echo "ok: rule 2: ${RC_REPO}@${sha} is on main (${status})"
    else
      fail 2 "${RC_REPO}@${sha} is not on main (compare: ${status}); pin a commit from main"
    fi
  done <<<"$uniq_shas"
fi

# --- rules 3 and 4: jobs holding a write token or a secret -------------------
violations="$(jq -r '
  def perms_write(p):
    if p == null then true
    elif (p | type) == "string" then p == "write-all"
    else [p[]] | any(. == "write") end;
  def full_sha_ref: test("@[0-9a-f]{40}$");
  def pinned(u):
    if (u | startswith("./")) then true
    elif (u | startswith("docker://")) then (u | test("@sha256:[0-9a-f]{64}$"))
    else (u | full_sha_ref) end;
  def under_github(p):
    (p | tostring) as $s
    | ($s | startswith(".github/")) or ($s | startswith("./.github/"))
      or ($s | test("^\\$\\{\\{ *github\\.workspace *\\}\\}/\\.github/"));
  .file as $f | .wf as $wf
  | ($wf.jobs // {}) | to_entries[] | .key as $j | .value as $job
  | (if $job | has("permissions") then $job.permissions else $wf.permissions end) as $p
  | ($job | tostring) as $text
  | (perms_write($p)
     or ($text | test("secrets\\.(?!GITHUB_TOKEN\\b)[A-Za-z_]"))
     or (($job.secrets // null) == "inherit")) as $privileged
  | select($privileged)
  | ( [$job.uses // empty, ($job.steps // [])[].uses // empty][] as $u
      | if ($u | test("^jdx/mise-action(@|/|$)")) then
          "3\t\($f) job \($j) uses mise-action (\($u))"
        elif ($u | startswith("./")) and (($u | startswith("./.github/")) | not) then
          "3\t\($f) job \($j) uses a local action outside ./.github/ (\($u))"
        elif (pinned($u) | not) then
          "3\t\($f) job \($j) uses \($u), not pinned to a full SHA"
        else empty end ),
    ( ($job.steps // []) as $steps
      | select([$steps[] | ((.uses // "") | test("^ko-build/setup-ko@")) or ((.run // "") | test("(^|[^A-Za-z0-9_-])ko +(build|apply|resolve|publish)\\b"))] | any)
      | [ $wf.env.KO_CONFIG_PATH, $job.env.KO_CONFIG_PATH, ($steps[] | .env.KO_CONFIG_PATH) ]
      | map(select(. != null)) as $cfg
      | if ($cfg | length) == 0 then
          "4\t\($f) job \($j) runs ko without KO_CONFIG_PATH; set it under .github/"
        elif ([$cfg[] | under_github(.)] | all | not) then
          "4\t\($f) job \($j) sets KO_CONFIG_PATH outside .github/ (\($cfg | join(", ")))"
        else empty end )
' <<<"$docs")"

while IFS=$'\t' read -r rule msg; do
  [ -n "${rule:-}" ] || continue
  fail "$rule" "$msg"
done <<<"$violations"
[ -n "$violations" ] || echo "ok: rules 3 and 4: no privileged job uses mise-action, an unpinned or outside action, or ko without KO_CONFIG_PATH under .github/"

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  { echo "- tools: $(yq --version 2>&1 | head -n1); $(jq --version); $(gh --version | head -n1)"; } >> "$GITHUB_STEP_SUMMARY"
fi
exit "$fails"
