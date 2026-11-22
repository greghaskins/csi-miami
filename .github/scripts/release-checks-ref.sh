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
# Fails unless (rule 0: every workflow file parses):
#   rule 1: every release-checks reference under .github/workflows/ is a full
#           SHA, and all name the same SHA;
#   rule 2: GitHub's compare of release-checks main...<sha> is `behind` or
#           `identical` (the SHA is on main); an API error fails;
#   rule 3: no job holding a write token or a secret uses mise-action, a
#           local action outside ./.github/, or an action or reusable
#           workflow not pinned to a full SHA (docker:// by sha256 digest);
#   rule 4: only a job that calls release-checks' build.yml holds
#           packages: write, and it passes no secrets; every workflow sets
#           top-level permissions, every reusable-workflow job its own, and
#           nothing grants write-all.
#
# A job holds a write token when its permissions (or, if it sets none, the
# workflow's) grant any `write`, or when neither sets permissions (GitHub's
# default may grant write). It holds a secret when it names one other than
# GITHUB_TOKEN (any use of the `secrets` context), or passes secrets to a
# reusable workflow. `run:` lines are left to review.
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

# One JSON document per workflow: {file, wf}. A file that does not parse
# fails the check (rule 0): it could hide any job.
docs=""
for f in "${files[@]}"; do
  if ! j="$(yq -o=json '.' "$f" 2>&1)" || ! d="$(jq -c --arg f "$f" '{file: $f, wf: .}' <<<"$j" 2>&1)"; then
    fail 0 "${f} does not parse: ${j%%$'\n'*}"
    continue
  fi
  docs+="$d"$'\n'
done

sha_re='^[0-9a-f]{40}$'

# --- rules 1 and 2: release-checks references --------------------------------
# `uses: greghaskins/release-checks/...@ref` (jobs and steps), and checkouts
# with `repository: greghaskins/release-checks`.
# GitHub matches owner and repository names in any case.
refs="$(jq -r --arg rc "$RC_REPO" '
  .file as $f | .wf.jobs // {} | to_entries[] | .key as $j | .value as $job
  | ( [$job.uses // empty] + [($job.steps // [])[] | .uses // empty]
      | .[] | select(ascii_downcase | startswith($rc + "/") or startswith($rc + "@"))
      | "\($f) \($j) \(split("@") | if length > 1 then .[-1] else "" end)" ),
    ( ($job.steps // [])[] | select(((.with.repository // "") | ascii_downcase) == $rc)
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

# --- rule 3: jobs holding a write token or a secret -------------------------
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
  .file as $f | .wf as $wf
  | ($wf.jobs // {}) | to_entries[] | .key as $j | .value as $job
  | (if $job | has("permissions") then $job.permissions else $wf.permissions end) as $p
  | ($job | tostring) as $text
  | (perms_write($p)
     or ($text
         | gsub("secrets\\.GITHUB_TOKEN\\b|secrets\\[ *[\\x27\\x22]GITHUB_TOKEN[\\x27\\x22] *\\]"; "")
         | test("\\bsecrets\\b"))
     or (($job.secrets // null) != null)) as $privileged
  | select($privileged)
  | ( [$job.uses // empty, ($job.steps // [])[].uses // empty][] as $u
      | if ($u | test("^jdx/mise-action(@|/|$)")) then
          "3\t\($f) job \($j) uses mise-action (\($u))"
        elif ($u | startswith("./")) and (($u | startswith("./.github/")) | not) then
          "3\t\($f) job \($j) uses a local action outside ./.github/ (\($u))"
        elif (pinned($u) | not) then
          "3\t\($f) job \($j) uses \($u), not pinned to a full SHA"
        else empty end )
' <<<"$docs")"

while IFS=$'\t' read -r rule msg; do
  [ -n "${rule:-}" ] || continue
  fail "$rule" "$msg"
done <<<"$violations"
[ -n "$violations" ] || echo "ok: rule 3: no privileged job uses mise-action, an unpinned action or an action outside ./.github/"

# --- rule 4: packages: write only where build.yml is called -----------------
# Parsed, not grepped. Every workflow sets permissions at its top level, every
# job that calls a reusable workflow sets its own (so the repository's default
# token permissions never apply), nothing grants write-all, and only a job that
# calls release-checks' build.yml holds packages: write, passing no secrets.
r4="$(jq -r --arg rc "$RC_REPO" '
  def grants_pkg(p): (p | type) == "object" and (p.packages // "") == "write";
  .file as $f | .wf as $wf
  | ( if ($wf | has("permissions") | not) then "4\t\($f) sets no top-level permissions"
      elif $wf.permissions == "write-all" then "4\t\($f) grants write-all at the top level"
      elif grants_pkg($wf.permissions) then "4\t\($f) grants packages: write at the top level"
      else empty end ),
    ( ($wf.jobs // {}) | to_entries[] | .key as $j | .value as $job
      | (if $job | has("permissions") then $job.permissions else $wf.permissions end) as $p
      | (($job.uses // "") | ascii_downcase | test("^" + $rc + "/\\.github/workflows/build\\.yml@")) as $builds
      | ( if (($job.uses // null) != null) and ($job | has("permissions") | not) then
            "4\t\($f) job \($j) calls a reusable workflow without its own permissions"
          else empty end ),
        ( if $job.permissions == "write-all" then "4\t\($f) job \($j) grants write-all"
          elif grants_pkg($p) and ($builds | not) then
            "4\t\($f) job \($j) holds packages: write but does not call \($rc)/.github/workflows/build.yml"
          elif grants_pkg($p) and (($job.secrets // null) != null) then
            "4\t\($f) job \($j) calls build.yml and passes secrets"
          else empty end ) )
' <<<"$docs")"
while IFS=$'\t' read -r rule msg; do
  [ -n "${rule:-}" ] || continue
  fail "$rule" "$msg"
done <<<"$r4"
[ -n "$r4" ] || echo "ok: rule 4: packages: write is held only by the job that calls build.yml"

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  [ "$fails" -ne 0 ] || echo "- release-checks-ref: all rules passed" >> "$GITHUB_STEP_SUMMARY"
  { echo "- tools: $(yq --version 2>&1 | head -n1); $(jq --version); $(gh --version | head -n1)"; } >> "$GITHUB_STEP_SUMMARY"
fi
exit "$fails"
