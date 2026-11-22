#!/usr/bin/env bash
# Tests for release-checks-ref.sh. Offline: `gh` is a stub that answers the
# compare API from $STUB_COMPARE (a status word, or "error").
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/bin"
cat > "$tmp/bin/gh" <<'STUB'
#!/usr/bin/env bash
[ "${STUB_COMPARE:-}" = error ] && { echo "gh: Server Error (HTTP 502)" >&2; exit 1; }
case "$*" in *compare/main...*) echo "${STUB_COMPARE:-identical}";; *) echo "unexpected gh call: $*" >&2; exit 1;; esac
STUB
chmod +x "$tmp/bin/gh"
A=$(printf 'a%.0s' {1..40}); B=$(printf 'b%.0s' {1..40}); P=$(printf 'c%.0s' {1..40})
n=0; failed=0
# case <name> <want: pass|fail> [rule to see in output] ; workflow YAML on stdin
case_() {
  local name="$1" want="$2" rule="${3:-}" d out rc
  n=$((n + 1)); d="$tmp/c$n"; mkdir -p "$d/.github/workflows"; cat > "$d/.github/workflows/w.yml"
  out="$(cd "$d" && PATH="$tmp/bin:$PATH" GITHUB_STEP_SUMMARY=/dev/null "$here/release-checks-ref.sh" 2>&1)"; rc=$?
  if { [ "$want" = pass ] && [ $rc -eq 0 ]; } || { [ "$want" = fail ] && [ $rc -ne 0 ] && grep -q "rule ${rule}" <<<"$out"; }; then
    echo "ok $n - $name"
  else
    echo "not ok $n - $name (want $want${rule:+ rule $rule}, rc=$rc)"; while IFS= read -r l; do echo "#   $l"; done <<<"$out"; failed=1
  fi
}
case_ "no references, read-only jobs" pass <<Y
on: push
permissions: {contents: read}
jobs: {a: {runs-on: x, steps: [{uses: jdx/mise-action@v2}]}}
Y
case_ "one full SHA on main (identical)" pass <<Y
on: push
permissions: {contents: read}
jobs: {p: {uses: "greghaskins/release-checks/.github/workflows/pin.yml@$A"}}
Y
STUB_COMPARE=behind case_ "SHA behind main" pass <<Y
on: push
permissions: {contents: read}
jobs: {p: {uses: "greghaskins/release-checks/.github/workflows/pin.yml@$A"}}
Y
STUB_COMPARE=diverged case_ "SHA diverged from main (a fork's commit)" fail 2 <<Y
on: push
permissions: {contents: read}
jobs: {p: {uses: "greghaskins/release-checks/.github/workflows/pin.yml@$A"}}
Y
STUB_COMPARE=ahead case_ "SHA ahead of main (unmerged)" fail 2 <<Y
on: push
permissions: {contents: read}
jobs: {p: {uses: "greghaskins/release-checks/.github/workflows/pin.yml@$A"}}
Y
STUB_COMPARE=error case_ "compare API error fails" fail 2 <<Y
on: push
permissions: {contents: read}
jobs: {p: {uses: "greghaskins/release-checks/.github/workflows/pin.yml@$A"}}
Y
case_ "branch ref" fail 1 <<Y
on: push
permissions: {contents: read}
jobs: {p: {uses: "greghaskins/release-checks/.github/workflows/pin.yml@main"}}
Y
case_ "two different SHAs" fail 1 <<Y
on: push
permissions: {contents: read}
jobs:
  p: {uses: "greghaskins/release-checks/.github/workflows/pin.yml@$A"}
  q: {uses: "greghaskins/release-checks/.github/workflows/ref.yml@$B"}
Y
case_ "checkout of release-checks at a branch" fail 1 <<Y
on: push
permissions: {contents: read}
jobs: {a: {runs-on: x, steps: [{uses: "actions/checkout@$P", with: {repository: greghaskins/release-checks, ref: main}}]}}
Y
case_ "checkout of release-checks with no ref" fail 1 <<Y
on: push
permissions: {contents: read}
jobs: {a: {runs-on: x, steps: [{uses: "actions/checkout@$P", with: {repository: greghaskins/release-checks}}]}}
Y
case_ "write job uses mise-action" fail 3 <<Y
on: push
jobs: {a: {runs-on: x, permissions: {packages: write}, steps: [{uses: "jdx/mise-action@$P"}]}}
Y
case_ "no permissions anywhere counts as write" fail 3 <<Y
on: push
jobs: {a: {runs-on: x, steps: [{uses: "jdx/mise-action@$P"}]}}
Y
case_ "workflow write-all inherited" fail 3 <<Y
on: push
permissions: write-all
jobs: {a: {runs-on: x, steps: [{uses: actions/checkout@v5}]}}
Y
case_ "job read-all overrides workflow write" pass <<Y
on: push
permissions: {contents: write}
jobs: {a: {runs-on: x, permissions: read-all, steps: [{uses: actions/checkout@v5}]}}
Y
case_ "job empty permissions" pass <<Y
on: push
permissions: {contents: write}
jobs: {a: {runs-on: x, permissions: {}, steps: [{uses: actions/checkout@v5}]}}
Y
case_ "read-only job with a secret, unpinned action" fail 3 <<'Y'
on: push
permissions: {contents: read}
jobs: {a: {runs-on: x, steps: [{uses: actions/checkout@v5}, {run: x, env: {K: "${{ secrets.GITLEAKS_CONFIG_TOML }}"}}]}}
Y
case_ "GITHUB_TOKEN alone is not a secret" pass <<'Y'
on: push
permissions: {contents: read}
jobs: {a: {runs-on: x, steps: [{uses: actions/checkout@v5}, {run: x, env: {T: "${{ secrets.GITHUB_TOKEN }}"}}]}}
Y
case_ "write job, local action outside .github" fail 3 <<Y
on: push
jobs: {a: {runs-on: x, permissions: {contents: write}, steps: [{uses: ./tools/act}]}}
Y
case_ "write job, local action under .github" pass <<Y
on: push
jobs: {a: {runs-on: x, permissions: {contents: write}, steps: [{uses: ./.github/actions/act}]}}
Y
case_ "write job, unpinned docker action" fail 3 <<Y
on: push
jobs: {a: {runs-on: x, permissions: {contents: write}, steps: [{uses: "docker://alpine:3.20"}]}}
Y
case_ "write job, docker action by digest" pass <<Y
on: push
jobs: {a: {runs-on: x, permissions: {contents: write}, steps: [{uses: "docker://alpine@sha256:$(printf 'd%.0s' {1..64})"}]}}
Y
case_ "write job calls another repo's workflow at a tag" fail 3 <<Y
on: push
jobs: {a: {permissions: {contents: write}, uses: other/repo/.github/workflows/x.yml@v1}}
Y
case_ "secrets: inherit makes a job a secret job" fail 3 <<Y
on: push
permissions: {contents: read}
jobs: {a: {uses: other/repo/.github/workflows/x.yml@v1, secrets: inherit}}
Y
case_ "write job runs ko without KO_CONFIG_PATH" fail 4 <<Y
on: push
jobs: {a: {runs-on: x, permissions: {packages: write}, steps: [{uses: "ko-build/setup-ko@$P"}, {run: "ko build ./cmd/x"}]}}
Y
case_ "ko config from .ko.yaml in the tree" fail 4 <<Y
on: push
jobs: {a: {runs-on: x, permissions: {packages: write}, env: {KO_CONFIG_PATH: .ko.yaml}, steps: [{run: "ko build ./cmd/x"}]}}
Y
case_ "ko config under .github (job env)" pass <<Y
on: push
jobs: {a: {runs-on: x, permissions: {packages: write}, env: {KO_CONFIG_PATH: .github/ko.yaml}, steps: [{uses: "ko-build/setup-ko@$P"}, {run: "ko build ./cmd/x"}]}}
Y
case_ "ko config under .github (workflow env, workspace prefix)" pass <<'Y'
on: push
env: {KO_CONFIG_PATH: "${{ github.workspace }}/.github/ko.yaml"}
jobs: {a: {runs-on: x, permissions: {packages: write}, steps: [{run: "ko build ./cmd/x"}]}}
Y
case_ "ko step env must also be under .github" fail 4 <<Y
on: push
jobs: {a: {runs-on: x, permissions: {packages: write}, env: {KO_CONFIG_PATH: .github/ko.yaml}, steps: [{run: "ko build ./cmd/x", env: {KO_CONFIG_PATH: cfg/ko.yaml}}]}}
Y
case_ "ko in a read-only job is out of scope" pass <<Y
on: push
permissions: {contents: read}
jobs: {a: {runs-on: x, steps: [{run: "ko build ./cmd/x"}]}}
Y
case_ "owner and repo in another case" fail 1 <<Y
on: push
permissions: {contents: read}
jobs: {p: {uses: "GregHaskins/Release-Checks/.github/workflows/pin.yml@main"}}
Y
STUB_COMPARE=diverged case_ "another case still runs the compare" fail 2 <<Y
on: push
permissions: {contents: read}
jobs: {p: {uses: "GregHaskins/release-checks/.github/workflows/pin.yml@$A"}}
Y
case_ "secret by index" fail 3 <<'Y'
on: push
permissions: {contents: read}
jobs: {a: {runs-on: x, steps: [{uses: actions/checkout@v5}, {run: x, env: {K: "${{ secrets['GITLEAKS_CONFIG_TOML'] }}"}}]}}
Y
case_ "all secrets as JSON" fail 3 <<'Y'
on: push
permissions: {contents: read}
jobs: {a: {runs-on: x, steps: [{uses: actions/checkout@v5}, {run: x, env: {K: "${{ toJSON(secrets) }}"}}]}}
Y
case_ "secrets passed by name to a reusable workflow" fail 3 <<'Y'
on: push
permissions: {contents: read}
jobs: {a: {uses: other/repo/.github/workflows/x.yml@v1, secrets: {K: "${{ secrets.K }}"}}}
Y
case_ "GITHUB_TOKEN by index is not a secret" pass <<'Y'
on: push
permissions: {contents: read}
jobs: {a: {runs-on: x, steps: [{uses: actions/checkout@v5}, {run: x, env: {T: "${{ secrets['GITHUB_TOKEN'] }}"}}]}}
Y
case_ "a workflow that does not parse fails" fail 0 <<'Y'
on: push
jobs: {a: [unclosed
Y
echo "1..$n"
exit $failed
