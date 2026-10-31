#!/usr/bin/env bash
# Fails if any tracked file, or any commit message on the current branch
# beyond the base, contains a forbidden token. A token is a maximal run of
# letters and digits, compared in lower case by HMAC-SHA256 under a key
# that is not in this repository (SCRUB_KEY, or the file named by
# SCRUB_KEY_FILE), so the list below reveals nothing without it.
#
#   hack/scrub.sh            # tracked files + commits since origin/main
#   hack/scrub.sh --files    # tracked files only
#   BASE=<ref> hack/scrub.sh # commits since <ref>
set -euo pipefail

key=${SCRUB_KEY:-}
if [[ -z "$key" && -n "${SCRUB_KEY_FILE:-}" && -r "$SCRUB_KEY_FILE" ]]; then
  key=$(cat "$SCRUB_KEY_FILE")
fi
if [[ -z "$key" ]]; then
  echo "scrub: no key (set SCRUB_KEY or SCRUB_KEY_FILE)" >&2
  exit 2
fi

forbidden='
6226a189edcbb7a380326d8d93ea847ff6137aac58d70791ae75be9fb88293b4
1647b7ff7dcc1a565867ecf458a0455db45e05c63e8c3cacad878c0094f64a09
470861408bcef3e241ceaad1eb6f6d1adae299bce9ff65ad4adaff04f826120a
2d918df038829c7897f7999d71a2b87acff9cd6129e0389c428f14eebc9cd9d3
fc0968fb8afb6016fa59f59a9d9b846bc33000274929aee0c2e561c50d909ea1
'

check() {
  local label=$1
  grep -o -E '[A-Za-z0-9]+' | tr 'A-Z' 'a-z' | sort -u | while read -r tok; do
    h=$(printf '%s' "$tok" | openssl dgst -sha256 -hmac "$key" | sed 's/^.*= //')
    if grep -q -x "$h" <<<"$forbidden"; then
      echo "scrub: forbidden token in $label" >&2
      exit 1
    fi
  done
}

fail=0
while IFS= read -r f; do
  check "file $f" <"$f" || fail=1
  printf '%s' "$f" | check "path $f" || fail=1
done < <(git ls-files)

if [[ "${1:-}" != "--files" ]]; then
  base=${BASE:-origin/main}
  if git rev-parse -q --verify "$base" >/dev/null; then
    while IFS= read -r c; do
      git log -1 --format='%B' "$c" | check "commit message $c" || fail=1
    done < <(git rev-list "$base..HEAD")
  fi
fi

if [[ $fail -ne 0 ]]; then exit 1; fi
echo "scrub: clean"
