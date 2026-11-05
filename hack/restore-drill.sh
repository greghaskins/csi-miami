#!/bin/bash
# Run the restore drill of docs/restore.md, line for line, against a copy of a
# volume's directory, and leave the image at <output> (default: volume.img in
# the directory). The lines between the markers are the drill; this script
# adds only the safety flags, the cd, and the final move.
set -euo pipefail

dir=${1:?usage: restore-drill.sh <volume-dir> [<output>]}
out=${2:-}
cd "$dir"

# drill-begin
m=manifests/$(cat current)
grep -v '^#' "$m" | sed 's|^\(..\)\(.*\)|\1\2  chunks/\1/\1\2|' | sha256sum -c --quiet
grep -v '^#' "$m" | sed 's|^\(..\)|chunks/\1/&|' | xargs cat > volume.img
# drill-end

if [ -n "$out" ]; then
	mv volume.img "$out"
fi
