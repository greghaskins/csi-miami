#!/bin/sh
# Copies programs and everything they need to run into a directory, for an
# image with no shell and no package manager.
#
#   hack/image-closure.sh <dest> <program>...
#
# Each program keeps its own path under <dest>. A symlink is copied as a
# symlink together with what it points to, because some programs read the name
# they were started under (mkfs.ext4 is mke2fs under another name). The shared
# libraries come from `ldd`, with their symlink chains. The build fails if a
# program is missing from <dest> afterwards.
#
# Runs in the Dockerfile's tools stage (busybox sh on Alpine), and is one of
# the inputs of hack/content-hash.sh.
set -eu

dest="${1:?usage: image-closure.sh <dest> <program>...}"
shift

# copy PATH: PATH itself, then each link target in turn until a regular file.
copy() {
  p="$1"
  while :; do
    [ -e "$dest$p" ] || [ -L "$dest$p" ] || { mkdir -p "$dest$(dirname "$p")" && cp -a "$p" "$dest$p"; }
    [ -L "$p" ] || break
    t="$(readlink "$p")"
    case "$t" in
      /*) p="$t" ;;
      *) p="$(dirname "$p")/$t" ;;
    esac
  done
}

libs="$(mktemp)"
trap 'rm -f "$libs" "$libs.all"' EXIT

for prog in "$@"; do
  [ -e "$prog" ] || { echo "image-closure: $prog not found" >&2; exit 1; }
  copy "$prog"
  real="$(readlink -f "$prog")"
  # ldd output is captured first so its failure stops the build instead of
  # vanishing into a pipe. A library it cannot resolve is a hard error.
  out="$(ldd "$real" 2>&1)" || { echo "image-closure: ldd $real failed: $out" >&2; exit 1; }
  case "$out" in
    *"not found"*) echo "image-closure: $real has an unresolved library: $out" >&2; exit 1 ;;
  esac
  # ldd prints "name => /path (addr)" for each library and "/path (addr)" for
  # the loader; every absolute path on a line is a file to carry.
  printf '%s\n' "$out" | tr -s ' \t()' '\n\n\n\n' | grep '^/' | sort -u > "$libs" || true
  while read -r lib; do
    [ "$lib" = "$real" ] && continue
    copy "$lib"
    echo "$lib" >> "$libs.all"
  done < "$libs"
done

for prog in "$@"; do
  [ -e "$dest$prog" ] || { echo "image-closure: $prog missing from $dest" >&2; exit 1; }
done
if [ -f "$libs.all" ]; then
  while read -r lib; do
    [ -e "$dest$lib" ] || { echo "image-closure: library $lib missing from $dest" >&2; exit 1; }
  done < "$libs.all"
  rm -f "$libs.all"
fi
