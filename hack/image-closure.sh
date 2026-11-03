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
    [ -e "$dest$p" ] || [ -L "$dest$p" ] || cp -a --parents "$p" "$dest"
    [ -L "$p" ] || break
    t="$(readlink "$p")"
    case "$t" in
      /*) p="$t" ;;
      *) p="$(dirname "$p")/$t" ;;
    esac
  done
}

for prog in "$@"; do
  [ -e "$prog" ] || { echo "image-closure: $prog not found" >&2; exit 1; }
  copy "$prog"
  real="$(readlink -f "$prog")"
  # ldd prints "name => /path (addr)" for each library and "/path (addr)" for
  # the loader; every absolute path on a line is a file to carry.
  ldd "$real" | tr -s ' \t()' '\n\n\n\n' | grep '^/' | sort -u | while read -r lib; do
    [ "$lib" = "$real" ] || copy "$lib"
  done
done

for prog in "$@"; do
  [ -e "$dest$prog" ] || { echo "image-closure: $prog missing from $dest" >&2; exit 1; }
done
