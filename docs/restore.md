# Restoring a volume without the driver

A volume's directory (`pvc-<uuid>/`) holds `chunks/<aa>/<sha256>`,
`manifests/<epoch>.<sequence>` and `current`. A volume is its newest complete
manifest, so a restore needs only coreutils and, for an encrypted volume,
cryptsetup and the namespace's key.

From the NAS share, or from the offsite bucket pulled into a directory, on any
Linux machine (work on a copy: the drill writes `volume.img` into the
directory it runs in):

```sh
cd pvc-<uuid>
m=manifests/$(cat current)
c=$(sed -n 's/^# chunk //p' "$m"); z=$(head -c "$c" /dev/zero | sha256sum | cut -d' ' -f1)
grep -v '^#' "$m" | grep -vx "$z" | sed 's|^\(..\)\(.*\)|\1\2  chunks/\1/\1\2|' | sha256sum -c --quiet
grep -v '^#' "$m" | while read -r h; do if [ "$h" = "$z" ]; then head -c "$c" /dev/zero; else cat "chunks/${h%"${h#??}"}/$h"; fi; done > volume.img
cryptsetup open volume.img vol      # the namespace's key
mount /dev/mapper/vol /mnt
```

What each step does:

- Line 2 picks the manifest `current` names; line 3 reads the chunk size from
  its header and computes the name of an all-zero chunk of that size.
- The all-zero chunk is not required to be a file: a writer names it in the
  manifest for every region never written or since discarded, and stores it
  only if a generation happened to upload one. The drill therefore never
  looks for it; it writes zeros of the chunk size where the manifest names
  it. If the zero chunk is present it is simply not read.
- Line 4 verifies every other chunk against its name (a chunk is named by its
  SHA-256). Stop here if it prints anything. A volume that is nothing but zero
  chunks has no lines for it to check and `sha256sum` complains; such a volume
  has no filesystem to restore.
- Line 5 concatenates the chunks in manifest order into `volume.img`.
- If a chunk is missing from an offsite copy taken mid-publish (chunks are
  uploaded before the manifest, but a mirror may copy in any order), use the
  previous manifest in `manifests/` instead of `current`: set `m` to it.

`miami restore` does the same for a volume larger than the restoring
machine's disk: it verifies every chunk before it writes a byte, then
assembles the image into a new file (never overwriting one), leaving holes
for zero chunks, with memory bounded by one read buffer.

```sh
miami restore --store /path/to/pvc-<uuid> --out /path/to/volume.img [--manifest 3.17]
```

`hack/restore-drill.sh <volume-dir> [<output>]` runs the drill above, line for
line, and is what the end-to-end test compares against `miami restore`.

## Importing an existing image

`miami import --image /path/to/disk.img --store /path/to/pvc-<name>` writes a
raw image file as manifest `0.1` of a new volume. It refuses a directory that
already has `current`, any manifest, or a holder file. The image's own format
decides the class (LUKS2 header: encrypted class; ext4 superblock: `-plain`
class); anything else is refused. It needs no key. An image that is not a whole
number of chunks is padded with zeros, which the filesystem inside never sees.
