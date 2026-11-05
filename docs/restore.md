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
grep -v '^#' "$m" | sed 's|^\(..\)\(.*\)|\1\2  chunks/\1/\1\2|' | sha256sum -c --quiet
grep -v '^#' "$m" | sed 's|^\(..\)|chunks/\1/&|' | xargs cat > volume.img
cryptsetup open volume.img vol      # the namespace's key
mount /dev/mapper/vol /mnt
```

What each step does:

- Line 2 picks the manifest `current` names.
- Line 3 verifies every chunk against its name (a chunk is named by its
  SHA-256). Stop here if it prints anything. The all-zero chunk is a file like
  any other: every writer (the block server on each generation, `miami import`)
  stores it before a manifest that names it, as a sparse file of one chunk.
- Line 4 concatenates the chunks in manifest order into `volume.img`.
- If a chunk is missing from an offsite copy taken mid-publish (chunks are
  uploaded before the manifest, but a mirror may copy in any order), use the
  previous manifest in `manifests/` instead of `current`: set `m` to it.

`miami restore` does the same for a volume larger than the restoring
machine's disk: it verifies every chunk before it writes a byte, then
assembles the image into a new file (never overwriting one), leaving holes
for zero chunks, with memory bounded by one read buffer. The image is assembled into a hidden
file `.<name>.tmp-<random>` beside the output and linked to its name only when
complete, so the output either does not exist or is whole. A restore that was
killed leaves that hidden file, full size, behind: delete it.

```sh
miami restore --store /path/to/pvc-<uuid> --out /path/to/volume.img [--manifest 3.17]
```

`hack/restore-drill.sh <volume-dir> [<output>]` runs the drill above, line for
line, and is what the end-to-end test compares against `miami restore`.

## Importing an existing image

```sh
miami import --image /path/to/disk.img --store /path/to/pvc-<name> [--capacity BYTES] [--chunk-size BYTES]
```

writes a raw image file as manifest `0.1` of a new volume (epoch 0, below the
epoch 1 of the volume's first holder). The image's own format decides the class
(LUKS2 header: encrypted class; ext4 superblock: `-plain` class); anything else
is refused. It needs no key and never opens the image.

- The device is the capacity (`--capacity`, the PV's `requests.storage`; the
  image size when absent) rounded up to whole chunks, zero padded; the
  filesystem inside never sees the padding. The chunk size is `--chunk-size`
  (the class's `chunkSize`) or, like the controller does for a class without
  one, derived from the capacity. The command prints the `chunkSize` the class
  needs and the PV capacity to use.
- All-zero chunks are named in the manifest and stored once, as one sparse
  file, before the manifest is written.
- It refuses a directory that already has `current`, a manifest, a holder file
  or an epoch claim, and `current` is created only if absent, so an import can
  never replace a volume that went live meanwhile.
- An import killed part way is not resumed. If the directory holds only
  `manifests/0.1`, with no `current` and no holder, the command says so;
  remove `manifests/0.1` (chunks already stored are reused) and run it again.
