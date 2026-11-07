# Gate G1: running `miami bench`

Gate G1 asks one question: is the block server's data path (nbd, `miami
blockd`, LUKS2, ext4) within 1.5 times of the same stack over a loop device on
every phase of the design's section 3.2? `miami bench` builds both stacks from
the production pieces, times the phases, and compares them. The gate is run by
the owner on a Raspberry Pi 4 worker, twice: once with the chunk store on a
local directory and once with it on the NFSv3 transport mount.

CI runs the same harness at a small scale on an amd64 runner and only logs the
output. That proves the harness works; it says nothing about the Pi.

## What it measures

Eight phases, each timed, run in a fresh directory on the stack's mount; the
whole list runs for two rounds (`--rounds`) and the second round is the one
compared.

| Phase | What it does |
|---|---|
| create 2000 x 1 KiB files | `open`, `write`, `close`, no fsync |
| write 5000-file tree | 5000 files of 1 to 8 KiB over 50 directories, no fsync |
| stat sweep | `lstat` of everything written so far |
| 1000 x create + fsync | a 4 KiB file, fsynced, 1000 times |
| 1000 x 4 KiB overwrite + fsync | in place, in a file that has reached the disk |
| sequential write 512 MiB | 1 MiB writes, then one fsync |
| sequential read 512 MiB | after the page cache is dropped (root only) |
| rm -rf | removes the round's directory |

Between phases the file system is synced, outside the timer, so one phase's
writeback is not charged to the next. Both stacks are encrypted (LUKS2 with
Adiantum, a random key held in memory only) so the ratio is nbd against loop
and nothing else. The nbd run also reports, but does not gate, the time to
publish one generation after the rounds.

A time under 0.05 s counts as 0.05 s on both sides when the ratio is taken, so
timer noise on a very short phase cannot fail the gate.

## Getting the binary

The image is the artifact that ships, so take the binary from it. It is
statically linked; the host needs `cryptsetup` and `e2fsprogs` (the image
carries them too, but its entrypoint is the driver, not a shell).

On a Kubernetes node, which runs containerd rather than Docker, use the
node's own `ctr` (on k0s it is `k0s ctr`), as root:

    k0s ctr -n k8s.io images pull --platform linux/arm64 ghcr.io/greghaskins/csi-miami:<tag>
    mkdir -p /tmp/miami-img
    k0s ctr -n k8s.io images mount ghcr.io/greghaskins/csi-miami:<tag> /tmp/miami-img
    cp /tmp/miami-img/miami ./miami
    k0s ctr -n k8s.io images unmount --rm /tmp/miami-img
    ./miami version

`--rm` drops the snapshot `images mount` created; without it the next mount at
the same path fails with "already exists" and `cp` silently keeps the previous
binary. Always check that `./miami version` prints the tag you pulled.

Use `--platform linux/amd64` on an amd64 host. Where Docker is installed
instead:

    docker create --name miami-bench ghcr.io/greghaskins/csi-miami:<tag>
    docker cp miami-bench:/miami ./miami
    docker rm miami-bench
    ./miami version

Use the tag of the commit under test. Every CI run builds and pushes a
multi-arch image tagged `x-<content hash>` for the commit, on any branch (only
the release tags wait for `main`); the run's build job logs it as
`reports version x-<hash>`, and `./miami version` prints the same string. Load the kernel modules once: `modprobe nbd loop dm_crypt adiantum`.

The bench must run as root, and it refuses to start otherwise. It also refuses
any `--dir` or `--store` that holds something it did not create, and it only
ever writes inside a `miami-bench-*` directory it makes there. Use empty
directories.

`--out` refuses a file that already exists. No generation runs during the
phases (by G1's definition the gate measures the data path only); the one
publish happens after the rounds and is not gated.

The loop image is a sparse file of `--size` under `--dir`, so `--dir` needs
that much free space (8 GiB for the runs below) while the loop run is in
progress. It is removed at the end.

## One folder per attempt

Each attempt at the gate gets its own folders, named after the binary's
version, so a rerun with a newer image never reuses or overwrites an earlier
attempt's directories or results (`--out` refuses an existing file anyway).
Set `V` once in the shell you run both runs from:

    V=$(./miami version)       # e.g. x-b28161145cb4eba3
    mkdir -p "g1-$V"           # the results of this attempt

## Run 1: store on a local directory

    sudo mkdir -p "/var/tmp/g1/$V/dir" "/var/tmp/g1/$V/store"
    sudo unshare --mount --propagation private sh -c "
      ./miami bench --stack nbd  --dir /var/tmp/g1/$V/dir/nbd  --store /var/tmp/g1/$V/store --size 8Gi --out g1-$V/nbd-local.json
      ./miami bench --stack loop --dir /var/tmp/g1/$V/dir/loop --size 8Gi --out g1-$V/loop.json
    "
    ./miami bench --compare "g1-$V/nbd-local.json" "g1-$V/loop.json"

`--dir` is local disk in both runs: it holds the loop image, and the nbd
stack's cache. `unshare` keeps every mount the bench makes out of the host's
namespace. Ctrl-C is safe: the bench unmounts, closes the mapping, stops the
block server with a Stop request so the device disconnects, detaches the loop
device and removes its directories before it exits.

## Run 2: store on the NFSv3 transport mount

Mount the export with the driver's own option profile, then point `--store` at
an empty directory on it:

    sudo mkdir -p /mnt/g1-transport
    sudo mount -t nfs -o vers=3,proto=tcp,softerr,timeo=100,retrans=17,nolock,lookupcache=positive,actimeo=30,noacl <server>:<export> /mnt/g1-transport
    sudo mkdir -p "/mnt/g1-transport/g1-$V"
    sudo unshare --mount --propagation private ./miami bench --stack nbd \
        --dir "/var/tmp/g1/$V/dir/nbd" --store "/mnt/g1-transport/g1-$V" --size 8Gi --out "g1-$V/nbd-nfs.json"
    ./miami bench --compare "g1-$V/nbd-nfs.json" "g1-$V/loop.json"

The loop baseline is the same as in run 1 (local disk); only the nbd stack's
store moves.

The nbd run leaves its chunks under the store directory on the export, in a
`miami-bench-*` directory, until you remove them. After the run, with the
export still mounted: `sudo rm -rf "/mnt/g1-transport/g1-$V"`, then unmount
the export. The local `/var/tmp/g1/$V` folders are empty after a clean run;
remove them with `sudo rm -rf "/var/tmp/g1/$V"`. Keep `g1-$V/` for the
record.

## After a crash

A bench that is interrupted cleans up after itself. If it was killed with
SIGKILL or the machine lost power, its block server is sent SIGTERM and exits
by itself, but check for leftovers as root:

    sudo pkill -f 'miami blockd'            # only if one is still running
    sudo cryptsetup close miami-benchvol*   # the nbd run's mapping, if present
    sudo cryptsetup close miami-bench-*     # the loop run's mapping, if present

The loop device clears itself once nothing holds it. Run these inside the same
`unshare` shell if it is still open; otherwise the mounts went with it. Then
remove the `miami-bench-*` directories under `--dir` and `--store`.

## Reading the result

`--compare` prints one row per phase, the nbd and loop seconds of round 2, the
ratio, and PASS or FAIL against 1.5x, then a last line `G1: PASS` or `G1:
FAIL`. The exit status is 0 on PASS, 1 on FAIL and 3 when the files cannot be
compared (for example two runs at different `--scale`).

**G1 passes when both comparisons print `G1: PASS`.** One FAIL row in either
comparison fails the gate. Run 2 is the one that matters for the shipped
configuration; run 1 separates the data path's own cost from the transport's.

On a FAIL, stop. The plan stops with it, and the design's section 7 names the
fallback (`ublk`) for the owner to decide on. Do not retune and rerun until it
passes without saying so.

## What to paste into PR #41

- The Pi model, kernel version (`uname -r`) and the image tag.
- The output of `./miami bench --compare ...` for run 1 and for run 2, verbatim.
- The two nbd tables and the loop table (`./miami bench` prints one at the end
  of each run; the JSON files hold the same numbers).
- The line `G1: PASS` or `G1: FAIL` for each, and, on a FAIL, the rows that
  failed.
- Anything odd: a note the bench printed, a run that was interrupted, a phase
  that varied between rounds by more than a factor of two.

## Trying the harness on any Linux host

`hack/bench.sh` runs both stacks on the host as root at the scale you choose
and prints both tables and the ratios:

    sudo -E hack/bench.sh --scale small --size 2Gi

`--scale small` is the quick run CI uses. `--no-threshold` makes a FAIL exit 0.
`miami bench --stack none --dir <empty dir>` runs the phases on a plain
directory, without root, which is a quick check of the disk itself.
