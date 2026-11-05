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

    docker create --name miami-bench ghcr.io/greghaskins/csi-miami:<tag>
    docker cp miami-bench:/miami ./miami
    docker rm miami-bench
    ./miami version

Use the tag of the commit under test (the publish job tags images by commit
sha). Load the kernel modules once: `modprobe nbd loop dm_crypt adiantum`.

The bench must run as root, and it refuses to start otherwise. It also refuses
any `--dir` or `--store` that holds something it did not create, and it only
ever writes inside a `miami-bench-*` directory it makes there. Use empty
directories.

## Run 1: store on a local directory

    sudo mkdir -p /var/tmp/g1/dir /var/tmp/g1/store
    sudo unshare --mount --propagation private sh -c '
      ./miami bench --stack nbd  --dir /var/tmp/g1/dir/nbd  --store /var/tmp/g1/store --size 8Gi --out nbd-local.json
      ./miami bench --stack loop --dir /var/tmp/g1/dir/loop --size 8Gi --out loop.json
    '
    ./miami bench --compare nbd-local.json loop.json

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
    sudo mkdir -p /mnt/g1-transport/g1
    sudo unshare --mount --propagation private ./miami bench --stack nbd \
        --dir /var/tmp/g1/dir/nbd --store /mnt/g1-transport/g1 --size 8Gi --out nbd-nfs.json
    ./miami bench --compare nbd-nfs.json loop.json

The loop baseline is the same as in run 1 (local disk); only the nbd stack's
store moves. Unmount the export and remove `g1` afterwards.

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
