# Pinning the driver image

Every commit on `main` names the driver image it deploys, in
`deploy/kustomize/base/kustomization.yaml`:

```yaml
images:
  - name: ghcr.io/greghaskins/csi-miami
    newTag: x-<16 hex>          # hack/content-hash.sh of this tree
    digest: sha256:<64 hex>     # the multi-arch index; what the runtime pulls
```

The base renders this as `ghcr.io/greghaskins/csi-miami:x-<16 hex>@sha256:<64 hex>`.
The pin file is not an image input, so pinning never changes the tag.

## Pinning a branch

After you push a change, with its pull request open:

```sh
go run ./hack/pin pin
```

It runs four steps, printing `pin 1/4` to `pin 4/4`:

1. Merges `origin/main` in. It never rebases. A conflict in the pin file only
   takes main's pin and re-pins; any other conflict stops it.
2. Computes the content tag. If the tag is already pinned, it stops: there is
   nothing to do.
3. Pushes the head if needed, then waits for that commit's `manifest` job (up
   to an hour, `--timeout`).
4. Reads the tag and digest from the run's `image-ref` artifact, checks the
   tag, commits `pin: <tag>` and pushes.

On any failure it exits non-zero and pushes no pin commit. Each failure prints
the cause, the run's URL when there is one, and the next step. Ask for review
once `pin` is green: the pin push dismisses earlier approvals.

## The `pin` check

The `pin` job runs on every pull request and every push to `main`, after
`manifest` (also when `manifest` was skipped). `ci-ok` requires it. Each
failure prints its rule and the fix:

| Rule | Holds when | Usual fix |
|---|---|---|
| 1 | `newTag` is this tree's content tag | run the pin script |
| 2 | the registry has that tag, under the pinned digest | wait for `manifest`, then run the pin script |
| 3 | the base renders the image as `...@<pinned digest>` | set the image only in `images` |
| 4 | a tag the base branch already pins keeps its digest | escalate to the maintainer |

Rule 4 failing means the same tag now resolves to another index: possibly a
re-pushed tag. Do not re-pin it. Change a hashed input to get a new tag, and
escalate.

A documentation-only change keeps its tag, so it passes without a new pin.
