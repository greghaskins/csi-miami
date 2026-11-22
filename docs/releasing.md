# Pinning the driver image

Every commit on `main` names the driver image it deploys, in
`deploy/kustomize/base/kustomization.yaml`:

```yaml
images:
  - name: ghcr.io/greghaskins/csi-miami
    newTag: r-<64 hex>          # the content tag of this commit's input set
    digest: sha256:<64 hex>     # the multi-arch index; what the runtime pulls
```

The base renders this as `ghcr.io/greghaskins/csi-miami:r-<64 hex>@sha256:<64 hex>`.

## The image config and the content tag

`.github/image.json` names the image, the Dockerfile, the pin file and the
input set: the git pathspecs whose files are hashed into the tag and form the
whole build context. greghaskins/release-checks' `content-tag.sh` computes the
tag from a commit (never the work tree), and its README documents the format
and the rules. The pin file is never in the input set, so pinning does not
change the tag.

The image job builds with release-checks' `build.yml` and passes the tag to the
Dockerfile as `CONTENT_TAG`, which becomes the binary's version.

## Pinning a branch

After you push a change, with its pull request open:

```sh
go run ./hack/pin pin
```

It runs four steps, printing `pin 1/4` to `pin 4/4`:

1. Merges `origin/main` in. It never rebases. A conflict in the pin file only
   takes main's pin and re-pins; any other conflict stops it.
2. Computes the content tag with release-checks' `content-tag.sh`, at the SHA
   `.github/workflows/ci.yml` pins, so it agrees with the `pin` check. If the
   tag is already pinned, it stops: there is nothing to do.
3. Pushes the head if needed, then waits for that commit's `image / ref` job
   (up to two hours, `--timeout`). Each git or gh call it makes is stopped
   after five minutes.
4. Reads the tag and digest from the run's `image-ref` artifact, checks the
   tag, commits `pin: <tag>` and pushes.

On any failure it exits non-zero and pushes no pin commit. Each failure prints
the cause, the run's URL when there is one, and the next step. Ask for review
once `pin` is green: the pin push dismisses earlier approvals.

## The `pin` check

The `pin` job calls release-checks' `pin.yml` at a pinned SHA, so its check is
`pin / pin` and none of this repository's code runs in it. It runs on every
pull request and every push to `main`, after `image` (also when `image`
was skipped) and once `release-checks-ref` has passed, and checks the pull
request's head commit. `ci-ok` requires it. Each failure prints its rule and
the fix:

| Rule | Holds when | Usual fix |
|---|---|---|
| 1 | `newTag` is the commit's content tag | run the pin script |
| 2 | the registry has that tag, under the pinned digest | wait for the image job, then run the pin script |
| 3 | the base renders the image as `...@<pinned digest>` | set the image only in `images` |
| 4 | a tag the base branch already pins keeps its digest | escalate to the maintainer |

Rule 4 failing, or rule 2 saying "may have been re-pushed", means the same tag
now resolves to another index: possibly a re-pushed tag. Do not re-pin it.
Change a hashed input to get a new tag, and escalate.

A documentation-only change keeps its tag, so it passes without a new pin.

## Releases

A release is a `vX.Y.Z` tag on a commit of `main`, pushed once the commit has
a green `release-review` from the release reviewer and a green `pin`. The tag
push runs `.github/workflows/release.yml`, which calls release-checks'
`release.yml`: it checks the review, the version, that the commit is on `main`
and that the pinned image is still in the registry, then creates the GitHub
Release once.

Before the first tag, set the repository variable `RELEASE_REVIEWER_ID`
(Settings > Secrets and variables > Actions > Variables) to the release
reviewer's bot account id. Unset, the release job fails at its first rule and
names the variable.
