# A kernel NFSv3 server for the e2e tests: the host kernel's nfsd, started and
# configured from inside a privileged container, exporting a tmpfs. Nothing
# here ships; it exists so that the nfs3 transport is tested against a real
# server over a real network path (see hack/nfs-server/run.sh).

# alpine:3.24.3, pinned by digest; bump the tag and the digest together.
FROM alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a

# nfs-utils provides rpc.nfsd, rpc.mountd and exportfs; rpcbind answers the
# kernel client's portmapper request. The packages are not pinned: the Alpine
# release above fixes the repository, and the harness uses only their stable
# command lines.
RUN apk add --no-cache nfs-utils rpcbind

COPY entrypoint.sh /entrypoint.sh

# portmapper, nfsd, and the fixed port rpc.mountd is started on.
EXPOSE 111/tcp 2049/tcp 20048/tcp

ENTRYPOINT ["/entrypoint.sh"]
