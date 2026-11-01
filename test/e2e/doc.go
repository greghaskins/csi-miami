//go:build e2e

// Package e2e is the end-to-end test of the driver on a kind cluster: the
// exit criteria 1 to 6 of the M1 plan as tests, driven through the Kubernetes
// API with client-go.
//
// The test binary never creates the cluster. hack/e2e.sh (E2E_MODE=kind)
// creates it, loads the driver image, applies the e2e overlay, starts the NFS
// server for the nfs3 transport and then runs this binary with the
// environment that env_test.go lists; a missing value fails the run, it never
// skips a test. Each top-level test is one criterion and runs the variant of
// the transport the run is for ("local" or "nfs3"); a criterion that does not
// apply to a transport (a local volume is bound to its node, so two nodes
// cannot share it) runs nothing for it, and the script checks that every
// variant it expects reported a pass.
//
// Every wait polls a condition through the API (or the node's mount table)
// under a deadline and, when it times out, says what it waited for and what
// it last saw. Each test makes its resources in a namespace of its own and
// deletes it at the end, even on failure, waiting a bounded time for its
// PersistentVolumes to go. On failure it first writes the driver pods' logs,
// YAML of the claims, volumes, attachments and pods, the events and each
// node's mount table to the artifact directory.
//
// The test shells out to docker for exactly these reasons: to read a node's
// mount table, list and read a node's files, restart a node, and look at the
// NFS server container's export. Nothing else leaves the API.
package e2e
