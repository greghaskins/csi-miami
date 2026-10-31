// Package deploy holds the kustomize manifests of the driver (kustomize/base,
// kustomize/examples, kustomize/overlays/e2e) and the test that pins their
// security-relevant content.
//
// The package has no code. Its test renders the manifests with the kustomize
// libraries that kubectl embeds, so what it checks is what `kubectl apply -k`
// applies, and asserts the RBAC rule sets, the CSIDriver fields, the security
// posture and host mounts of both pods, the resource limits, the image pins,
// the ports and the e2e overlay's difference from the base. It also compares
// the rendered base with testdata/base.golden.yaml, so a review sees every
// manifest change as a diff; regenerate it with
// `go test ./deploy -update`.
package deploy
