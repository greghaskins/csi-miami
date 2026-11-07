//go:build smoke

package prereq

import "testing"

// The real kernel can provide Adiantum even before any mapping has used it, so
// the probe finds it where /proc/crypto would not list it. The runners' kernels
// have Adiantum: a failure here is a kernel without it (or one that refuses
// AF_ALG), and the test fails rather than skips.
func TestSmokeAdiantumBind(t *testing.T) {
	if err := checkAdiantum(bindAlg); err != nil {
		t.Fatal(err)
	}
}
