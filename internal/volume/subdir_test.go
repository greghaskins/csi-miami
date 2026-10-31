package volume

import (
	"path"
	"strings"
	"testing"
)

const (
	pvUUID1 = "pvc-0a1b2c3d-1111-2222-3333-444455556666"
	pvUUID2 = "pvc-9f8e7d6c-aaaa-bbbb-cccc-ddddeeeeffff"
)

func TestResolveSubdir(t *testing.T) {
	t.Parallel()
	tokens := Tokens{PVCNamespace: "team-a", PVCName: "data"}
	elem255 := strings.Repeat("a", 255)
	tests := []struct {
		name     string
		template string
		tokens   Tokens
		want     string
		// wantErr is a substring of the error message; empty means valid.
		wantErr string
	}{
		{name: "empty template is no prefix", template: "", tokens: tokens, want: ""},
		{name: "dot is no prefix", template: ".", tokens: tokens, want: ""},
		{name: "dots and slashes are no prefix", template: "././/", tokens: tokens, want: ""},
		{name: "both tokens", template: "${pvc.namespace}/${pvc.name}", tokens: tokens, want: "team-a/data"},
		{name: "one token", template: "${pvc.namespace}", tokens: tokens, want: "team-a"},
		{name: "token inside an element", template: "vol-${pvc.name}-x", tokens: tokens, want: "vol-data-x"},
		{name: "repeated token", template: "${pvc.name}/${pvc.name}", tokens: tokens, want: "data/data"},
		{name: "no tokens", template: "fixed/dir", tokens: Tokens{}, want: "fixed/dir"},
		{name: "result is cleaned", template: "a//b/./c/", tokens: tokens, want: "a/b/c"},
		{name: "lone dollar is literal", template: "a$b/$", tokens: tokens, want: "a$b/$"},
		{name: "unused empty token is fine", template: "${pvc.name}", tokens: Tokens{PVCName: "c"}, want: "c"},
		{name: "dots inside an element are fine", template: "a..b", tokens: tokens, want: "a..b"},
		{name: "255 byte element", template: elem255, tokens: tokens, want: elem255},
		{name: "near-PV-shaped element is fine", template: "pvc-0a1b2c3d-1111-2222-3333-44445555666", tokens: tokens, want: "pvc-0a1b2c3d-1111-2222-3333-44445555666"},
		{name: "upper-case UUID is refused too: an export may ignore case", template: "pvc-0A1B2C3D-1111-2222-3333-444455556666", tokens: tokens, wantErr: "PV name"},
		{name: "mixed-case prefix is refused too", template: "PVC-0a1b2c3d-1111-2222-3333-444455556666", tokens: tokens, wantErr: "PV name"},
		{name: "an upper-case PV name inside a longer path is refused", template: "a/Pvc-0a1b2c3d-1111-2222-3333-444455556666/b", tokens: tokens, wantErr: "PV name"},
		// Two claims whose tokens flatten to the same prefix are fine: the
		// volume's own name, appended later, tells the directories apart.
		{name: "flattened A", template: "${pvc.namespace}-${pvc.name}", tokens: Tokens{PVCNamespace: "a-b", PVCName: "c"}, want: "a-b-c"},
		{name: "flattened B", template: "${pvc.namespace}-${pvc.name}", tokens: Tokens{PVCNamespace: "a", PVCName: "b-c"}, want: "a-b-c"},

		{name: "absolute", template: "/abs/${pvc.name}", tokens: tokens, wantErr: "relative"},
		{name: "dot dot element", template: "a/../b", tokens: tokens, wantErr: "'..'"},
		{name: "leading dot dot", template: "../${pvc.name}", tokens: tokens, wantErr: "'..'"},
		{name: "trailing dot dot", template: "a/..", tokens: tokens, wantErr: "'..'"},
		{name: "NUL", template: "a\x00b", tokens: tokens, wantErr: "control"},
		{name: "newline", template: "a\nb", tokens: tokens, wantErr: "control"},
		{name: "DEL", template: "a\x7fb", tokens: tokens, wantErr: "control"},
		{name: "element too long", template: elem255 + "a", tokens: tokens, wantErr: "255 bytes"},
		{name: "subdir too long", template: strings.Repeat(elem255+"/", 16), tokens: tokens, wantErr: "3977"},
		{name: "PV-shaped element", template: "x/" + pvUUID1, tokens: tokens, wantErr: "PV name"},
		{name: "PV-shaped first element", template: pvUUID1 + "/x", tokens: tokens, wantErr: "PV name"},

		{name: "token value with slash", template: "${pvc.name}", tokens: Tokens{PVCName: "a/b"}, wantErr: "single path element"},
		{name: "token value is dot dot", template: "${pvc.name}", tokens: Tokens{PVCName: ".."}, wantErr: "'.' or '..'"},
		{name: "token value is dot", template: "x/${pvc.name}", tokens: Tokens{PVCName: "."}, wantErr: "'.' or '..'"},
		{name: "token value with newline", template: "${pvc.name}", tokens: Tokens{PVCName: "a\nb"}, wantErr: "control"},
		{name: "token value too long", template: "${pvc.name}", tokens: Tokens{PVCName: elem255 + "a"}, wantErr: "at most 255"},
		{name: "token value shaped like a PV name", template: "${pvc.name}", tokens: Tokens{PVCName: pvUUID1}, wantErr: "PV name"},
		{name: "empty token value", template: "${pvc.namespace}/${pvc.name}", tokens: Tokens{PVCName: "c"}, wantErr: "${pvc.namespace} is empty"},
		{name: "empty token value, no tokens given", template: "${pvc.name}", tokens: Tokens{}, wantErr: "${pvc.name} is empty"},
		{name: "pv.name is no longer a token", template: "${pv.name}", tokens: tokens, wantErr: `unknown token "pv.name"`},
		{name: "unknown token", template: "${pv.id}", tokens: tokens, wantErr: `unknown token "pv.id"`},
		{name: "empty token name", template: "${}", tokens: tokens, wantErr: `unknown token ""`},
		{name: "token names are case sensitive", template: "${PVC.name}", tokens: tokens, wantErr: "unknown token"},
		{name: "unterminated token", template: "a/${pvc.name", tokens: tokens, wantErr: "unterminated"},
		{name: "token value carrying a token", template: "${pvc.name}", tokens: Tokens{PVCName: "${pvc.name}"}, wantErr: "unresolved token"},
		{name: "token name is quoted", template: "${a\"b}", tokens: tokens, wantErr: `unknown token "a?b"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ResolveSubdir(tc.template, tc.tokens)
			if tc.wantErr != "" {
				wantInvalid(t, err, tc.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("ResolveSubdir(%q) = %v", tc.template, err)
			}
			if got != tc.want {
				t.Errorf("ResolveSubdir(%q) = %q, want %q", tc.template, got, tc.want)
			}
		})
	}
}

func TestUnknownTokenErrorIsSanitised(t *testing.T) {
	t.Parallel()
	// U+0085 is a C1 control: not printable, but above the C0 range.
	nel := string(rune(0x85))
	_, err := ResolveSubdir("${"+strings.Repeat("x", 100)+nel+"}", Tokens{})
	wantInvalid(t, err, "unknown token")
	if len(err.Error()) > 200 {
		t.Errorf("error is %d bytes; token names must be truncated", len(err.Error()))
	}
	if strings.Contains(err.Error(), nel) {
		t.Errorf("error %q contains a C1 control character", err.Error())
	}
}

// checkResolved asserts the invariants of every successful ResolveSubdir.
func checkResolved(t *testing.T, got string) {
	t.Helper()
	if got == "" {
		return
	}
	if path.IsAbs(got) {
		t.Errorf("result %q is absolute", got)
	}
	if strings.Contains(got, "${") {
		t.Errorf("result %q has an unresolved token", got)
	}
	if path.Clean(got) != got {
		t.Errorf("result %q is not cleaned", got)
	}
	if len(got) > maxSubdirLen {
		t.Errorf("result is %d bytes, over %d", len(got), maxSubdirLen)
	}
	for i := 0; i < len(got); i++ {
		if got[i] < 0x20 || got[i] == 0x7f {
			t.Errorf("result %q has a control character", got)
			break
		}
	}
	for _, elem := range strings.Split(got, "/") {
		switch {
		case elem == "" || elem == "." || elem == "..":
			t.Errorf("result %q has element %q", got, elem)
		case len(elem) > maxElementLen:
			t.Errorf("result %q has an element of %d bytes", got, len(elem))
		case pvNameShapeFold.MatchString(elem):
			t.Errorf("result %q has PV-shaped element %q", got, elem)
		}
	}
}

func FuzzResolveSubdir(f *testing.F) {
	f.Add("", "", "")
	f.Add(".", "ns", "claim")
	f.Add("${pvc.namespace}/${pvc.name}", "team-a", "data")
	f.Add("${pvc.namespace}-${pvc.name}", "a-b", "c")
	f.Add("a/../b", "ns", "claim")
	f.Add("/abs", "ns", "claim")
	f.Add("${pv.name}", "ns", "claim")
	f.Add("${pvc.name}", "ns", pvUUID1)
	f.Add("x/"+pvUUID2+"/y", "ns", "claim")
	f.Add("${pvc.name}", "ns", "${pvc.name}")
	f.Add("a\x00b/${pvc.name", "ns", "claim")
	f.Add("${pvc.name}/${pvc.name}", "ns", strings.Repeat("a", 255))
	f.Add("a//b/./c/", "ns", "claim")
	f.Fuzz(func(t *testing.T, template, ns, name string) {
		got, err := ResolveSubdir(template, Tokens{PVCNamespace: ns, PVCName: name})
		if err != nil {
			wantInvalid(t, err, "")
			return
		}
		checkResolved(t, got)
		// Whatever the controller accepts, the node must accept.
		c := Context{Kind: KindDirectory, Transport: validSpec(), Subdir: got}
		if err := c.Validate(); err != nil {
			t.Errorf("resolved subdir %q does not validate: %v", got, err)
		}
	})
}
