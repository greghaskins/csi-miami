package volume

import "testing"

func TestResolveSubdir(t *testing.T) {
	t.Parallel()
	tokens := Tokens{PVCNamespace: "team-a", PVCName: "data", PVName: "pvc-0a1b"}
	tests := []struct {
		name     string
		template string
		tokens   Tokens
		want     string
		// wantErr is a substring of the error message; empty means valid.
		wantErr string
	}{
		{name: "default", template: DefaultSubdir, tokens: tokens, want: "pvc-0a1b"},
		{name: "all tokens", template: "${pvc.namespace}/${pvc.name}/${pv.name}", tokens: tokens, want: "team-a/data/pvc-0a1b"},
		{name: "token inside an element", template: "vol-${pvc.name}-x", tokens: tokens, want: "vol-data-x"},
		{name: "repeated token", template: "${pv.name}/${pv.name}", tokens: tokens, want: "pvc-0a1b/pvc-0a1b"},
		{name: "no tokens", template: "fixed/dir", tokens: Tokens{}, want: "fixed/dir"},
		{name: "result is cleaned", template: "a//b/./c/", tokens: tokens, want: "a/b/c"},
		{name: "lone dollar is literal", template: "a$b/$", tokens: tokens, want: "a$b/$"},
		{name: "unused empty token is fine", template: "${pv.name}", tokens: Tokens{PVName: "pvc-1"}, want: "pvc-1"},

		{name: "empty template", template: "", tokens: tokens, wantErr: "must not be empty"},
		{name: "absolute", template: "/abs/${pv.name}", tokens: tokens, wantErr: "relative"},
		{name: "dot dot element", template: "a/../b", tokens: tokens, wantErr: "'..'"},
		{name: "leading dot dot", template: "../${pv.name}", tokens: tokens, wantErr: "'..'"},
		{name: "trailing dot dot", template: "a/..", tokens: tokens, wantErr: "'..'"},
		{name: "dots inside an element are fine", template: "a..b", tokens: tokens, want: "a..b"},
		{name: "NUL", template: "a\x00b", tokens: tokens, wantErr: "NUL"},
		{name: "cleans to dot", template: ".", tokens: tokens, wantErr: "transport root"},
		{name: "cleans to dot, slashes", template: "././/", tokens: tokens, wantErr: "transport root"},

		{name: "token value with slash", template: "${pvc.name}", tokens: Tokens{PVCName: "a/b"}, wantErr: "single path element"},
		{name: "token value is dot dot", template: "${pvc.name}", tokens: Tokens{PVCName: ".."}, wantErr: "'.' or '..'"},
		{name: "token value is dot", template: "x/${pvc.name}", tokens: Tokens{PVCName: "."}, wantErr: "'.' or '..'"},
		{name: "empty token value", template: "${pvc.namespace}/${pv.name}", tokens: Tokens{PVName: "pvc-1"}, wantErr: "${pvc.namespace} is empty"},
		{name: "empty default token", template: DefaultSubdir, tokens: Tokens{}, wantErr: "${pv.name} is empty"},
		{name: "unknown token", template: "${pv.id}", tokens: tokens, wantErr: "unknown token ${pv.id}"},
		{name: "empty token name", template: "${}", tokens: tokens, wantErr: "unknown token ${}"},
		{name: "token names are case sensitive", template: "${PV.name}", tokens: tokens, wantErr: "unknown token"},
		{name: "unterminated token", template: "a/${pv.name", tokens: tokens, wantErr: "unterminated"},
		{name: "token value carrying a token", template: "${pvc.name}", tokens: Tokens{PVCName: "${pv.name}"}, wantErr: "unresolved token"},
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
