package volume

import (
	"maps"
	"testing"

	"github.com/greghaskins/csi-miami/internal/transport"
)

func TestContextEncode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		c    Context
		want map[string]string
	}{
		{
			name: "nfs3",
			c: Context{
				Kind:      KindDirectory,
				Transport: transport.Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/data"},
				Subdir:    "team-a/data",
			},
			want: map[string]string{
				"miami/kind":             "directory",
				"miami/transport.kind":   "nfs3",
				"miami/transport.server": "nas.example.com",
				"miami/transport.export": "/export/data",
				"miami/subdir":           "team-a/data",
			},
		},
		{
			name: "local omits the nfs3 keys",
			c: Context{
				Kind:      KindDirectory,
				Transport: transport.Spec{Kind: "local", Path: "/export/data"},
				Subdir:    "pvc-1",
			},
			want: map[string]string{
				"miami/kind":           "directory",
				"miami/transport.kind": "local",
				"miami/transport.path": "/export/data",
				"miami/subdir":         "pvc-1",
			},
		},
		{name: "zero value encodes to nothing", c: Context{}, want: map[string]string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.c.Encode(); !maps.Equal(got, tc.want) {
				t.Errorf("Encode() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestContextRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []Context{
		{
			Kind:      KindDirectory,
			Transport: transport.Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/data"},
			Subdir:    "team-a/data",
		},
		{
			Kind:      KindDirectory,
			Transport: transport.Spec{Kind: "nfs3", Server: "[2001:db8::1]", Export: "/export/data"},
			Subdir:    "pvc-1",
		},
		{
			Kind:      KindDirectory,
			Transport: transport.Spec{Kind: "local", Path: "/export/data"},
			Subdir:    "pvc-1",
		},
	}
	for _, c := range tests {
		t.Run(c.Transport.Canonical(), func(t *testing.T) {
			t.Parallel()
			if err := c.Validate(); err != nil {
				t.Fatalf("Validate() = %v", err)
			}
			got, err := DecodeContext(c.Encode())
			if err != nil {
				t.Fatalf("DecodeContext(Encode()) = %v", err)
			}
			if got != c {
				t.Errorf("round trip = %+v, want %+v", got, c)
			}
		})
	}
}

// nfsContext returns a valid encoded context with overrides applied; an
// override of "\x00" deletes the key.
func nfsContext(overrides map[string]string) map[string]string {
	m := map[string]string{
		"miami/kind":             "directory",
		"miami/transport.kind":   "nfs3",
		"miami/transport.server": "nas.example.com",
		"miami/transport.export": "/export/data",
		"miami/subdir":           "pvc-1",
	}
	for k, v := range overrides {
		if v == "\x00" {
			delete(m, k)
		} else {
			m[k] = v
		}
	}
	return m
}

func TestDecodeContext(t *testing.T) {
	t.Parallel()
	nfs := Context{
		Kind:      KindDirectory,
		Transport: transport.Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/data"},
		Subdir:    "pvc-1",
	}
	tests := []struct {
		name    string
		in      map[string]string
		want    Context
		wantErr string
	}{
		{name: "valid", in: nfsContext(nil), want: nfs},
		{
			name: "keys without the miami/ prefix are ignored",
			in: nfsContext(map[string]string{
				"csi.storage.k8s.io/pod.name": "p", "storage.kubernetes.io/csiProvisionerIdentity": "i", "miami": "x", "Miami/other": "y",
			}),
			want: nfs,
		},
		{
			name: "subdir is cleaned",
			in:   nfsContext(map[string]string{"miami/subdir": "a//b/"}),
			want: Context{Kind: KindDirectory, Transport: nfs.Transport, Subdir: "a/b"},
		},

		{name: "nil map", in: nil, wantErr: "miami/kind is required"},
		{name: "missing kind", in: nfsContext(map[string]string{"miami/kind": "\x00"}), wantErr: "miami/kind is required"},
		{name: "empty kind", in: nfsContext(map[string]string{"miami/kind": ""}), wantErr: "miami/kind is required"},
		{name: "unknown kind", in: nfsContext(map[string]string{"miami/kind": "image"}), wantErr: "miami/kind must be directory"},
		{name: "missing transport kind", in: nfsContext(map[string]string{"miami/transport.kind": "\x00"}), wantErr: "miami/transport.kind is required"},
		{name: "unknown transport kind", in: nfsContext(map[string]string{"miami/transport.kind": "smb"}), wantErr: "transport.kind must be"},
		{name: "missing subdir", in: nfsContext(map[string]string{"miami/subdir": "\x00"}), wantErr: "miami/subdir is required"},
		{name: "nfs3 missing server", in: nfsContext(map[string]string{"miami/transport.server": "\x00"}), wantErr: "transport.server is required"},
		{name: "nfs3 missing export", in: nfsContext(map[string]string{"miami/transport.export": "\x00"}), wantErr: "transport.export is required"},
		{name: "nfs3 relative export", in: nfsContext(map[string]string{"miami/transport.export": "export"}), wantErr: "absolute"},
		{name: "nfs3 with a path", in: nfsContext(map[string]string{"miami/transport.path": "/x"}), wantErr: "transport.path must be empty"},
		{
			name:    "local with a server",
			in:      map[string]string{"miami/kind": "directory", "miami/transport.kind": "local", "miami/transport.path": "/x", "miami/transport.server": "nas.example.com", "miami/subdir": "a"},
			wantErr: "transport.server must be empty",
		},
		{name: "absolute subdir", in: nfsContext(map[string]string{"miami/subdir": "/etc"}), wantErr: "relative"},
		{name: "subdir with ..", in: nfsContext(map[string]string{"miami/subdir": "a/../../b"}), wantErr: "'..'"},
		{name: "subdir is dot", in: nfsContext(map[string]string{"miami/subdir": "."}), wantErr: "transport root"},
		{name: "subdir with NUL", in: nfsContext(map[string]string{"miami/subdir": "a\x00"}), wantErr: "NUL"},

		{name: "unknown miami key", in: nfsContext(map[string]string{"miami/mode": "0777"}), wantErr: `unknown volume context key "miami/mode"`},
		{name: "prefix alone is an unknown key", in: nfsContext(map[string]string{"miami/": "x"}), wantErr: "unknown volume context key"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := DecodeContext(tc.in)
			if tc.wantErr != "" {
				wantInvalid(t, err, tc.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("DecodeContext() = %v", err)
			}
			if got != tc.want {
				t.Errorf("DecodeContext() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestContextValidate(t *testing.T) {
	t.Parallel()
	good := Context{
		Kind:      KindDirectory,
		Transport: transport.Spec{Kind: "local", Path: "/export/data"},
		Subdir:    "a/b",
	}
	if err := good.Validate(); err != nil {
		t.Fatalf("Validate() = %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(*Context)
		wantErr string
	}{
		{"no kind", func(c *Context) { c.Kind = "" }, "miami/kind"},
		{"bad transport", func(c *Context) { c.Transport.Path = "" }, "transport.path is required"},
		{"empty subdir", func(c *Context) { c.Subdir = "" }, "must not be empty"},
		{"subdir not cleaned", func(c *Context) { c.Subdir = "a/" }, "cleaned"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := good
			tc.mutate(&c)
			wantInvalid(t, c.Validate(), tc.wantErr)
		})
	}
}
