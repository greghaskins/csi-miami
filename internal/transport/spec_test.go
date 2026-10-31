package transport

import (
	"regexp"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestValidate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		spec Spec
		// want is a substring of the error message; empty means valid.
		want string
	}{
		{"nfs3 host name", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/data"}, ""},
		{"nfs3 IPv4 literal", Spec{Kind: "nfs3", Server: "192.0.2.10", Export: "/export/data"}, ""},
		{"nfs3 IPv6 literal", Spec{Kind: "nfs3", Server: "[2001:db8::1]", Export: "/export/data"}, ""},
		{"nfs3 trailing slash accepted", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/data/"}, ""},
		{"nfs3 root export", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/"}, ""},
		{"local", Spec{Kind: "local", Path: "/export/data"}, ""},

		{"empty kind", Spec{}, "transport.kind is required"},
		{"unknown kind", Spec{Kind: "iscsi", Server: "nas.example.com", Export: "/x"}, "transport.kind must be"},
		{"kind is case sensitive", Spec{Kind: "NFS3", Server: "nas.example.com", Export: "/x"}, "transport.kind must be"},

		{"nfs3 missing server", Spec{Kind: "nfs3", Export: "/export/data"}, "transport.server is required"},
		{"nfs3 server with space", Spec{Kind: "nfs3", Server: "nas example", Export: "/x"}, "whitespace"},
		{"nfs3 server with tab", Spec{Kind: "nfs3", Server: "nas\texample", Export: "/x"}, "whitespace"},
		{"nfs3 server with NUL", Spec{Kind: "nfs3", Server: "nas\x00", Export: "/x"}, "control"},
		{"nfs3 server with slash", Spec{Kind: "nfs3", Server: "nas/example", Export: "/x"}, "'/'"},
		{"nfs3 server with port", Spec{Kind: "nfs3", Server: "nas.example.com:2049", Export: "/x"}, "':'"},
		{"nfs3 unbracketed IPv6", Spec{Kind: "nfs3", Server: "2001:db8::1", Export: "/x"}, "':'"},
		{"nfs3 bracket in the middle", Spec{Kind: "nfs3", Server: "nas[1]", Export: "/x"}, "':'"},
		{"nfs3 brackets around a name", Spec{Kind: "nfs3", Server: "[nas.example.com]", Export: "/x"}, "IPv6 literal"},
		{"nfs3 brackets around IPv4", Spec{Kind: "nfs3", Server: "[192.0.2.10]", Export: "/x"}, "IPv6 literal"},
		{"nfs3 IPv6 with zone", Spec{Kind: "nfs3", Server: "[fe80::1%eth0]", Export: "/x"}, "IPv6 literal"},
		{"nfs3 unclosed bracket", Spec{Kind: "nfs3", Server: "[2001:db8::1", Export: "/x"}, "IPv6 literal"},
		{"nfs3 empty brackets", Spec{Kind: "nfs3", Server: "[]", Export: "/x"}, "IPv6 literal"},

		{"nfs3 missing export", Spec{Kind: "nfs3", Server: "nas.example.com"}, "transport.export is required"},
		{"nfs3 relative export", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "export/data"}, "absolute"},
		{"nfs3 export with ..", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/../etc"}, "'..'"},
		{"nfs3 export ending in ..", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/.."}, "'..'"},
		{"nfs3 export with NUL", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export\x00/x"}, "NUL"},
		{"nfs3 with path", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/x", Path: "/y"}, "transport.path must be empty"},

		{"local missing path", Spec{Kind: "local"}, "transport.path is required"},
		{"local relative path", Spec{Kind: "local", Path: "data"}, "absolute"},
		{"local path with ..", Spec{Kind: "local", Path: "/data/../etc"}, "'..'"},
		{"local path with NUL", Spec{Kind: "local", Path: "/data\x00"}, "NUL"},
		{"local with server", Spec{Kind: "local", Path: "/data", Server: "nas.example.com"}, "transport.server must be empty"},
		{"local with export", Spec{Kind: "local", Path: "/data", Export: "/x"}, "transport.export must be empty"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.spec.Validate()
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want error containing %q", tc.want)
			}
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Errorf("code = %v, want InvalidArgument", got)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.want)
			}
		})
	}
}

func TestCanonical(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		spec Spec
		want string
	}{
		{"nfs3", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/data"}, "nfs3://nas.example.com/export/data"},
		{"nfs3 server lower-cased", Spec{Kind: "nfs3", Server: "NAS.Example.COM", Export: "/export/data"}, "nfs3://nas.example.com/export/data"},
		{"nfs3 export cleaned", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export//data/./"}, "nfs3://nas.example.com/export/data"},
		{"nfs3 root export", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/"}, "nfs3://nas.example.com/"},
		{"nfs3 IPv6 shortest form", Spec{Kind: "nfs3", Server: "[2001:DB8:0:0::1]", Export: "/x"}, "nfs3://[2001:db8::1]/x"},
		{"local", Spec{Kind: "local", Path: "/export/data"}, "local:///export/data"},
		{"local cleaned", Spec{Kind: "local", Path: "/export/data/"}, "local:///export/data"},
		{"unknown kind still renders", Spec{Kind: "x", Server: "A", Export: "/e", Path: "/p/"}, "x://a/e/p"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.spec.Canonical(); got != tc.want {
				t.Errorf("Canonical() = %q, want %q", got, tc.want)
			}
		})
	}
}

var hex12 = regexp.MustCompile(`^[0-9a-f]{12}$`)

func TestIDStable(t *testing.T) {
	t.Parallel()
	// Pinned values: a change here means the canonical form changed, and
	// every existing mount point would move with it.
	tests := []struct {
		spec Spec
		want string
	}{
		{Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/data"}, "6f54d7299e7b"},
		{Spec{Kind: "local", Path: "/export/data"}, "2101f71e67e1"},
	}
	for _, tc := range tests {
		got := tc.spec.ID()
		if !hex12.MatchString(got) {
			t.Fatalf("ID() = %q, want 12 lower-case hex characters", got)
		}
		if got != tc.want {
			t.Errorf("ID() of %q = %q, want %q", tc.spec.Canonical(), got, tc.want)
		}
	}
}

func TestIDEquivalentSpecs(t *testing.T) {
	t.Parallel()
	base := Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/data"}
	same := []Spec{
		{Kind: "nfs3", Server: "nas.example.com", Export: "/export/data/"},
		{Kind: "nfs3", Server: "NAS.example.com", Export: "/export/data"},
		{Kind: "nfs3", Server: "nas.example.com", Export: "/export//data"},
	}
	for _, s := range same {
		if s.ID() != base.ID() {
			t.Errorf("ID of %+v = %s, want %s", s, s.ID(), base.ID())
		}
	}

	v6a := Spec{Kind: "nfs3", Server: "[2001:db8::1]", Export: "/x"}
	v6b := Spec{Kind: "nfs3", Server: "[2001:DB8:0:0:0:0:0:1]", Export: "/x"}
	if v6a.ID() != v6b.ID() {
		t.Errorf("IPv6 spellings differ: %s vs %s", v6a.ID(), v6b.ID())
	}

	different := []Spec{
		{Kind: "nfs3", Server: "nas.example.com", Export: "/export/other"},
		{Kind: "nfs3", Server: "other.example.com", Export: "/export/data"},
		{Kind: "local", Path: "/export/data"},
		// Export case is significant.
		{Kind: "nfs3", Server: "nas.example.com", Export: "/Export/Data"},
	}
	for _, s := range different {
		if s.ID() == base.ID() {
			t.Errorf("ID of %+v equals the base ID %s", s, base.ID())
		}
	}
}

func TestMountPoint(t *testing.T) {
	t.Parallel()
	s := Spec{Kind: "local", Path: "/export/data"}
	want := "/var/lib/miami/transports/" + s.ID()
	if got := s.MountPoint("/var/lib/miami/"); got != want {
		t.Errorf("MountPoint() = %q, want %q", got, want)
	}
}

func TestParseSpec(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		params  map[string]string
		want    Spec
		wantErr bool
	}{
		{
			name: "nfs3",
			params: map[string]string{
				"transport.kind": "nfs3", "transport.server": "nas.example.com", "transport.export": "/export/data",
			},
			want: Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/data"},
		},
		{
			name:   "local, other keys ignored",
			params: map[string]string{"transport.kind": "local", "transport.path": "/data", "kind": "directory", "subdir": "x"},
			want:   Spec{Kind: "local", Path: "/data"},
		},
		{name: "no parameters", params: nil, wantErr: true},
		{name: "nfs3 without server", params: map[string]string{"transport.kind": "nfs3", "transport.export": "/x"}, wantErr: true},
		{name: "local with export", params: map[string]string{"transport.kind": "local", "transport.path": "/x", "transport.export": "/y"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseSpec(tc.params)
			if tc.wantErr {
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("ParseSpec() error = %v, want InvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseSpec() = %v", err)
			}
			if got != tc.want {
				t.Errorf("ParseSpec() = %+v, want %+v", got, tc.want)
			}
		})
	}
}
