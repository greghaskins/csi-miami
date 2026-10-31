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
	long := strings.Repeat("a", 255)
	label63 := strings.Repeat("a", 63)
	tests := []struct {
		name string
		spec Spec
		// want is a substring of the error message; empty means valid.
		want string
	}{
		{"nfs3 host name", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/data"}, ""},
		{"nfs3 single label", Spec{Kind: "nfs3", Server: "nas", Export: "/export/data"}, ""},
		{"nfs3 hyphen and digits", Spec{Kind: "nfs3", Server: "nas-1.example.com", Export: "/x"}, ""},
		{"nfs3 digits in last label of a name", Spec{Kind: "nfs3", Server: "nas.example2", Export: "/x"}, ""},
		{"nfs3 IPv4 literal", Spec{Kind: "nfs3", Server: "192.0.2.10", Export: "/export/data"}, ""},
		{"nfs3 IPv6 literal", Spec{Kind: "nfs3", Server: "[2001:db8::1]", Export: "/export/data"}, ""},
		{"nfs3 63 byte label", Spec{Kind: "nfs3", Server: label63 + ".example.com", Export: "/x"}, ""},
		{"nfs3 255 byte element", Spec{Kind: "nfs3", Server: "nas", Export: "/" + long}, ""},
		{"local", Spec{Kind: "local", Path: "/export/data"}, ""},

		{"empty kind", Spec{}, "transport.kind is required"},
		{"unknown kind", Spec{Kind: "iscsi", Server: "nas.example.com", Export: "/x"}, "transport.kind must be"},
		{"kind is case sensitive", Spec{Kind: "NFS3", Server: "nas.example.com", Export: "/x"}, "transport.kind must be"},

		{"nfs3 missing server", Spec{Kind: "nfs3", Export: "/export/data"}, "transport.server is required"},
		{"nfs3 server with space", Spec{Kind: "nfs3", Server: "nas example", Export: "/x"}, "whitespace"},
		{"nfs3 server with tab", Spec{Kind: "nfs3", Server: "nas\texample", Export: "/x"}, "whitespace"},
		{"nfs3 server with NUL", Spec{Kind: "nfs3", Server: "nas\x00", Export: "/x"}, "control"},
		{"nfs3 server with newline", Spec{Kind: "nfs3", Server: "nas\nexample", Export: "/x"}, "control"},
		{"nfs3 server with slash", Spec{Kind: "nfs3", Server: "nas/example", Export: "/x"}, "'/'"},
		{"nfs3 server with port", Spec{Kind: "nfs3", Server: "nas.example.com:2049", Export: "/x"}, "':'"},
		{"nfs3 unbracketed IPv6", Spec{Kind: "nfs3", Server: "2001:db8::1", Export: "/x"}, "':'"},
		{"nfs3 bracket in the middle", Spec{Kind: "nfs3", Server: "nas[1]", Export: "/x"}, "'[' or ']'"},
		{"nfs3 trailing bracket", Spec{Kind: "nfs3", Server: "nas]", Export: "/x"}, "'[' or ']'"},
		{"nfs3 brackets around a name", Spec{Kind: "nfs3", Server: "[nas.example.com]", Export: "/x"}, "IPv6 literal"},
		{"nfs3 brackets around IPv4", Spec{Kind: "nfs3", Server: "[192.0.2.10]", Export: "/x"}, "IPv6 literal"},
		{"nfs3 IPv6 with zone", Spec{Kind: "nfs3", Server: "[fe80::1%eth0]", Export: "/x"}, "IPv6 literal"},
		{"nfs3 unclosed bracket", Spec{Kind: "nfs3", Server: "[2001:db8::1", Export: "/x"}, "IPv6 literal"},
		{"nfs3 empty brackets", Spec{Kind: "nfs3", Server: "[]", Export: "/x"}, "IPv6 literal"},
		{"nfs3 IPv6 not shortest", Spec{Kind: "nfs3", Server: "[2001:db8:0:0::1]", Export: "/x"}, "shortest"},
		{"nfs3 IPv6 upper case", Spec{Kind: "nfs3", Server: "[2001:DB8::1]", Export: "/x"}, "shortest"},
		{"nfs3 upper case host", Spec{Kind: "nfs3", Server: "NAS.example.com", Export: "/x"}, "lower case"},
		{"nfs3 Kelvin sign", Spec{Kind: "nfs3", Server: "Kas.example.com", Export: "/x"}, "may contain only"},
		{"nfs3 non-ASCII", Spec{Kind: "nfs3", Server: "näs.example.com", Export: "/x"}, "may contain only"},
		{"nfs3 comma", Spec{Kind: "nfs3", Server: "nas,vers=4", Export: "/x"}, "may contain only"},
		{"nfs3 underscore", Spec{Kind: "nfs3", Server: "nas_1.example.com", Export: "/x"}, "may contain only"},
		{"nfs3 leading hyphen", Spec{Kind: "nfs3", Server: "-nas.example.com", Export: "/x"}, "start or end with '-'"},
		{"nfs3 trailing hyphen", Spec{Kind: "nfs3", Server: "nas-.example.com", Export: "/x"}, "start or end with '-'"},
		{"nfs3 empty label", Spec{Kind: "nfs3", Server: "nas..example.com", Export: "/x"}, "1 to 63 bytes"},
		{"nfs3 trailing dot", Spec{Kind: "nfs3", Server: "nas.example.com.", Export: "/x"}, "1 to 63 bytes"},
		{"nfs3 64 byte label", Spec{Kind: "nfs3", Server: label63 + "a.example.com", Export: "/x"}, "1 to 63 bytes"},
		{"nfs3 254 byte name", Spec{Kind: "nfs3", Server: strings.Repeat(label63+".", 3) + strings.Repeat("a", 62), Export: "/x"}, "at most 253"},
		{"nfs3 bad IPv4", Spec{Kind: "nfs3", Server: "192.0.2.256", Export: "/x"}, "IPv4"},
		{"nfs3 short IPv4", Spec{Kind: "nfs3", Server: "192.0.2", Export: "/x"}, "IPv4"},
		{"nfs3 leading zero IPv4", Spec{Kind: "nfs3", Server: "192.0.2.010", Export: "/x"}, "IPv4"},

		{"nfs3 missing export", Spec{Kind: "nfs3", Server: "nas.example.com"}, "transport.export is required"},
		{"nfs3 relative export", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "export/data"}, "absolute"},
		{"nfs3 export with ..", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/../etc"}, "'..'"},
		{"nfs3 export ending in ..", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/.."}, "'..'"},
		{"nfs3 export with NUL", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export\x00/x"}, "control"},
		{"nfs3 export with newline", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export\n/x"}, "control"},
		{"nfs3 export with DEL", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export\x7f"}, "control"},
		{"nfs3 export root", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/"}, "filesystem root"},
		{"nfs3 export trailing slash", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/data/"}, "cleaned"},
		{"nfs3 export doubled slash", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export//data"}, "cleaned"},
		{"nfs3 export dot element", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/./data"}, "cleaned"},
		{"nfs3 export element too long", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/" + long + "a"}, "255 bytes"},
		{"nfs3 export too long", Spec{Kind: "nfs3", Server: "nas.example.com", Export: strings.Repeat("/"+long, 17)}, "4096 bytes"},
		{"nfs3 with path", Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/x", Path: "/y"}, "transport.path must be empty"},

		{"local missing path", Spec{Kind: "local"}, "transport.path is required"},
		{"local relative path", Spec{Kind: "local", Path: "data"}, "absolute"},
		{"local path with ..", Spec{Kind: "local", Path: "/data/../etc"}, "'..'"},
		{"local path with NUL", Spec{Kind: "local", Path: "/data\x00"}, "control"},
		{"local root", Spec{Kind: "local", Path: "/"}, "filesystem root"},
		{"local trailing slash", Spec{Kind: "local", Path: "/data/"}, "cleaned"},
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
		{"nfs3 IPv6 shortest form", Spec{Kind: "nfs3", Server: "[2001:DB8:0:0::1]", Export: "/x"}, "nfs3://[2001:db8::1]/x"},
		{"nfs3 Kelvin sign is not folded", Spec{Kind: "nfs3", Server: "K", Export: "/x"}, "nfs3://K/x"},
		{"local", Spec{Kind: "local", Path: "/export/data"}, "local:///export/data"},
		{"local cleaned", Spec{Kind: "local", Path: "/export/data/"}, "local:///export/data"},
		{"unknown kind is empty", Spec{Kind: "x", Server: "A", Export: "/e", Path: "/p/"}, ""},
		{"zero value is empty", Spec{}, ""},
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

func TestNormalizeKeepsDotDot(t *testing.T) {
	t.Parallel()
	s := Spec{Kind: "local", Path: "/a/../b"}
	if got := s.Normalize(); got != s {
		t.Fatalf("Normalize() = %+v, want it unchanged so Validate sees the '..'", got)
	}
	if err := s.Normalize().Validate(); status.Code(err) != codes.InvalidArgument {
		t.Errorf("Validate() = %v, want InvalidArgument", err)
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
		// Unicode case folding must not merge servers.
		{Kind: "nfs3", Server: "Kas.example.com", Export: "/export/data"},
	}
	for _, s := range different {
		if s.ID() == base.ID() {
			t.Errorf("ID of %+v equals the base ID %s", s, base.ID())
		}
	}
	k1 := Spec{Kind: "nfs3", Server: "K", Export: "/x"}
	k2 := Spec{Kind: "nfs3", Server: "k", Export: "/x"}
	if k1.ID() == k2.ID() {
		t.Errorf("Kelvin sign and k share ID %s", k1.ID())
	}
}

func TestMountPoint(t *testing.T) {
	t.Parallel()
	s := Spec{Kind: "local", Path: "/export/data"}
	tests := []struct {
		name    string
		spec    Spec
		root    string
		want    string
		wantErr bool
	}{
		{"valid", s, "/var/lib/miami", "/var/lib/miami/transports/" + s.ID(), false},
		{"root directory", s, "/", "/transports/" + s.ID(), false},
		{"trailing slash", s, "/var/lib/miami/", "", true},
		{"relative", s, "var/lib/miami", "", true},
		{"empty", s, "", "", true},
		{"dot dot", s, "/var/../lib", "", true},
		{"invalid spec", Spec{Kind: "local", Path: "relative"}, "/var/lib/miami", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := tc.spec.MountPoint(tc.root)
			if tc.wantErr {
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("MountPoint() error = %v, want InvalidArgument", err)
				}
				if got != "" {
					t.Errorf("MountPoint() = %q on error, want empty", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("MountPoint() = %v", err)
			}
			if got != tc.want {
				t.Errorf("MountPoint() = %q, want %q", got, tc.want)
			}
		})
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
			name: "nfs3 is normalized",
			params: map[string]string{
				"transport.kind": "nfs3", "transport.server": "NAS.Example.com", "transport.export": "/export//data/",
			},
			want: Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/data"},
		},
		{
			name: "IPv6 is shortened",
			params: map[string]string{
				"transport.kind": "nfs3", "transport.server": "[2001:DB8:0:0::1]", "transport.export": "/x",
			},
			want: Spec{Kind: "nfs3", Server: "[2001:db8::1]", Export: "/x"},
		},
		{
			name:   "local, other keys ignored",
			params: map[string]string{"transport.kind": "local", "transport.path": "/data/", "kind": "directory", "subdir": "x"},
			want:   Spec{Kind: "local", Path: "/data"},
		},
		{name: "no parameters", params: nil, wantErr: true},
		{name: "nfs3 without server", params: map[string]string{"transport.kind": "nfs3", "transport.export": "/x"}, wantErr: true},
		{name: "local with export", params: map[string]string{"transport.kind": "local", "transport.path": "/x", "transport.export": "/y"}, wantErr: true},
		{name: "dot dot is not cleaned away", params: map[string]string{"transport.kind": "local", "transport.path": "/a/../b"}, wantErr: true},
		{name: "root is refused", params: map[string]string{"transport.kind": "local", "transport.path": "//"}, wantErr: true},
		{name: "Kelvin sign is refused", params: map[string]string{"transport.kind": "nfs3", "transport.server": "K", "transport.export": "/x"}, wantErr: true},
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
			if err := got.Validate(); err != nil {
				t.Errorf("ParseSpec result does not validate: %v", err)
			}
		})
	}
}
