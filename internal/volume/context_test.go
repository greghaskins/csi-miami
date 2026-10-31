package volume

import (
	"maps"
	"strings"
	"testing"

	"github.com/greghaskins/csi-miami/internal/transport"
)

func validSpec() transport.Spec {
	return transport.Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/data"}
}

func TestContextEncode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		c    Context
		want map[string]string
	}{
		{
			name: "nfs3",
			c:    Context{Kind: KindDirectory, Transport: validSpec(), Subdir: "team-a/data"},
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
				Subdir:    "team-a",
			},
			want: map[string]string{
				"miami/kind":           "directory",
				"miami/transport.kind": "local",
				"miami/transport.path": "/export/data",
				"miami/subdir":         "team-a",
			},
		},
		{
			name: "no prefix omits the subdir",
			c:    Context{Kind: KindDirectory, Transport: validSpec()},
			want: map[string]string{
				"miami/kind":             "directory",
				"miami/transport.kind":   "nfs3",
				"miami/transport.server": "nas.example.com",
				"miami/transport.export": "/export/data",
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
		{Kind: KindDirectory, Transport: validSpec(), Subdir: "team-a/data"},
		{Kind: KindDirectory, Transport: validSpec()},
		{Kind: KindDirectory, Transport: transport.Spec{Kind: "nfs3", Server: "192.0.2.10", Export: "/export/data"}, Subdir: "x"},
		{Kind: KindDirectory, Transport: transport.Spec{Kind: "local", Path: "/export/data"}, Subdir: "x"},
	}
	for _, c := range tests {
		t.Run(c.Transport.Canonical()+"|"+c.Subdir, func(t *testing.T) {
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

// nfsContext returns a valid encoded context with overrides applied.
func nfsContext(overrides map[string]string) map[string]string {
	m := map[string]string{
		"miami/kind":             "directory",
		"miami/transport.kind":   "nfs3",
		"miami/transport.server": "nas.example.com",
		"miami/transport.export": "/export/data",
		"miami/subdir":           "team-a/data",
	}
	for k, v := range overrides {
		if v == del {
			delete(m, k)
		} else {
			m[k] = v
		}
	}
	return m
}

func TestDecodeContext(t *testing.T) {
	t.Parallel()
	nfs := Context{Kind: KindDirectory, Transport: validSpec(), Subdir: "team-a/data"}
	tests := []struct {
		name    string
		in      map[string]string
		want    Context
		wantErr string
	}{
		{name: "valid", in: nfsContext(nil), want: nfs},
		{
			name: "subdir may be absent",
			in:   nfsContext(map[string]string{"miami/subdir": del}),
			want: Context{Kind: KindDirectory, Transport: validSpec()},
		},
		{
			name: "subdir may be empty",
			in:   nfsContext(map[string]string{"miami/subdir": ""}),
			want: Context{Kind: KindDirectory, Transport: validSpec()},
		},
		{
			name: "keys without the miami/ prefix are ignored",
			in: nfsContext(map[string]string{
				"csi.storage.k8s.io/pod.name": "p", "storage.kubernetes.io/csiProvisionerIdentity": "i", "miami": "x", "Miami/other": "y",
			}),
			want: nfs,
		},

		{name: "nil map", in: nil, wantErr: "miami/kind is required"},
		{name: "missing kind", in: nfsContext(map[string]string{"miami/kind": del}), wantErr: "miami/kind is required"},
		{name: "empty kind", in: nfsContext(map[string]string{"miami/kind": ""}), wantErr: "miami/kind is required"},
		{name: "unknown kind", in: nfsContext(map[string]string{"miami/kind": "image"}), wantErr: "miami/kind must be directory"},
		{name: "missing transport kind", in: nfsContext(map[string]string{"miami/transport.kind": del}), wantErr: "miami/transport.kind is required"},
		{name: "unknown transport kind", in: nfsContext(map[string]string{"miami/transport.kind": "smb"}), wantErr: "transport.kind must be"},
		{name: "nfs3 missing server", in: nfsContext(map[string]string{"miami/transport.server": del}), wantErr: "transport.server is required"},
		{name: "nfs3 missing export", in: nfsContext(map[string]string{"miami/transport.export": del}), wantErr: "transport.export is required"},
		{name: "nfs3 relative export", in: nfsContext(map[string]string{"miami/transport.export": "export"}), wantErr: "absolute"},
		{name: "nfs3 with a path", in: nfsContext(map[string]string{"miami/transport.path": "/x"}), wantErr: "transport.path must be empty"},
		{
			name:    "local with a server",
			in:      map[string]string{"miami/kind": "directory", "miami/transport.kind": "local", "miami/transport.path": "/x", "miami/transport.server": "nas.example.com", "miami/subdir": "a"},
			wantErr: "transport.server must be empty",
		},

		// The node is never more lenient than the controller: a value that
		// is not exactly what NewContext produces is refused, not repaired.
		{name: "uncleaned export", in: nfsContext(map[string]string{"miami/transport.export": "/export/data/"}), wantErr: "cleaned"},
		{name: "upper-case server", in: nfsContext(map[string]string{"miami/transport.server": "NAS.example.com"}), wantErr: "lower case"},
		{name: "IPv6 literal", in: nfsContext(map[string]string{"miami/transport.server": "[2001:db8::1]"}), wantErr: "IPv6 is not supported"},
		{name: "trailing slash in subdir", in: nfsContext(map[string]string{"miami/subdir": "a/"}), wantErr: "cleaned"},
		{name: "doubled slash in subdir", in: nfsContext(map[string]string{"miami/subdir": "a//b"}), wantErr: "cleaned"},
		{name: "dot element in subdir", in: nfsContext(map[string]string{"miami/subdir": "./a"}), wantErr: "cleaned"},
		{name: "subdir is dot", in: nfsContext(map[string]string{"miami/subdir": "."}), wantErr: "cleaned"},
		{name: "literal token in subdir", in: nfsContext(map[string]string{"miami/subdir": "${pvc.name}"}), wantErr: "unresolved token"},
		{name: "PV-shaped subdir element", in: nfsContext(map[string]string{"miami/subdir": "a/" + pvUUID1}), wantErr: "PV name"},

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

// TestDecodeRejectsWhatNewContextRejects feeds each rejected StorageClass
// value back through DecodeContext as a stored value: whatever the
// controller refuses to create, the node must refuse to stage.
func TestDecodeRejectsWhatNewContextRejects(t *testing.T) {
	t.Parallel()
	elem256 := strings.Repeat("a", 256)
	rejections := []struct{ key, value string }{
		{"kind", "image"},
		{"transport.kind", "smb"},
		{"transport.server", "nas example"},
		{"transport.server", "nas,vers=4"},
		{"transport.server", "nas/x"},
		{"transport.server", "nas.example.com:2049"},
		{"transport.server", "K"},
		{"transport.server", "nas\nx"},
		{"transport.server", "[nas]"},
		{"transport.server", "192.0.2.256"},
		{"transport.export", "export"},
		{"transport.export", "/a/../b"},
		{"transport.export", "/"},
		{"transport.export", "/a\nb"},
		{"transport.export", "/a\x00b"},
		{"transport.export", "/" + elem256},
		{"subdir", "../x"},
		{"subdir", "a/../b"},
		{"subdir", "/abs"},
		{"subdir", "a\x00b"},
		{"subdir", "a\nb"},
		{"subdir", "a\x7fb"},
		{"subdir", pvUUID1},
		{"subdir", "x/" + pvUUID2 + "/y"},
		{"subdir", elem256},
		{"subdir", strings.Repeat(strings.Repeat("a", 255)+"/", 16)},
		{"subdir", "${pv.name}"},
		{"subdir", "a/${pvc.name"},
	}
	for _, r := range rejections {
		t.Run(r.key+"="+r.value[:min(len(r.value), 20)], func(t *testing.T) {
			t.Parallel()
			if _, err := NewContext(nfsParams(map[string]string{r.key: r.value})); err == nil {
				// ${pvc.name}-style values that resolve are not rejections;
				// none of the table's values does.
				t.Fatalf("NewContext accepted %s=%q; the table is wrong", r.key, r.value)
			}
			ctxKey := "miami/" + r.key
			_, err := DecodeContext(nfsContext(map[string]string{ctxKey: r.value}))
			wantInvalid(t, err, "")
		})
	}
}

func TestContextValidate(t *testing.T) {
	t.Parallel()
	good := Context{Kind: KindDirectory, Transport: transport.Spec{Kind: "local", Path: "/export/data"}, Subdir: "a/b"}
	if err := good.Validate(); err != nil {
		t.Fatalf("Validate() = %v", err)
	}
	noPrefix := good
	noPrefix.Subdir = ""
	if err := noPrefix.Validate(); err != nil {
		t.Fatalf("Validate() without a prefix = %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(*Context)
		wantErr string
	}{
		{"no kind", func(c *Context) { c.Kind = "" }, "miami/kind"},
		{"bad transport", func(c *Context) { c.Transport.Path = "" }, "transport.path is required"},
		{"subdir not cleaned", func(c *Context) { c.Subdir = "a/" }, "cleaned"},
		{"subdir absolute", func(c *Context) { c.Subdir = "/a" }, "relative"},
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

func TestMatchesID(t *testing.T) {
	t.Parallel()
	c := Context{Kind: KindDirectory, Transport: validSpec()}
	if err := c.MatchesID(ID{Kind: KindDirectory, Name: PV}); err != nil {
		t.Fatalf("MatchesID() = %v", err)
	}
	wantInvalid(t, c.MatchesID(ID{Kind: KindDirectory, Name: ""}), "must not be empty")
	wantInvalid(t, c.MatchesID(ID{Kind: "image", Name: PV}), "kind")

	// A kind both sides know about but that differs.
	other := Context{Kind: "image", Transport: validSpec()}
	wantInvalid(t, other.MatchesID(ID{Kind: KindDirectory, Name: PV}), "does not match")
}

func TestDir(t *testing.T) {
	t.Parallel()
	const root = "/var/lib/miami"
	mount, err := validSpec().MountPoint(root)
	if err != nil {
		t.Fatal(err)
	}
	id1 := ID{Kind: KindDirectory, Name: pvUUID1}
	id2 := ID{Kind: KindDirectory, Name: pvUUID2}

	tests := []struct {
		name string
		c    Context
		id   ID
		want string
	}{
		{"prefix", Context{Kind: KindDirectory, Transport: validSpec(), Subdir: "team-a/data"}, id1, mount + "/team-a/data/" + pvUUID1},
		{"no prefix", Context{Kind: KindDirectory, Transport: validSpec()}, id1, mount + "/" + pvUUID1},
		{"other volume, same prefix", Context{Kind: KindDirectory, Transport: validSpec(), Subdir: "team-a/data"}, id2, mount + "/team-a/data/" + pvUUID2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := tc.c.Dir(root, tc.id)
			if err != nil {
				t.Fatalf("Dir() = %v", err)
			}
			if got != tc.want {
				t.Errorf("Dir() = %q, want %q", got, tc.want)
			}
		})
	}

	errTests := []struct {
		name    string
		c       Context
		root    string
		id      ID
		wantErr string
	}{
		{"invalid context", Context{Kind: KindDirectory, Transport: validSpec(), Subdir: "../x"}, root, id1, "'..'"},
		{"invalid ID", Context{Kind: KindDirectory, Transport: validSpec()}, root, ID{Kind: KindDirectory, Name: "a/b"}, shapeMsg},
		{"kind mismatch", Context{Kind: "image", Transport: validSpec()}, root, id1, "miami/kind"},
		{"relative root", Context{Kind: KindDirectory, Transport: validSpec()}, "var/lib", id1, "transports root"},
		{"uncleaned root", Context{Kind: KindDirectory, Transport: validSpec()}, root + "/", id1, "transports root"},
	}
	for _, tc := range errTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := tc.c.Dir(tc.root, tc.id)
			wantInvalid(t, err, tc.wantErr)
			if got != "" {
				t.Errorf("Dir() = %q on error, want empty", got)
			}
		})
	}
}

// TestDirIsUniquePerVolume covers the aliasing the volume name closes: no
// subdir template can make two volumes share a directory.
func TestDirIsUniquePerVolume(t *testing.T) {
	t.Parallel()
	const root = "/var/lib/miami"
	ctxFor := func(t *testing.T, subdir, ns, claim string) Context {
		t.Helper()
		c, err := NewContext(nfsParams(map[string]string{
			"subdir":                           subdir,
			"csi.storage.k8s.io/pvc/namespace": ns,
			"csi.storage.k8s.io/pvc/name":      claim,
		}))
		if err != nil {
			t.Fatalf("NewContext() = %v", err)
		}
		return c
	}
	dir := func(t *testing.T, c Context, name string) string {
		t.Helper()
		d, err := c.Dir(root, ID{Kind: KindDirectory, Name: name})
		if err != nil {
			t.Fatalf("Dir() = %v", err)
		}
		return d
	}

	t.Run("flattened tokens", func(t *testing.T) {
		t.Parallel()
		a := ctxFor(t, "${pvc.namespace}-${pvc.name}", "a-b", "c")
		b := ctxFor(t, "${pvc.namespace}-${pvc.name}", "a", "b-c")
		if a.Subdir != b.Subdir {
			t.Fatalf("prefixes differ (%q, %q); the case no longer collides", a.Subdir, b.Subdir)
		}
		if da, db := dir(t, a, pvUUID1), dir(t, b, pvUUID2); da == db {
			t.Errorf("two volumes share directory %q", da)
		}
	})

	t.Run("fixed prefix shared by every volume of a class", func(t *testing.T) {
		t.Parallel()
		c := ctxFor(t, "shared", "ns", "claim")
		if d1, d2 := dir(t, c, pvUUID1), dir(t, c, pvUUID2); d1 == d2 {
			t.Errorf("two volumes share directory %q", d1)
		}
	})

	t.Run("claim recreated under the same name", func(t *testing.T) {
		t.Parallel()
		c := ctxFor(t, "${pvc.namespace}/${pvc.name}", "team-a", "data")
		old, recreated := dir(t, c, pvUUID1), dir(t, c, pvUUID2)
		if old == recreated {
			t.Fatalf("old and recreated volume share directory %q: a late delete of the old one would remove the new one", old)
		}
		if strings.HasPrefix(recreated, old+"/") || strings.HasPrefix(old, recreated+"/") {
			t.Errorf("directories %q and %q nest", old, recreated)
		}
	})

	t.Run("no prefix", func(t *testing.T) {
		t.Parallel()
		c := ctxFor(t, "", "ns", "claim")
		if d1, d2 := dir(t, c, pvUUID1), dir(t, c, pvUUID2); d1 == d2 {
			t.Errorf("two volumes share directory %q", d1)
		}
	})
}
