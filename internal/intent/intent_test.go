package intent

import (
	"strings"
	"testing"

	"github.com/greghaskins/csi-miami/internal/transport"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// sample returns a valid Intent for the volume name, with no publishes.
func sample(name string) Intent {
	return Intent{
		Version:  Version,
		VolumeID: "directory:" + name,
		Kind:     volume.KindDirectory,
		Transport: transport.Spec{
			Kind:   transport.KindNFS3,
			Server: "192.0.2.10",
			Export: "/export/data",
		},
		Subdir:     "tenant-a",
		StagePath:  "/var/lib/kubelet/plugins/example/stage/" + name,
		AccessMode: "MULTI_NODE_MULTI_WRITER",
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*Intent)
		want   string // substring of the error; empty means valid
	}{
		{"valid", func(*Intent) {}, ""},
		{"valid with publishes", func(in *Intent) {
			in.Publishes = []Publish{{TargetPath: "/pods/a/mount"}, {TargetPath: "/pods/b/mount", ReadOnly: true}}
		}, ""},
		{"valid local transport", func(in *Intent) {
			in.Transport = transport.Spec{Kind: transport.KindLocal, Path: "/srv/data"}
		}, ""},
		{"version zero", func(in *Intent) { in.Version = 0 }, "version 0"},
		{"version two", func(in *Intent) { in.Version = 2 }, "version 2"},
		{"empty volume ID", func(in *Intent) { in.VolumeID = "" }, "volumeID"},
		{"volume ID without kind", func(in *Intent) { in.VolumeID = "pvc-1" }, "volumeID"},
		{"volume ID with unknown kind", func(in *Intent) { in.VolumeID = "image:pvc-1" }, "volumeID"},
		{"volume ID with bad name", func(in *Intent) { in.VolumeID = "directory:a/b" }, "volumeID"},
		{"kind empty", func(in *Intent) { in.Kind = "" }, "does not match"},
		{"kind differs from ID", func(in *Intent) { in.Kind = "image" }, "does not match"},
		{"transport kind missing", func(in *Intent) { in.Transport = transport.Spec{} }, "transport"},
		{"transport nfs3 without server", func(in *Intent) { in.Transport.Server = "" }, "transport.server"},
		{"transport export relative", func(in *Intent) { in.Transport.Export = "export" }, "transport.export"},
		{"subdir empty means no prefix", func(in *Intent) { in.Subdir = "" }, ""},
		{"subdir nested", func(in *Intent) { in.Subdir = "tenant-a/team-b" }, ""},
		{"subdir absolute", func(in *Intent) { in.Subdir = "/abs" }, "subdir"},
		{"subdir traversal", func(in *Intent) { in.Subdir = "../x" }, "subdir"},
		{"subdir not cleaned", func(in *Intent) { in.Subdir = "a//b" }, "subdir"},
		{"subdir trailing slash", func(in *Intent) { in.Subdir = "a/" }, "subdir"},
		{"subdir unresolved token", func(in *Intent) { in.Subdir = "${pvc.name}" }, "subdir"},
		{"subdir shaped like a PV name", func(in *Intent) {
			in.Subdir = "pvc-0a1b2c3d-1111-2222-3333-444455556666"
		}, "subdir"},
		{"stage path empty", func(in *Intent) { in.StagePath = "" }, "stagePath is required"},
		{"stage path relative", func(in *Intent) { in.StagePath = "stage/a" }, "stagePath"},
		{"stage path not clean", func(in *Intent) { in.StagePath = "/stage//a" }, "stagePath"},
		{"stage path with dotdot", func(in *Intent) { in.StagePath = "/stage/../a" }, "stagePath"},
		{"stage path trailing slash", func(in *Intent) { in.StagePath = "/stage/a/" }, "stagePath"},
		{"publish target empty", func(in *Intent) {
			in.Publishes = []Publish{{TargetPath: ""}}
		}, "publishes[0].targetPath is required"},
		{"publish target relative", func(in *Intent) {
			in.Publishes = []Publish{{TargetPath: "pods/a"}}
		}, "publishes[0].targetPath"},
		{"publish target not clean", func(in *Intent) {
			in.Publishes = []Publish{{TargetPath: "/pods/a"}, {TargetPath: "/pods/./b"}}
		}, "publishes[1].targetPath"},
		{"publish targets duplicated", func(in *Intent) {
			in.Publishes = []Publish{{TargetPath: "/pods/a"}, {TargetPath: "/pods/b"}, {TargetPath: "/pods/a", ReadOnly: true}}
		}, "listed twice"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			in := sample("pvc-1")
			tc.mutate(&in)
			err := in.Validate()
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestEscape(t *testing.T) {
	t.Parallel()
	tests := []struct{ id, want string }{
		{"directory:pvc-123", "directory%3Apvc-123"},
		{"plain.name_1-2", "plain.name_1-2"},
		{"a/b", "a%2Fb"},
		{"100%", "100%25"},
		{"sp ace", "sp%20ace"},
		{"\xff\x00", "%FF%00"},
		{"é", "%C3%A9"},
		{"", ""},
	}
	for _, tc := range tests {
		t.Run(tc.id, func(t *testing.T) {
			t.Parallel()
			if got := Escape(tc.id); got != tc.want {
				t.Fatalf("Escape(%q) = %q, want %q", tc.id, got, tc.want)
			}
			back, err := Unescape(tc.want)
			if err != nil {
				t.Fatalf("Unescape(%q): %v", tc.want, err)
			}
			if back != tc.id {
				t.Fatalf("Unescape(%q) = %q, want %q", tc.want, back, tc.id)
			}
		})
	}
}

func TestUnescapeRejects(t *testing.T) {
	t.Parallel()
	for _, in := range []string{"%", "%4", "%zz", "%3a", "a%", "a:b", "a/b", "%4G"} {
		t.Run(in, func(t *testing.T) {
			t.Parallel()
			if got, err := Unescape(in); err == nil {
				t.Fatalf("Unescape(%q) = %q, want an error", in, got)
			}
		})
	}
}
