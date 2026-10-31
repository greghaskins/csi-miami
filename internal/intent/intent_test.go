package intent

import (
	"reflect"
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
	long := "/" + strings.Repeat("a", maxPathLen)
	justUnder := "/" + strings.Repeat("a", maxPathLen-2)
	tests := []struct {
		name   string
		mutate func(*Intent)
		want   string // substring of the error; empty means valid
	}{
		{"valid", func(*Intent) {}, ""},
		{"valid with publishes", func(in *Intent) {
			in.Publishes = []Publish{{TargetPath: "/pods/a/mount"}, {TargetPath: "/pods/b/mount", ReadOnly: true}}
		}, ""},
		{"valid sibling targets", func(in *Intent) {
			in.Publishes = []Publish{{TargetPath: "/pods/a"}, {TargetPath: "/pods/ab"}}
		}, ""},
		{"valid local transport", func(in *Intent) {
			in.Transport = transport.Spec{Kind: transport.KindLocal, Path: "/srv/data"}
		}, ""},
		{"valid empty subdir", func(in *Intent) { in.Subdir = "" }, ""},
		{"valid nested subdir", func(in *Intent) { in.Subdir = "tenant-a/team-b" }, ""},
		{"valid path just under the limit", func(in *Intent) { in.StagePath = justUnder }, ""},
		{"valid every access mode", func(in *Intent) { in.AccessMode = "SINGLE_NODE_MULTI_WRITER" }, ""},
		{"valid mount flags", func(in *Intent) { in.MountFlags = []string{"ro", "noatime", "nfsvers=3"} }, ""},

		{"version zero", func(in *Intent) { in.Version = 0 }, "version 0"},
		{"version two", func(in *Intent) { in.Version = 2 }, "version 2"},

		{"volume ID empty", func(in *Intent) { in.VolumeID = "" }, "volumeID"},
		{"volume ID without colon", func(in *Intent) { in.VolumeID = "pvc-1" }, "volumeID"},
		{"volume ID without kind", func(in *Intent) { in.VolumeID = ":pvc-1" }, "volumeID"},
		{"volume ID without name", func(in *Intent) { in.VolumeID = "directory:" }, "volumeID"},
		{"volume ID with control character", func(in *Intent) { in.VolumeID = "directory:a\nb" }, "volumeID"},
		{"volume ID too long", func(in *Intent) { in.VolumeID = "directory:" + strings.Repeat("a", 300) }, "too long"},
		{"kind unknown", func(in *Intent) {
			in.VolumeID = "image:pvc-1"
			in.Kind = "image"
		}, "known kind"},
		{"kind empty", func(in *Intent) { in.Kind = "" }, "known kind"},
		{"kind differs from ID", func(in *Intent) { in.VolumeID = "image:pvc-1" }, "does not match"},

		{"transport kind missing", func(in *Intent) { in.Transport = transport.Spec{} }, "transport.kind"},
		{"transport kind unknown", func(in *Intent) { in.Transport.Kind = "smb" }, "transport.kind"},
		{"nfs3 without server", func(in *Intent) { in.Transport.Server = "" }, "transport.server is required"},
		{"nfs3 without export", func(in *Intent) { in.Transport.Export = "" }, "transport.export is required"},
		{"nfs3 with path", func(in *Intent) { in.Transport.Path = "/x" }, "transport.path must be empty"},
		{"nfs3 export relative", func(in *Intent) { in.Transport.Export = "export" }, "transport.export must be an absolute"},
		{"nfs3 export root", func(in *Intent) { in.Transport.Export = "/" }, "transport.export must not be the root"},
		{"nfs3 export not clean", func(in *Intent) { in.Transport.Export = "/export//data" }, "transport.export must be a cleaned"},
		{"nfs3 export dotdot", func(in *Intent) { in.Transport.Export = "/export/../data" }, "transport.export must not contain '..'"},
		{"nfs3 server with control character", func(in *Intent) { in.Transport.Server = "a\x00b" }, "transport.server contains a control"},
		{"local without path", func(in *Intent) { in.Transport = transport.Spec{Kind: transport.KindLocal} }, "transport.path is required"},
		{"local with server", func(in *Intent) {
			in.Transport = transport.Spec{Kind: transport.KindLocal, Path: "/srv", Server: "h"}
		}, "transport.server must be empty"},
		{"local with export", func(in *Intent) {
			in.Transport = transport.Spec{Kind: transport.KindLocal, Path: "/srv", Export: "/e"}
		}, "transport.export must be empty"},
		{"local path with NUL", func(in *Intent) {
			in.Transport = transport.Spec{Kind: transport.KindLocal, Path: "/srv/\x00"}
		}, "transport.path contains a control"},

		{"subdir absolute", func(in *Intent) { in.Subdir = "/abs" }, "subdir must be a relative"},
		{"subdir dotdot", func(in *Intent) { in.Subdir = "a/../b" }, "subdir must not contain '..'"},
		{"subdir leading dotdot", func(in *Intent) { in.Subdir = "../x" }, "subdir must not contain '..'"},
		{"subdir not cleaned", func(in *Intent) { in.Subdir = "a//b" }, "subdir must be a cleaned"},
		{"subdir trailing slash", func(in *Intent) { in.Subdir = "a/" }, "subdir must be a cleaned"},
		{"subdir dot", func(in *Intent) { in.Subdir = "." }, "subdir must be a cleaned"},
		{"subdir with NUL", func(in *Intent) { in.Subdir = "a\x00b" }, "subdir contains a control"},
		{"subdir with newline", func(in *Intent) { in.Subdir = "a\nb" }, "subdir contains a control"},
		{"subdir too long", func(in *Intent) { in.Subdir = strings.Repeat("a", maxPathLen) }, "subdir is too long"},

		{"stage path empty", func(in *Intent) { in.StagePath = "" }, "stagePath is required"},
		{"stage path relative", func(in *Intent) { in.StagePath = "stage/a" }, "stagePath must be an absolute"},
		{"stage path root", func(in *Intent) { in.StagePath = "/" }, "stagePath must not be the root"},
		{"stage path not clean", func(in *Intent) { in.StagePath = "/stage//a" }, "stagePath must be a cleaned"},
		{"stage path with dotdot", func(in *Intent) { in.StagePath = "/stage/../a" }, "stagePath must not contain '..'"},
		{"stage path trailing slash", func(in *Intent) { in.StagePath = "/stage/a/" }, "stagePath must be a cleaned"},
		{"stage path with NUL", func(in *Intent) { in.StagePath = "/stage/\x00" }, "stagePath contains a control"},
		{"stage path with tab", func(in *Intent) { in.StagePath = "/stage/\ta" }, "stagePath contains a control"},
		{"stage path too long", func(in *Intent) { in.StagePath = long }, "stagePath is too long"},

		{"publish target empty", func(in *Intent) {
			in.Publishes = []Publish{{TargetPath: ""}}
		}, "publishes[0].targetPath is required"},
		{"publish target relative", func(in *Intent) {
			in.Publishes = []Publish{{TargetPath: "pods/a"}}
		}, "publishes[0].targetPath must be an absolute"},
		{"publish target root", func(in *Intent) {
			in.Publishes = []Publish{{TargetPath: "/"}}
		}, "publishes[0].targetPath must not be the root"},
		{"publish target not clean", func(in *Intent) {
			in.Publishes = []Publish{{TargetPath: "/pods/a"}, {TargetPath: "/pods/./b"}}
		}, "publishes[1].targetPath must be a cleaned"},
		{"publish target with dotdot", func(in *Intent) {
			in.Publishes = []Publish{{TargetPath: "/pods/../b"}}
		}, "publishes[0].targetPath must not contain '..'"},
		{"publish target with control character", func(in *Intent) {
			in.Publishes = []Publish{{TargetPath: "/pods/a\n"}}
		}, "publishes[0].targetPath contains a control"},
		{"publish target too long", func(in *Intent) {
			in.Publishes = []Publish{{TargetPath: long}}
		}, "publishes[0].targetPath is too long"},
		{"publish targets duplicated", func(in *Intent) {
			in.Publishes = []Publish{{TargetPath: "/pods/a"}, {TargetPath: "/pods/b"}, {TargetPath: "/pods/a", ReadOnly: true}}
		}, "publishes[2].targetPath repeats publishes[0]"},
		{"publish target is the stage path", func(in *Intent) {
			in.Publishes = []Publish{{TargetPath: in.StagePath}}
		}, "must not be the stage path or inside it"},
		{"publish target inside the stage path", func(in *Intent) {
			in.Publishes = []Publish{{TargetPath: in.StagePath + "/sub"}}
		}, "must not be the stage path or inside it"},
		{"publish target inside an earlier target", func(in *Intent) {
			in.Publishes = []Publish{{TargetPath: "/pods/a"}, {TargetPath: "/pods/a/b"}}
		}, "publishes[1].targetPath is inside publishes[0]"},
		{"publish target contains a later target", func(in *Intent) {
			in.Publishes = []Publish{{TargetPath: "/pods/a/b"}, {TargetPath: "/pods/a"}}
		}, "publishes[0].targetPath is inside publishes[1]"},

		{"access mode empty", func(in *Intent) { in.AccessMode = "" }, "accessMode"},
		{"access mode unknown", func(in *Intent) { in.AccessMode = "READ_WRITE_MANY" }, "accessMode"},
		{"access mode lower case", func(in *Intent) { in.AccessMode = "multi_node_multi_writer" }, "accessMode"},
		{"access mode UNKNOWN is not a mode", func(in *Intent) { in.AccessMode = "UNKNOWN" }, "accessMode"},

		{"mount flag empty", func(in *Intent) { in.MountFlags = []string{"ro", ""} }, "mountFlags[1] is empty"},
		{"mount flag with comma", func(in *Intent) { in.MountFlags = []string{"ro,rw"} }, "mountFlags[0] contains a comma"},
		{"mount flag with NUL", func(in *Intent) { in.MountFlags = []string{"a\x00"} }, "mountFlags[0] contains a control"},
		{"mount flag with newline", func(in *Intent) { in.MountFlags = []string{"a\nb"} }, "mountFlags[0] contains a control"},
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

func TestAllAccessModesAreAccepted(t *testing.T) {
	t.Parallel()
	for _, m := range []string{
		"SINGLE_NODE_WRITER", "SINGLE_NODE_READER_ONLY", "MULTI_NODE_READER_ONLY",
		"MULTI_NODE_SINGLE_WRITER", "MULTI_NODE_MULTI_WRITER",
		"SINGLE_NODE_SINGLE_WRITER", "SINGLE_NODE_MULTI_WRITER",
	} {
		in := sample("pvc-1")
		in.AccessMode = m
		if err := in.Validate(); err != nil {
			t.Errorf("access mode %s: %v", m, err)
		}
	}
}

func TestIDAndContext(t *testing.T) {
	t.Parallel()
	in := sample("pvc-1")
	id, err := in.ID()
	if err != nil {
		t.Fatal(err)
	}
	if want := (volume.ID{Kind: volume.KindDirectory, Name: "pvc-1"}); id != want {
		t.Errorf("ID() = %+v, want %+v", id, want)
	}
	if id.String() != in.VolumeID {
		t.Errorf("ID().String() = %q, want %q", id.String(), in.VolumeID)
	}
	wantCtx := volume.Context{Kind: volume.KindDirectory, Transport: in.Transport, Subdir: "tenant-a"}
	if got := in.Context(); !reflect.DeepEqual(got, wantCtx) {
		t.Errorf("Context() = %+v, want %+v", got, wantCtx)
	}

	// ID does not apply the boundary rules of volume.ParseID.
	in.VolumeID = "directory:odd name~"
	if id, err := in.ID(); err != nil || id.Name != "odd name~" {
		t.Errorf("ID() = %+v, %v; want the name back without boundary checks", id, err)
	}
	in.VolumeID = "nocolon"
	if _, err := in.ID(); err == nil {
		t.Error("ID() of a malformed volume ID returned no error")
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
