package observe

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func loadFixture(t *testing.T) Table {
	t.Helper()
	f, err := os.Open("testdata/mountinfo.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	ms, err := Parse(f)
	if err != nil {
		t.Fatalf("Parse(fixture): %v", err)
	}
	return Table(ms)
}

func TestParseFixture(t *testing.T) {
	t.Parallel()
	tbl := loadFixture(t)
	if got, want := len(tbl), 14; got != want {
		t.Fatalf("parsed %d mounts, want %d", got, want)
	}

	byID := map[int]Mount{}
	for _, m := range tbl {
		byID[m.ID] = m
	}
	nfsSuper := []string{"rw", "vers=3", "addr=198.51.100.10"}

	tests := []struct {
		name string
		id   int
		want Mount
	}{
		{
			name: "plain nfs mount",
			id:   31,
			want: Mount{
				ID: 31, ParentID: 20, Major: 0, Minor: 40,
				Root: "/vols/pvc-1", MountPoint: "/mnt/stage/pvc-1",
				Options: []string{"rw", "noatime"}, Optional: []string{"shared:5"},
				FSType: "nfs", Source: "198.51.100.10:/export/data", SuperOptions: nfsSuper,
			},
		},
		{
			name: "spaces escaped as 040",
			id:   35,
			want: Mount{
				ID: 35, ParentID: 20, Major: 0, Minor: 40,
				Root: "/vols/my volume", MountPoint: "/mnt/publish/my pod",
				Options: []string{"rw", "noatime"}, Optional: []string{"shared:5"},
				FSType: "nfs", Source: "198.51.100.10:/export/data", SuperOptions: nfsSuper,
			},
		},
		{
			name: "several optional fields",
			id:   38,
			want: Mount{
				ID: 38, ParentID: 20, Major: 8, Minor: 2,
				Root: "/", MountPoint: "/mnt/extra",
				Options:  []string{"rw", "relatime"},
				Optional: []string{"shared:7", "master:3", "propagate_from:2", "unbindable"},
				FSType:   "ext4", Source: "/dev/sda2", SuperOptions: []string{"rw"},
			},
		},
		{
			name: "no optional fields",
			id:   36,
			want: Mount{
				ID: 36, ParentID: 20, Major: 0, Minor: 52,
				Root: "/", MountPoint: "/mnt/scratch",
				Options: []string{"rw", "relatime"},
				FSType:  "tmpfs", Source: "tmpfs", SuperOptions: []string{"rw", "size=1024k"},
			},
		},
		{
			name: "source with space, backslash and tab",
			id:   39,
			want: Mount{
				ID: 39, ParentID: 20, Major: 0, Minor: 54,
				Root: "/", MountPoint: "/srv/odd",
				Options: []string{"rw", "relatime"},
				FSType:  "fuse.example", Source: "/srv/with space\\back\ttab",
				SuperOptions: []string{"rw", "user_id=0", "group_id=0"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := byID[tt.id]
			if !ok {
				t.Fatalf("no mount with ID %d", tt.id)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mount %d:\n got %+v\nwant %+v", tt.id, got, tt.want)
			}
		})
	}
}

func TestParseMalformed(t *testing.T) {
	t.Parallel()
	const good = "20 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw"
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{"too few fields", good + "\n20 1 8:1 / /x rw\n", "line 2: want at least 10 fields"},
		{"no separator", "20 1 8:1 / / rw,relatime shared:1 shared:2 ext4 /dev/sda1 rw\n", "line 1: no \"-\" separator"},
		{"nothing after separator", "20 1 8:1 / / rw,relatime shared:1 shared:2 shared:3 shared:4 -\n", "line 1: want filesystem type, source and super options"},
		{"too few fields after separator", "20 1 8:1 / / rw shared:1 shared:2 shared:3 - ext4 /dev/sda1\n", "line 1: want filesystem type, source and super options"},
		{"bad mount ID", "x 1 8:1 / / rw - ext4 /dev/sda1 rw\n", "line 1: mount ID"},
		{"negative parent ID", "20 -1 8:1 / / rw - ext4 /dev/sda1 rw\n", "line 1: parent ID"},
		{"device without colon", "20 1 81 / / rw - ext4 /dev/sda1 rw\n", "line 1: device \"81\" is not major:minor"},
		{"bad major", "20 1 a:1 / / rw - ext4 /dev/sda1 rw\n", "line 1: device major"},
		{"bad minor", "20 1 8:b / / rw - ext4 /dev/sda1 rw\n", "line 1: device minor"},
		{"truncated escape in root", "20 1 8:1 /a\\04 / rw - ext4 /dev/sda1 rw\n", "line 1: root: truncated escape"},
		{"non-octal escape in mount point", "20 1 8:1 / /a\\09b rw - ext4 /dev/sda1 rw\n", "line 1: mount point: bad escape"},
		{"escape out of range in source", "20 1 8:1 / / rw - ext4 /dev/a\\777 rw\n", "line 1: source: escape"},
		{"bad escape in fstype", "20 1 8:1 / / rw - ex\\zzzt4 /dev/sda1 rw\n", "line 1: filesystem type: bad escape"},
		{"line past the limit", "20 1 8:1 / / rw - ext4 /dev/sda1 " + strings.Repeat("a", maxLine) + "\n", "reading mountinfo after line 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ms, err := Parse(strings.NewReader(tt.input))
			if err == nil {
				t.Fatalf("Parse succeeded with %d mounts, want error containing %q", len(ms), tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q does not contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestParseEmptyAndBlank(t *testing.T) {
	t.Parallel()
	for _, in := range []string{"", "\n", "\n  \n"} {
		ms, err := Parse(strings.NewReader(in))
		if err != nil || len(ms) != 0 {
			t.Errorf("Parse(%q) = %v, %v; want no mounts and no error", in, ms, err)
		}
	}
}

func TestReadOnly(t *testing.T) {
	t.Parallel()
	tests := []struct {
		opts []string
		want bool
	}{
		{[]string{"rw", "noatime"}, false},
		{[]string{"ro", "noatime"}, true},
		{[]string{"rw", "ro"}, true},
		{[]string{"rw", "rootcontext=ro"}, false},
		{nil, false},
	}
	for _, tt := range tests {
		if got := (Mount{Options: tt.opts}).ReadOnly(); got != tt.want {
			t.Errorf("ReadOnly(%v) = %v, want %v", tt.opts, got, tt.want)
		}
	}
}

func TestReadMountinfo(t *testing.T) {
	t.Parallel()
	ms, err := ReadMountinfo()
	if err != nil {
		t.Fatalf("ReadMountinfo: %v", err)
	}
	if _, ok := Table(ms).At("/"); !ok {
		t.Errorf("the live table has no mount at /")
	}
}
