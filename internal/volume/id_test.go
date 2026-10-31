package volume

import (
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// wantInvalid fails the test unless err is InvalidArgument and its message
// contains substr.
func wantInvalid(t *testing.T, err error, substr string) {
	t.Helper()
	if err == nil {
		t.Fatalf("got nil error, want InvalidArgument containing %q", substr)
	}
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument (error: %v)", got, err)
	}
	if !strings.Contains(err.Error(), substr) {
		t.Errorf("error %q does not contain %q", err.Error(), substr)
	}
}

const shapeMsg = "provisioner name"

const PV = "pvc-0a1b2c3d-1111-2222-3333-444455556666"

func TestParseID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want ID
		// wantErr is a substring of the error message; empty means valid.
		wantErr string
	}{
		{name: "pvc name", in: "directory:" + PV, want: ID{KindDirectory, PV}},
		{name: "every hex digit", in: "directory:pvc-01234567-89ab-cdef-0123-456789abcdef", want: ID{KindDirectory, "pvc-01234567-89ab-cdef-0123-456789abcdef"}},

		{name: "empty", in: "", wantErr: "<kind>:<name>"},
		{name: "no separator", in: "directory", wantErr: "<kind>:<name>"},
		{name: "empty kind", in: ":" + PV, wantErr: "kind"},
		{name: "unknown kind", in: "image:" + PV, wantErr: "kind"},
		{name: "kind is case sensitive", in: "Directory:" + PV, wantErr: "kind"},
		{name: "empty name", in: "directory:", wantErr: "must not be empty"},

		// Only the provisioner's shape is a volume name, so that no name can be
		// one a subdir element may have (the rule in ResolveSubdir).
		{name: "a subdir-like name", in: "directory:team-a", wantErr: shapeMsg},
		{name: "a short pvc name", in: "directory:pvc-1", wantErr: shapeMsg},
		{name: "dot name", in: "directory:.", wantErr: shapeMsg},
		{name: "dot dot name", in: "directory:..", wantErr: shapeMsg},
		{name: "every formerly allowed character", in: "directory:aZ09._-", wantErr: shapeMsg},
		{name: "long name", in: "directory:" + strings.Repeat("a", 118), wantErr: shapeMsg},
		{name: "second colon", in: "directory:" + PV + ":1", wantErr: shapeMsg},
		{name: "slash in name", in: "directory:pvc/1", wantErr: shapeMsg},
		{name: "space in name", in: "directory:" + PV + " ", wantErr: shapeMsg},
		{name: "NUL in name", in: "directory:" + PV + "\x00", wantErr: shapeMsg},
		{name: "non-ASCII name", in: "directory:pvc-é", wantErr: shapeMsg},
		{name: "upper-case hex", in: "directory:pvc-0A1B2C3D-1111-2222-3333-444455556666", wantErr: shapeMsg},
		{name: "non-hex digit", in: "directory:pvc-0a1b2c3g-1111-2222-3333-444455556666", wantErr: shapeMsg},
		{name: "one digit short", in: "directory:pvc-0a1b2c3d-1111-2222-3333-44445555666", wantErr: shapeMsg},
		{name: "one digit long", in: "directory:pvc-0a1b2c3d-1111-2222-3333-4444555566666", wantErr: shapeMsg},
		{name: "another prefix", in: "directory:pv-0a1b2c3d-1111-2222-3333-444455556666", wantErr: shapeMsg},
		{name: "a trailing suffix", in: "directory:" + PV + ".bak", wantErr: shapeMsg},
		{name: "a leading prefix", in: "directory:x" + PV, wantErr: shapeMsg},
		{name: "a trailing newline", in: "directory:" + PV + "\n", wantErr: shapeMsg},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseID(tc.in)
			if tc.wantErr != "" {
				wantInvalid(t, err, tc.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("ParseID(%q) = %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("ParseID(%q) = %+v, want %+v", tc.in, got, tc.want)
			}
		})
	}
}

func TestIDRoundTrip(t *testing.T) {
	t.Parallel()
	id := ID{Kind: KindDirectory, Name: PV}
	if got, want := id.String(), "directory:"+PV; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
	back, err := ParseID(id.String())
	if err != nil {
		t.Fatalf("ParseID(String()) = %v", err)
	}
	if back != id {
		t.Errorf("round trip = %+v, want %+v", back, id)
	}
}

// TestNameLimitFitsEveryKind keeps the longest ID within CSI's 128-byte
// volume_id limit when a kind is added.
func TestNameLimitFitsEveryKind(t *testing.T) {
	t.Parallel()
	for _, k := range []Kind{KindDirectory} {
		id := ID{Kind: k, Name: strings.Repeat("a", maxNameLen)}
		if got := len(id.String()); got > maxIDLen {
			t.Errorf("longest %s ID is %d bytes, over %d", k, got, maxIDLen)
		}
	}
}

func TestNewID(t *testing.T) {
	t.Parallel()
	id, err := NewID(KindDirectory, PV)
	if err != nil || id.String() != "directory:"+PV {
		t.Fatalf("NewID() = %+v, %v", id, err)
	}
	_, err = NewID("", PV)
	wantInvalid(t, err, "kind")
	_, err = NewID(KindDirectory, "")
	wantInvalid(t, err, "must not be empty")
	_, err = NewID(KindDirectory, "team-a")
	wantInvalid(t, err, shapeMsg)
}

func TestIsPVName(t *testing.T) {
	t.Parallel()
	if !IsPVName(PV) {
		t.Errorf("IsPVName(%q) = false", PV)
	}
	for _, n := range []string{"", "team-a", "pvc-1", PV + "x", "x" + PV, strings.ToUpper(PV)} {
		if IsPVName(n) {
			t.Errorf("IsPVName(%q) = true", n)
		}
	}
}

// A name that ResolveSubdir accepts as a subdir element is never a volume
// name, and the other way round: the two rules are one shape, used in
// opposite directions.
func TestSubdirAndVolumeNamesAreDisjoint(t *testing.T) {
	t.Parallel()
	for _, n := range []string{PV, "team-a", "pvc-1", "x"} {
		_, subdirErr := ResolveSubdir(n, Tokens{})
		_, idErr := NewID(KindDirectory, n)
		if (subdirErr == nil) == (idErr == nil) {
			t.Errorf("%q: subdir error %v, ID error %v; exactly one of them must refuse it", n, subdirErr, idErr)
		}
	}
}
