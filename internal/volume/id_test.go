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

func TestParseID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want ID
		// wantErr is a substring of the error message; empty means valid.
		wantErr string
	}{
		{name: "pvc name", in: "directory:pvc-0a1b2c3d-1111-2222-3333-444455556666", want: ID{KindDirectory, "pvc-0a1b2c3d-1111-2222-3333-444455556666"}},
		{name: "every allowed character", in: "directory:aZ09._-", want: ID{KindDirectory, "aZ09._-"}},
		{name: "longest name", in: "directory:" + strings.Repeat("a", 200), want: ID{KindDirectory, strings.Repeat("a", 200)}},
		{name: "dots inside a name", in: "directory:a..b", want: ID{KindDirectory, "a..b"}},

		{name: "empty", in: "", wantErr: "<kind>:<name>"},
		{name: "no separator", in: "directory", wantErr: "<kind>:<name>"},
		{name: "empty kind", in: ":pvc-1", wantErr: "kind"},
		{name: "unknown kind", in: "image:pvc-1", wantErr: "kind"},
		{name: "kind is case sensitive", in: "Directory:pvc-1", wantErr: "kind"},
		{name: "empty name", in: "directory:", wantErr: "must not be empty"},
		{name: "name too long", in: "directory:" + strings.Repeat("a", 201), wantErr: "at most 200 bytes"},
		{name: "second colon", in: "directory:pvc:1", wantErr: "may contain only"},
		{name: "slash in name", in: "directory:pvc/1", wantErr: "may contain only"},
		{name: "space in name", in: "directory:pvc 1", wantErr: "may contain only"},
		{name: "NUL in name", in: "directory:pvc\x001", wantErr: "may contain only"},
		{name: "non-ASCII name", in: "directory:pvc-é", wantErr: "may contain only"},
		{name: "dot name", in: "directory:.", wantErr: "'.' or '..'"},
		{name: "dot dot name", in: "directory:..", wantErr: "'.' or '..'"},
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
	id := ID{Kind: KindDirectory, Name: "pvc-0a1b2c3d-1111-2222-3333-444455556666"}
	if got, want := id.String(), "directory:pvc-0a1b2c3d-1111-2222-3333-444455556666"; got != want {
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

func TestNewID(t *testing.T) {
	t.Parallel()
	id, err := NewID(KindDirectory, "pvc-1")
	if err != nil || id.String() != "directory:pvc-1" {
		t.Fatalf("NewID() = %+v, %v", id, err)
	}
	_, err = NewID("", "pvc-1")
	wantInvalid(t, err, "kind")
	_, err = NewID(KindDirectory, "")
	wantInvalid(t, err, "must not be empty")
}
