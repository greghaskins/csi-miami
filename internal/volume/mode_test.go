package volume

import (
	"maps"
	"strings"
	"testing"

	"github.com/greghaskins/csi-miami/internal/transport"
)

func TestParseMode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in        string
		mode      uint32
		canonical string
		wantErr   bool
	}{
		{in: "0755", mode: 0o755, canonical: "0755"},
		{in: "755", mode: 0o755, canonical: "0755"},
		{in: "0777", mode: 0o777, canonical: "0777"},
		{in: "2775", mode: 0o2775, canonical: "2775"},
		{in: "1777", mode: 0o1777, canonical: "1777"},
		{in: "4755", mode: 0o4755, canonical: "4755"},
		{in: "7777", mode: 0o7777, canonical: "7777"},
		{in: "0", mode: 0, canonical: "0000"},
		{in: "0000", mode: 0, canonical: "0000"},
		{in: "70", mode: 0o70, canonical: "0070"},

		{in: "", wantErr: true},
		{in: "10755", wantErr: true},
		{in: "00755", wantErr: true},
		{in: "0758", wantErr: true},
		{in: "8", wantErr: true},
		{in: "0o755", wantErr: true},
		{in: "0x1ed", wantErr: true},
		{in: "-755", wantErr: true},
		{in: "+755", wantErr: true},
		{in: " 755", wantErr: true},
		{in: "755 ", wantErr: true},
		{in: "7\n5", wantErr: true},
		{in: "rwxr-xr-x", wantErr: true},
		{in: "u=rwx", wantErr: true},
		{in: "０７５５", wantErr: true}, // full-width digits
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			mode, canonical, err := ParseMode(tc.in)
			if tc.wantErr {
				wantInvalid(t, err, "mode")
				return
			}
			if err != nil {
				t.Fatalf("ParseMode(%q) = %v", tc.in, err)
			}
			if mode != tc.mode || canonical != tc.canonical {
				t.Errorf("ParseMode(%q) = %o, %q; want %o, %q", tc.in, mode, canonical, tc.mode, tc.canonical)
			}
		})
	}
}

func TestCheckModeAcceptsOnlyTheCanonicalForm(t *testing.T) {
	t.Parallel()
	for _, ok := range []string{"", "0755", "0777", "2775", "0000"} {
		if err := CheckMode(ok); err != nil {
			t.Errorf("CheckMode(%q) = %v", ok, err)
		}
	}
	for _, bad := range []string{"755", "0o755", "12345", "0758", "x"} {
		wantInvalid(t, CheckMode(bad), "mode")
	}
}

func TestModeBits(t *testing.T) {
	t.Parallel()
	if got := ModeBits(""); got != DefaultMode {
		t.Errorf("ModeBits(\"\") = %o, want the default %o", got, DefaultMode)
	}
	if got := ModeBits("2775"); got != 0o2775 {
		t.Errorf("ModeBits(2775) = %o", got)
	}
	if got := (Context{}).DirMode(); got != 0o755 {
		t.Errorf("an empty Context's DirMode = %o, want 755", got)
	}
	if got := (Context{Mode: "0777"}).DirMode(); got != 0o777 {
		t.Errorf("DirMode = %o, want 777", got)
	}
}

func TestParseParametersMode(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ in, want string }{
		{"0777", "0777"}, {"2775", "2775"}, {"755", "0755"}, {"", ""},
	} {
		got, err := ParseParameters(nfsParams(map[string]string{"mode": tc.in}))
		if err != nil {
			t.Fatalf("mode %q: %v", tc.in, err)
		}
		if got.Mode != tc.want {
			t.Errorf("mode %q: Params.Mode = %q, want %q", tc.in, got.Mode, tc.want)
		}
	}
	got, err := ParseParameters(nfsParams(nil))
	if err != nil || got.Mode != "" {
		t.Errorf("no mode parameter: %q, %v; want empty", got.Mode, err)
	}
	for _, bad := range []string{"0758", "10755", "0o755", "-1", "rwx"} {
		_, err := ParseParameters(nfsParams(map[string]string{"mode": bad}))
		wantInvalid(t, err, "parameter mode")
		if strings.Contains(err.Error(), bad) {
			t.Errorf("error %q repeats the value", err)
		}
	}
	// Parameter names are case sensitive, like the others.
	_, err = ParseParameters(nfsParams(map[string]string{"Mode": "0777"}))
	wantInvalid(t, err, `unknown parameter "Mode"`)
}

func TestNewContextMode(t *testing.T) {
	t.Parallel()
	c, err := NewContext(nfsParams(map[string]string{"mode": "2775"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.Mode != "2775" || c.DirMode() != 0o2775 {
		t.Errorf("Mode = %q, DirMode = %o", c.Mode, c.DirMode())
	}
	enc := c.Encode()
	if enc["miami/mode"] != "2775" {
		t.Errorf("Encode() = %v, want miami/mode 2775", enc)
	}
	back, err := DecodeContext(enc)
	if err != nil || back != c {
		t.Errorf("round trip = %+v, %v; want %+v", back, err, c)
	}

	// Absent: no key at all, so the context of a volume without the parameter
	// is exactly what it was before the parameter existed.
	plain, err := NewContext(nfsParams(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, has := plain.Encode()["miami/mode"]; has || plain.Mode != "" || plain.DirMode() != DefaultMode {
		t.Errorf("no parameter: Encode() = %v, Mode = %q", plain.Encode(), plain.Mode)
	}
	want := map[string]string{
		"miami/kind": "directory", "miami/transport.kind": "nfs3",
		"miami/transport.server": "nas.example.com", "miami/transport.export": "/export/data",
	}
	if got := plain.Encode(); !maps.Equal(got, want) {
		t.Errorf("Encode() = %v, want %v", got, want)
	}

	_, err = NewContext(nfsParams(map[string]string{"mode": "0o755"}))
	wantInvalid(t, err, "mode")
}

func TestDecodeContextMode(t *testing.T) {
	t.Parallel()
	c, err := DecodeContext(nfsContext(map[string]string{"miami/mode": "0777"}))
	if err != nil || c.Mode != "0777" {
		t.Fatalf("Mode = %q, %v", c.Mode, err)
	}
	// A context from before the key reads as the default.
	c, err = DecodeContext(nfsContext(nil))
	if err != nil || c.Mode != "" || c.DirMode() != DefaultMode {
		t.Fatalf("Mode = %q, %v", c.Mode, err)
	}
	// Decode never repairs: only the form NewContext produces.
	for _, bad := range []string{"755", "0o755", "0758", "12345", " 0755"} {
		_, err := DecodeContext(nfsContext(map[string]string{"miami/mode": bad}))
		wantInvalid(t, err, "mode")
	}
}

func TestContextValidateMode(t *testing.T) {
	t.Parallel()
	c := Context{Kind: KindDirectory, Transport: transport.Spec{Kind: "local", Path: "/export/data"}, Mode: "2775"}
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() = %v", err)
	}
	c.Mode = "755"
	wantInvalid(t, c.Validate(), "mode")
}
