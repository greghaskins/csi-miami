package volume

import (
	"maps"
	"strings"
	"testing"

	"github.com/greghaskins/csi-miami/internal/transport"
)

// del, as an override value, deletes the key.
const del = "\x00delete"

// nfsParams returns valid StorageClass parameters for an NFS transport with
// the provisioner's metadata, with overrides applied.
func nfsParams(overrides map[string]string) map[string]string {
	p := map[string]string{
		"kind":                             "directory",
		"transport.kind":                   "nfs3",
		"transport.server":                 "nas.example.com",
		"transport.export":                 "/export/data",
		"csi.storage.k8s.io/pvc/name":      "data",
		"csi.storage.k8s.io/pvc/namespace": "team-a",
		"csi.storage.k8s.io/pv/name":       pvUUID1,
	}
	for k, v := range overrides {
		if v == del {
			delete(p, k)
		} else {
			p[k] = v
		}
	}
	return p
}

func TestParseParameters(t *testing.T) {
	t.Parallel()
	nfs := transport.Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/data"}
	tokens := Tokens{PVCNamespace: "team-a", PVCName: "data"}

	tests := []struct {
		name   string
		params map[string]string
		want   Params
		// wantErr is a substring of the error message; empty means valid.
		wantErr string
	}{
		{
			name:   "nfs3 with no subdir",
			params: nfsParams(nil),
			want:   Params{Kind: KindDirectory, Transport: nfs, Tokens: tokens},
		},
		{
			name:   "explicit subdir",
			params: nfsParams(map[string]string{"subdir": "${pvc.namespace}/${pvc.name}"}),
			want:   Params{Kind: KindDirectory, Transport: nfs, SubdirTemplate: "${pvc.namespace}/${pvc.name}", Tokens: tokens},
		},
		{
			name:   "explicit empty subdir means no prefix",
			params: nfsParams(map[string]string{"subdir": ""}),
			want:   Params{Kind: KindDirectory, Transport: nfs, Tokens: tokens},
		},
		{
			name: "local without provisioner metadata",
			params: map[string]string{
				"kind": "directory", "transport.kind": "local", "transport.path": "/export/data",
			},
			want: Params{Kind: KindDirectory, Transport: transport.Spec{Kind: "local", Path: "/export/data"}},
		},
		{
			name:   "transport is normalized",
			params: nfsParams(map[string]string{"transport.server": "NAS.Example.com", "transport.export": "/export/data/"}),
			want:   Params{Kind: KindDirectory, Transport: nfs, Tokens: tokens},
		},
		{
			name:   "other csi.storage.k8s.io keys are ignored, pv/name is not a token",
			params: nfsParams(map[string]string{"csi.storage.k8s.io/pv/annotations": "x", "csi.storage.k8s.io/future": "y"}),
			want:   Params{Kind: KindDirectory, Transport: nfs, Tokens: tokens},
		},

		{name: "no parameters", params: nil, wantErr: "parameter kind is required"},
		{name: "missing kind", params: nfsParams(map[string]string{"kind": del}), wantErr: "parameter kind is required"},
		{name: "empty kind", params: nfsParams(map[string]string{"kind": ""}), wantErr: "parameter kind must be directory"},
		{name: "unknown kind", params: nfsParams(map[string]string{"kind": "image"}), wantErr: "parameter kind must be directory"},
		{name: "missing transport.kind", params: nfsParams(map[string]string{"transport.kind": del}), wantErr: "transport.kind is required"},
		{name: "unknown transport.kind", params: nfsParams(map[string]string{"transport.kind": "smb"}), wantErr: "transport.kind must be"},
		{name: "nfs3 without server", params: nfsParams(map[string]string{"transport.server": del}), wantErr: "transport.server is required"},
		{name: "nfs3 without export", params: nfsParams(map[string]string{"transport.export": del}), wantErr: "transport.export is required"},
		{name: "nfs3 export is the root", params: nfsParams(map[string]string{"transport.export": "/"}), wantErr: "filesystem root"},
		{name: "local without path", params: map[string]string{"kind": "directory", "transport.kind": "local"}, wantErr: "transport.path is required"},

		{name: "unknown parameter", params: nfsParams(map[string]string{"subDir": "x"}), wantErr: `unknown parameter "subDir"`},
		{name: "parameter names are case sensitive", params: nfsParams(map[string]string{"Kind": "directory"}), wantErr: `unknown parameter "Kind"`},
		{name: "typo in transport key", params: nfsParams(map[string]string{"transport.servr": "x"}), wantErr: "unknown parameter"},
		{name: "similar prefix is not csi.storage.k8s.io", params: nfsParams(map[string]string{"csi.storage.k8s.io": "x"}), wantErr: "unknown parameter"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseParameters(tc.params)
			if tc.wantErr != "" {
				wantInvalid(t, err, tc.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("ParseParameters() = %v", err)
			}
			if got != tc.want {
				t.Errorf("ParseParameters() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestParseParametersErrorsNameOnlyTheKey(t *testing.T) {
	t.Parallel()
	params := nfsParams(map[string]string{"typo": "a-distinctive-value"})
	_, err := ParseParameters(params)
	wantInvalid(t, err, `"typo"`)
	if strings.Contains(err.Error(), "a-distinctive-value") || strings.Contains(err.Error(), "nas.example.com") {
		t.Errorf("error %q leaks parameter values", err.Error())
	}
}

func TestParseParametersQuotesUntrustedKeys(t *testing.T) {
	t.Parallel()
	// U+0085 is a C1 control: not printable, but above the C0 range.
	nel := string(rune(0x85))
	long := strings.Repeat("k", 100)
	_, err := ParseParameters(nfsParams(map[string]string{long: "x", "bad\"key\n" + nel: "y"}))
	wantInvalid(t, err, "unknown parameter")
	if len(err.Error()) > 200 {
		t.Errorf("error is %d bytes; keys must be truncated", len(err.Error()))
	}
	if strings.ContainsAny(err.Error(), "\n"+nel) {
		t.Errorf("error %q contains a non-printable character", err.Error())
	}
}

func TestNewContext(t *testing.T) {
	t.Parallel()
	nfs := transport.Spec{Kind: "nfs3", Server: "nas.example.com", Export: "/export/data"}
	tests := []struct {
		name    string
		params  map[string]string
		want    Context
		wantErr string
	}{
		{
			name:   "no subdir is no prefix",
			params: nfsParams(nil),
			want:   Context{Kind: KindDirectory, Transport: nfs},
		},
		{
			name:   "empty subdir is no prefix",
			params: nfsParams(map[string]string{"subdir": ""}),
			want:   Context{Kind: KindDirectory, Transport: nfs},
		},
		{
			name:   "namespace and name tokens",
			params: nfsParams(map[string]string{"subdir": "${pvc.namespace}/${pvc.name}"}),
			want:   Context{Kind: KindDirectory, Transport: nfs, Subdir: "team-a/data"},
		},
		{
			name:   "template that cleans to dot is no prefix",
			params: nfsParams(map[string]string{"subdir": "./"}),
			want:   Context{Kind: KindDirectory, Transport: nfs},
		},
		{
			name:   "forged pv/name does not steer anything",
			params: nfsParams(map[string]string{"subdir": "x", "csi.storage.k8s.io/pv/name": "../../etc"}),
			want:   Context{Kind: KindDirectory, Transport: nfs, Subdir: "x"},
		},
		{
			name:    "pv.name token is withdrawn",
			params:  nfsParams(map[string]string{"subdir": "${pv.name}"}),
			wantErr: `unknown token "pv.name"`,
		},
		{
			name:    "token without metadata",
			params:  nfsParams(map[string]string{"subdir": "${pvc.name}", "csi.storage.k8s.io/pvc/name": del}),
			wantErr: "${pvc.name} is empty",
		},
		{
			name:    "subdir escaping the transport",
			params:  nfsParams(map[string]string{"subdir": "../x"}),
			wantErr: "'..'",
		},
		{
			name:    "PVC name with a slash",
			params:  nfsParams(map[string]string{"subdir": "${pvc.name}", "csi.storage.k8s.io/pvc/name": "a/b"}),
			wantErr: "single path element",
		},
		{
			name:    "PVC named like a PV",
			params:  nfsParams(map[string]string{"subdir": "${pvc.name}", "csi.storage.k8s.io/pvc/name": pvUUID2}),
			wantErr: "PV name",
		},
		{
			name:    "unknown parameter",
			params:  nfsParams(map[string]string{"mode": "0777"}),
			wantErr: "unknown parameter",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := NewContext(tc.params)
			if tc.wantErr != "" {
				wantInvalid(t, err, tc.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("NewContext() = %v", err)
			}
			if got != tc.want {
				t.Errorf("NewContext() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestNewContextDoesNotModifyParams(t *testing.T) {
	t.Parallel()
	params := nfsParams(nil)
	before := maps.Clone(params)
	if _, err := NewContext(params); err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(params, before) {
		t.Errorf("params changed: %v", params)
	}
}
