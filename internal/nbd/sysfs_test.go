package nbd

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDevicesParsesSysfsFixture(t *testing.T) {
	t.Parallel()
	got, err := Devices(filepath.Join("testdata", "sys"))
	if err != nil {
		t.Fatal(err)
	}
	want := []Device{
		{Index: 0, Pid: 4321, Backend: "pvc-00000000-0000-0000-0000-000000000001", Size: 64 << 20},
		{Index: 1, Pid: 0, Backend: "", Size: 0},
		{Index: 12, Pid: 0, Backend: "", Size: 4096},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Devices:\n got %+v\nwant %+v", got, want)
	}
	if got2, err := Real(filepath.Join("testdata", "sys")).Devices(); err != nil || !reflect.DeepEqual(got2, want) {
		t.Fatalf("Real().Devices: %+v, %v", got2, err)
	}
}

func TestDevicesRejectsGarbage(t *testing.T) {
	t.Parallel()
	for name, files := range map[string]map[string]string{
		"pid":  {"pid": "abc\n"},
		"size": {"size": "-1\n"},
	} {
		root := t.TempDir()
		dir := filepath.Join(root, "block", "nbd3")
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatal(err)
		}
		for f, v := range files {
			if err := os.WriteFile(filepath.Join(dir, f), []byte(v), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := Devices(root); err == nil {
			t.Errorf("%s: garbage accepted", name)
		}
	}
	if _, err := Devices(t.TempDir()); err == nil {
		t.Error("a sysfs without block/ accepted")
	}
}
