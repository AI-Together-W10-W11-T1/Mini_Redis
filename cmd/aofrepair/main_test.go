package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/store"
)

func TestRecoveryCLI(t *testing.T) {
	input := filepath.Join(t.TempDir(), "source.aof")
	s, err := store.OpenPersistent(nil, nil, store.DefaultAOFOptions(input))
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Set("keep", "value", nil); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	good, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	damaged := append(append([]byte{}, good...), make([]byte, 64)...)
	if err = os.WriteFile(input, damaged, 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err = run([]string{"-input", input}, &output); err == nil {
		t.Fatal("inspection should report damage")
	}
	var report store.AOFInspection
	if err = json.Unmarshal(output.Bytes(), &report); err != nil || !report.ZeroTail {
		t.Fatalf("report=%s err=%v", output.String(), err)
	}
	target := filepath.Join(t.TempDir(), "fixed.aof")
	if err = run([]string{"-input", input, "-output", target}, &bytes.Buffer{}); err == nil {
		t.Fatal("missing acknowledgement accepted")
	}
	if err = run([]string{"-input", input, "-output", target, "-allow-data-loss"}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if err = run([]string{"-input", target}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(input)
	if !bytes.Equal(after, damaged) {
		t.Fatal("source changed")
	}
	if err = run([]string{"-unknown"}, &bytes.Buffer{}); err == nil {
		t.Fatal("unknown flag accepted")
	}
}
