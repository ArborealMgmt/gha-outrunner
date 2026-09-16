package outrunner

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareDrainReceiptsRemovesStaleFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "drain-receipt.json")
	if err := os.WriteFile(path, []byte(`{"status":"drained"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	config := &Config{Runners: map[string]RunnerConfig{
		"test": {DrainReceipt: &DrainReceiptConfig{Path: path}},
	}}
	if err := PrepareDrainReceipts(config); err != nil {
		t.Fatalf("PrepareDrainReceipts: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("stale receipt still exists: %v", err)
	}
}

func TestPrepareDrainReceiptsRejectsDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "drain-receipt.json")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	config := &Config{Runners: map[string]RunnerConfig{
		"test": {DrainReceipt: &DrainReceiptConfig{Path: path}},
	}}
	if err := PrepareDrainReceipts(config); err == nil {
		t.Fatal("expected a receipt directory to fail")
	}
}
