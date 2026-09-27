package docker

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	outrunner "github.com/NetwindHQ/gha-outrunner"
)

// Run explicitly with a local credential-free image whose /exit exits at once.
func TestImmediateExitIntegration(t *testing.T) {
	image := os.Getenv("OUTRUNNER_EXIT_TEST_IMAGE")
	if image == "" {
		t.Skip("requires local exit-test Docker image")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	p, err := New(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	name := "outrunner-exit-regression"
	defer func() { _ = p.Stop(context.Background(), name) }()
	if err := p.Start(ctx, &outrunner.RunnerRequest{Name: name, Runner: &outrunner.RunnerConfig{Docker: &outrunner.DockerImage{Image: image, RunnerCmd: "/exit"}}}); err != nil {
		t.Fatal(err)
	}
	if err := p.Wait(ctx, name); err != nil {
		t.Fatal(err)
	}
	info, err := p.client.ContainerInspect(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.ExitCode != 17 {
		t.Fatalf("exit=%d", info.State.ExitCode)
	}
	if err := p.Stop(ctx, name); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(os.Getenv("XDG_CACHE_HOME"), "outrunner", "diagnostics", "*.log"))
	if err != nil || len(files) != 1 {
		t.Fatalf("diagnostics missing: %v %v", files, err)
	}
	if err := p.Stop(ctx, name); err != nil {
		t.Fatalf("repeated cleanup: %v", err)
	}
}
