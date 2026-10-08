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
	second := name + "-second"
	defer func() { _ = p.Stop(context.Background(), second) }()
	if err := p.Start(ctx, &outrunner.RunnerRequest{Name: second, Runner: &outrunner.RunnerConfig{Docker: &outrunner.DockerImage{Image: image, RunnerCmd: "/exit"}}}); err != nil {
		t.Fatal(err)
	}
	if err := p.Wait(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := p.Stop(ctx, second); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(os.Getenv("XDG_CACHE_HOME"), "outrunner", "diagnostics", "*.log"))
	if err != nil || len(files) != 2 {
		t.Fatalf("distinct diagnostics missing: %v %v", files, err)
	}
	if err := p.Stop(ctx, name); err != nil {
		t.Fatalf("repeated cleanup: %v", err)
	}
}

func TestRestartCleanupPreservesLiveContainer(t *testing.T) {
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
	name := "outrunner-live-regression"
	defer func() { _ = p.Stop(context.Background(), name) }()
	if err := p.Start(ctx, &outrunner.RunnerRequest{Name: name, JITConfig: "hold", Runner: &outrunner.RunnerConfig{Docker: &outrunner.DockerImage{Image: image, RunnerCmd: "/exit"}}}); err != nil {
		t.Fatal(err)
	}
	// The sibling scale set named "outrunner" must not claim this runner.
	siblingCtx, siblingCancel := context.WithTimeout(ctx, time.Second)
	defer siblingCancel()
	if err := p.Cleanup(siblingCtx, "outrunner-"); err != nil {
		t.Fatalf("sibling scale set claimed a foreign orphan: %v", err)
	}
	recovered := make(chan error, 1)
	go func() { recovered <- p.Cleanup(ctx, "outrunner-live-") }()
	select {
	case err := <-recovered:
		t.Fatalf("recovery returned while orphan is live: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	info, err := p.client.ContainerInspect(ctx, name)
	if err != nil || !info.State.Running {
		t.Fatalf("restart interrupted live job: %v", err)
	}
	if err := p.Stop(ctx, name); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-recovered:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("recovery did not resume after orphan exit")
	}
}

func TestRestartCleanupStopsOrphanGitHubReleased(t *testing.T) {
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
	name := "outrunner-idle-orphan"
	defer func() { _ = p.Stop(context.Background(), name) }()
	if err := p.Start(ctx, &outrunner.RunnerRequest{Name: name, JITConfig: "hold", Runner: &outrunner.RunnerConfig{Docker: &outrunner.DockerImage{Image: image, RunnerCmd: "/exit"}}}); err != nil {
		t.Fatal(err)
	}
	var asked []string
	p.SetOrphanReleaser(func(_ context.Context, orphan string) (bool, error) {
		asked = append(asked, orphan)
		return true, nil
	})
	// The container would hold for a minute; GitHub's release stops it now.
	start := time.Now()
	if err := p.Cleanup(ctx, "outrunner-idle-"); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 15*time.Second {
		t.Fatalf("cleanup waited %s for an idle orphan GitHub released", elapsed)
	}
	if len(asked) != 1 || asked[0] != name {
		t.Fatalf("expected GitHub to be asked about %s, got %v", name, asked)
	}
	if _, err := p.client.ContainerInspect(ctx, name); err == nil {
		t.Fatal("released orphan was not removed")
	}
}

func TestRestartCleanupWaitsForOrphanGitHubKeeps(t *testing.T) {
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
	name := "outrunner-busy-orphan"
	defer func() { _ = p.Stop(context.Background(), name) }()
	if err := p.Start(ctx, &outrunner.RunnerRequest{Name: name, JITConfig: "hold", Runner: &outrunner.RunnerConfig{Docker: &outrunner.DockerImage{Image: image, RunnerCmd: "/exit"}}}); err != nil {
		t.Fatal(err)
	}
	// GitHub says the runner holds a job: cleanup must keep waiting.
	p.SetOrphanReleaser(func(context.Context, string) (bool, error) { return false, nil })
	recovered := make(chan error, 1)
	go func() { recovered <- p.Cleanup(ctx, "outrunner-busy-") }()
	select {
	case err := <-recovered:
		t.Fatalf("cleanup returned while GitHub reports a job: %v", err)
	case <-time.After(2 * time.Second):
	}
	info, err := p.client.ContainerInspect(ctx, name)
	if err != nil || !info.State.Running {
		t.Fatalf("cleanup stopped a runner GitHub says holds a job: %v", err)
	}
	if err := p.Stop(ctx, name); err != nil {
		t.Fatal(err)
	}
	if err := <-recovered; err != nil {
		t.Fatal(err)
	}
}
