package docker

import (
	"context"
	"fmt"
	"github.com/containerd/errdefs"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	outrunner "github.com/NetwindHQ/gha-outrunner"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
)

var diagnosticsMu sync.Mutex

// Provisioner creates ephemeral Docker containers as GitHub Actions runners.
type Provisioner struct {
	logger *slog.Logger
	client *client.Client
}

func New(logger *slog.Logger) (*Provisioner, error) {
	opts := []client.Opt{client.FromEnv, client.WithAPIVersionNegotiation()}

	// If DOCKER_HOST isn't set, ask the docker CLI for the active context's endpoint.
	if os.Getenv("DOCKER_HOST") == "" {
		if host := dockerHostFromContext(); host != "" {
			logger.Info("Auto-detected Docker host", slog.String("host", host))
			opts = append(opts, client.WithHost(host))
		}
	}

	cli, err := client.NewClientWithOpts(opts...)
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}

	return &Provisioner{
		logger: logger,
		client: cli,
	}, nil
}

// dockerHostFromContext asks the docker CLI for the active context's endpoint.
// Works with Colima, Docker Desktop, Podman, etc.
func dockerHostFromContext() string {
	out, err := exec.Command("docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}").Output()
	if err != nil {
		return ""
	}
	host := strings.TrimSpace(string(out))
	if host == "" || host == "<no value>" {
		return ""
	}
	// Verify the socket actually exists
	if strings.HasPrefix(host, "unix://") {
		if _, err := os.Stat(strings.TrimPrefix(host, "unix://")); err != nil {
			return ""
		}
	}
	return host
}

func (d *Provisioner) Start(ctx context.Context, req *outrunner.RunnerRequest) error {
	if req.Runner == nil || req.Runner.Docker == nil {
		return fmt.Errorf("no docker config for runner %s", req.Name)
	}
	dcfg := req.Runner.Docker
	img := dcfg.Image

	runnerCmd := dcfg.RunnerCmd
	if runnerCmd == "" {
		runnerCmd = "./run.sh"
	}

	// Pull image only if not available locally
	_, err := d.client.ImageInspect(ctx, img)
	if err != nil {
		d.logger.Debug("Pulling image", slog.String("image", img))
		reader, pullErr := d.client.ImagePull(ctx, img, image.PullOptions{})
		if pullErr != nil {
			return fmt.Errorf("pull image: %w", pullErr)
		}
		if _, err := io.Copy(io.Discard, reader); err != nil {
			_ = reader.Close()
			return fmt.Errorf("drain image pull: %w", err)
		}
		_ = reader.Close()
	}

	var mounts []mount.Mount
	for _, m := range dcfg.Mounts {
		mounts = append(mounts, mount.Mount{
			Type:     mount.TypeBind,
			Source:   m.Source,
			Target:   m.Target,
			ReadOnly: m.ReadOnly,
		})
	}
	networkMode := container.NetworkMode("")
	if dcfg.HostNetwork {
		networkMode = container.NetworkMode("host")
	}
	var tmpfs map[string]string
	if os.Getenv("OUTRUNNER_DOCKER_EXEC_SHM") == "1" {
		// Some CI suites execute temporary helpers from /dev/shm. Docker's
		// default mount is noexec; keep this opt-in for isolated runner hosts.
		tmpfs = map[string]string{"/dev/shm": "rw,exec,nosuid,nodev,size=1g"}
	}

	resp, err := d.client.ContainerCreate(ctx,
		&container.Config{
			Image: img,
			Cmd:   []string{runnerCmd, "--jitconfig", req.JITConfig},
			Labels: map[string]string{
				"outrunner":      "true",
				"outrunner.name": req.Name,
			},
		},
		&container.HostConfig{
			AutoRemove:  false,
			LogConfig:   container.LogConfig{Type: "json-file", Config: map[string]string{"max-size": "1m", "max-file": "2"}},
			Mounts:      mounts,
			NetworkMode: networkMode,
			Tmpfs:       tmpfs,
		},
		nil, nil, req.Name,
	)
	if err != nil {
		return fmt.Errorf("create container: %w", err)
	}

	if err := d.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("start container: %w", err)
	}

	d.logger.Info("Container started",
		slog.String("name", req.Name),
		slog.String("image", img),
		slog.String("id", resp.ID[:12]),
	)
	return nil
}

// Wait polls retained container state, including exits that beat Start's return.
// Inspection failures are retried: inability to observe is not proof of exit.
func (d *Provisioner) Wait(ctx context.Context, name string) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		info, err := d.client.ContainerInspect(ctx, name)
		if errdefs.IsNotFound(err) {
			return nil
		}
		if err == nil && info.State != nil && !info.State.Running {
			d.logger.Warn("Container exited", slog.String("name", name),
				slog.Int("exitCode", info.State.ExitCode), slog.Bool("oomKilled", info.State.OOMKilled))
			return nil
		}
		if err != nil && ctx.Err() == nil {
			d.logger.Warn("Container inspection failed; retrying", slog.String("name", name), slog.Any("error", err))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (d *Provisioner) Stop(ctx context.Context, name string) error {
	d.logger.Debug("Stopping container", slog.String("name", name))
	if err := d.client.ContainerStop(ctx, name, container.StopOptions{}); err != nil && !errdefs.IsNotFound(err) && !errdefs.IsNotModified(err) {
		// A stop response can fail after the daemon stopped the container.
		// Non-forced removal below is the final proof: it refuses a live one.
		d.logger.Warn("Container stop failed; checking removal", slog.String("name", name), slog.Any("error", err))
	}
	// Retain bounded diagnostics outside the disposable container, with private
	// permissions. Do not print job output or JIT material into the journal.
	d.preserveLogs(ctx, name)
	if err := d.client.ContainerRemove(ctx, name, container.RemoveOptions{RemoveVolumes: true}); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("remove container: %w", err)
	}
	return nil
}

func (d *Provisioner) preserveLogs(ctx context.Context, name string) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cache, err := os.UserCacheDir()
	if err != nil {
		return
	}
	dir := filepath.Join(cache, "outrunner", "diagnostics")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return
	}
	diagnosticsMu.Lock()
	defer diagnosticsMu.Unlock()
	reader, err := d.client.ContainerLogs(ctx, name, container.LogsOptions{ShowStdout: true, ShowStderr: true, Tail: "200"})
	if err != nil {
		return
	}
	defer func() { _ = reader.Close() }()
	file, err := os.CreateTemp(dir, "container-*.log")
	if err != nil {
		return
	}
	defer func() { _ = file.Close() }()
	_, writeErr := fmt.Fprintf(file, "runner=%s\n", name)
	if writeErr == nil {
		_, writeErr = io.Copy(file, io.LimitReader(reader, 64*1024))
	}
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		d.logger.Warn("Container diagnostics incomplete", slog.String("name", name), slog.Any("writeError", writeErr), slog.Any("closeError", closeErr))
		return
	}
	path := file.Name()
	// Keep the latest 32 records across all scale sets. Unique files and a
	// process-wide lock avoid overwriting simultaneous failures by hash collision.
	entries, err := os.ReadDir(dir)
	if err == nil {
		var records []os.FileInfo
		for _, entry := range entries {
			if !entry.IsDir() && strings.HasPrefix(entry.Name(), "container-") && strings.HasSuffix(entry.Name(), ".log") {
				if info, err := entry.Info(); err == nil {
					records = append(records, info)
				}
			}
		}
		sort.Slice(records, func(i, j int) bool { return records[i].ModTime().Before(records[j].ModTime()) })
		for _, record := range records[:max(0, len(records)-32)] {
			if err := os.Remove(filepath.Join(dir, record.Name())); err != nil {
				d.logger.Warn("Cannot prune old diagnostics", slog.Any("error", err))
			}
		}
	}
	d.logger.Info("Container diagnostics retained", slog.String("name", name), slog.String("path", path))
}

// Cleanup removes retained environments from a prior process using both our
// ownership label and the exact scale-set name prefix.
func (d *Provisioner) Cleanup(ctx context.Context, prefix string) error {
	containers, err := d.client.ContainerList(ctx, container.ListOptions{All: true, Filters: filters.NewArgs(filters.Arg("label", "outrunner=true"))})
	if err != nil {
		return fmt.Errorf("list orphan containers: %w", err)
	}
	for _, item := range containers {
		name := item.Labels["outrunner.name"]
		if split := strings.LastIndexByte(name, '-'); split < 0 || name[:split+1] != prefix {
			continue
		}
		if item.State != "exited" && item.State != "dead" && item.State != "created" {
			// A process restart must not abort a job that survived it. Refuse
			// new admission and drain proof until the old environment exits.
			d.logger.Warn("Waiting for surviving runner before admission", slog.String("name", name))
			if err := d.Wait(ctx, name); err != nil {
				return fmt.Errorf("wait for orphan %s: %w", name, err)
			}
		}
		d.preserveLogs(ctx, name)
		// Never call Stop or force removal here: the state can change after
		// listing, and Docker must reject removal if it is running again.
		if err := d.client.ContainerRemove(ctx, name, container.RemoveOptions{RemoveVolumes: true}); err != nil && !errdefs.IsNotFound(err) {
			return fmt.Errorf("remove orphan container %s: %w", name, err)
		}
	}
	return nil
}

func (d *Provisioner) Close() error {
	return d.client.Close()
}
