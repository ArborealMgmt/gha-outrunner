package outrunner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/actions/scaleset"
)

// mockClient implements ScaleSetClient for testing.
type mockClient struct {
	mu          sync.Mutex
	nextID      int
	removeCount atomic.Int32
	jitErr      error         // if set, GenerateJitRunnerConfig returns this error
	jitBlock    chan struct{} // if set, JIT calls after the first block until closed or ctx ends
	jitCalls    atomic.Int32
	removeErr   error
	removeFn    func(runnerID int64) error // if set, overrides removeErr per call
}

// jitIssued returns how many JIT configs the mock has handed out.
func (m *mockClient) jitIssued() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nextID - 1
}

func newMockClient() *mockClient {
	return &mockClient{nextID: 1}
}

func (m *mockClient) GenerateJitRunnerConfig(ctx context.Context, setting *scaleset.RunnerScaleSetJitRunnerSetting, _ int) (*scaleset.RunnerScaleSetJitRunnerConfig, error) {
	if m.jitCalls.Add(1) > 1 && m.jitBlock != nil {
		select {
		case <-m.jitBlock:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if m.jitErr != nil {
		return nil, m.jitErr
	}

	m.mu.Lock()
	id := m.nextID
	m.nextID++
	m.mu.Unlock()

	return &scaleset.RunnerScaleSetJitRunnerConfig{
		Runner: &scaleset.RunnerReference{
			ID:   id,
			Name: setting.Name,
		},
		EncodedJITConfig: "test-jit-config",
	}, nil
}

func (m *mockClient) RemoveRunner(_ context.Context, runnerID int64) error {
	m.removeCount.Add(1)
	if m.removeFn != nil {
		return m.removeFn(runnerID)
	}
	return m.removeErr
}

// mockProvisioner implements Provisioner for testing.
type mockProvisioner struct {
	mu       sync.Mutex
	started  []string
	stopped  []string
	startErr error
	stopErr  error
	startCh  chan struct{} // if set, Start blocks until closed
	stopCh   chan struct{} // if set, Stop blocks until closed
}

func newMockProvisioner() *mockProvisioner {
	return &mockProvisioner{}
}

func (m *mockProvisioner) Start(ctx context.Context, req *RunnerRequest) error {
	if m.startCh != nil {
		select {
		case <-m.startCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if m.startErr != nil {
		return m.startErr
	}
	m.mu.Lock()
	m.started = append(m.started, req.Name)
	m.mu.Unlock()
	return nil
}

func (m *mockProvisioner) Stop(_ context.Context, name string) error {
	m.mu.Lock()
	stopCh := m.stopCh
	m.mu.Unlock()
	if stopCh != nil {
		<-stopCh
	}
	m.mu.Lock()
	m.stopped = append(m.stopped, name)
	m.mu.Unlock()
	return m.stopErr
}

func (m *mockProvisioner) Close() error { return nil }

func (m *mockProvisioner) stoppedNames() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.stopped...)
}

func newTestScaler(client ScaleSetClient, prov Provisioner) *Scaler {
	return NewScaler(
		noopLogger(),
		client, 1, 10, "test",
		&RunnerConfig{Docker: &DockerImage{Image: "test:latest"}},
		prov,
	)
}

func noopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestNonBlockingProvisioning(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	prov.startCh = make(chan struct{}) // Start blocks until closed
	s := newTestScaler(client, prov)

	// HandleDesiredRunnerCount should return immediately
	done := make(chan struct{})
	go func() {
		count, err := s.HandleDesiredRunnerCount(context.Background(), 1)
		if err != nil {
			t.Errorf("HandleDesiredRunnerCount: %v", err)
		}
		if count != 1 {
			t.Errorf("expected count 1, got %d", count)
		}
		close(done)
	}()

	select {
	case <-done:
		// returned before Start completed
	case <-time.After(2 * time.Second):
		t.Fatal("HandleDesiredRunnerCount blocked on Start()")
	}

	// Start is still blocked, runner is in Provisioning phase
	runners := s.Runners()
	if len(runners) != 1 {
		t.Fatalf("expected 1 runner, got %d", len(runners))
	}
	if runners[0].Phase != RunnerProvisioning {
		t.Errorf("expected Provisioning, got %s", runners[0].Phase)
	}

	// Unblock Start
	close(prov.startCh)
	time.Sleep(50 * time.Millisecond) // let goroutine proceed

	runners = s.Runners()
	if len(runners) != 1 {
		t.Fatalf("expected 1 runner, got %d", len(runners))
	}
	if runners[0].Phase != RunnerIdle {
		t.Errorf("expected Idle, got %s", runners[0].Phase)
	}

	s.Shutdown(context.Background())
}

func TestHappyPath(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	s := newTestScaler(client, prov)

	// Request a runner
	count, err := s.HandleDesiredRunnerCount(context.Background(), 1)
	if err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}

	// Wait for provisioning
	time.Sleep(50 * time.Millisecond)

	runners := s.Runners()
	if len(runners) != 1 {
		t.Fatalf("expected 1 runner, got %d", len(runners))
	}
	name := runners[0].Name

	// Job started
	_ = s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
		RunnerName: name,
	})
	runners = s.Runners()
	if runners[0].Phase != RunnerRunning {
		t.Errorf("expected Running, got %s", runners[0].Phase)
	}

	// Job completed
	_ = s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{
		RunnerName: name,
		Result:     "succeeded",
	})

	// Wait for goroutine to finish cleanup
	time.Sleep(100 * time.Millisecond)

	runners = s.Runners()
	if len(runners) != 0 {
		t.Errorf("expected 0 runners, got %d", len(runners))
	}

	// Verify Stop was called
	stopped := prov.stoppedNames()
	if len(stopped) != 1 || stopped[0] != name {
		t.Errorf("expected Stop(%s), got %v", name, stopped)
	}

	// Verify RemoveRunner was called
	if client.removeCount.Load() != 1 {
		t.Errorf("expected 1 RemoveRunner call, got %d", client.removeCount.Load())
	}

	s.Shutdown(context.Background())
}

func TestMaxJobsStopsAdmissionBeforeRunnerRemoval(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	runner := &RunnerConfig{
		MaxJobs: 1,
		Docker:  &DockerImage{Image: "test:latest"},
	}
	s := NewScaler(noopLogger(), client, 1, 1, "test", runner, prov)

	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	name := s.Runners()[0].Name

	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{
		RunnerName: name,
		Result:     "succeeded",
	}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}

	// A desired-count message racing completion must not admit replacement work.
	if count, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount while draining: %v", err)
	} else if count > 1 {
		t.Fatalf("expected at most the completing runner, got %d", count)
	}

	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("scaler did not report drained")
	}

	if count, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount after drain: %v", err)
	} else if count != 0 {
		t.Fatalf("expected zero runners after drain, got %d", count)
	}
	if client.nextID != 2 {
		t.Fatalf("expected exactly one JIT runner, next ID is %d", client.nextID)
	}

	s.Shutdown(context.Background())
}

func TestMaxJobsWritesReceiptAfterCleanup(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	receiptPath := filepath.Join(t.TempDir(), "drain-receipt.json")
	runner := &RunnerConfig{
		MaxJobs: 1,
		DrainReceipt: &DrainReceiptConfig{
			Path: receiptPath,
			Identity: map[string]string{
				"provider":    "gcp",
				"instance_id": "1234",
			},
		},
		Docker: &DockerImage{Image: "test:latest"},
	}
	s := NewScaler(noopLogger(), client, 38, 1, "receipt-test", runner, prov)
	_, _ = s.HandleDesiredRunnerCount(context.Background(), 1)
	time.Sleep(50 * time.Millisecond)
	name := s.Runners()[0].Name
	finishedAt := time.Now().UTC().Truncate(time.Second)
	queueTime := finishedAt.Add(-2 * time.Minute)
	scaleSetAssignTime := finishedAt.Add(-90 * time.Second)
	runnerAssignTime := finishedAt.Add(-time.Minute)
	_ = s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
		RunnerID:   1,
		RunnerName: name,
		JobMessageBase: scaleset.JobMessageBase{
			QueueTime: queueTime,
		},
	})
	_ = s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{
		RunnerID:   1,
		RunnerName: name,
		Result:     "succeeded",
		JobMessageBase: scaleset.JobMessageBase{
			RunnerRequestID:    99,
			JobID:              "job-42",
			JobWorkflowRef:     "ArborealMgmt/MaynardApp/.github/workflows/tests.yml@refs/pull/1/merge",
			JobDisplayName:     "Unit Tests (1/3)",
			WorkflowRunID:      123,
			OwnerName:          "ArborealMgmt",
			RepositoryName:     "MaynardApp",
			RequestLabels:      []string{"outrunner-gcp-linux-x64"},
			ScaleSetAssignTime: scaleSetAssignTime,
			RunnerAssignTime:   runnerAssignTime,
			FinishTime:         finishedAt,
		},
	})

	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("scaler did not report drained")
	}

	data, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatalf("read drain receipt: %v", err)
	}
	var receipt DrainReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatalf("parse drain receipt: %v", err)
	}
	if receipt.Version != 3 || receipt.Status != "drained" || receipt.ScaleSet != "receipt-test" {
		t.Fatalf("unexpected receipt identity: %#v", receipt)
	}
	if receipt.CompletedJobs != 1 || receipt.MaxJobs != 1 || len(receipt.Jobs) != 1 {
		t.Fatalf("unexpected receipt counts: %#v", receipt)
	}
	if receipt.Identity["instance_id"] != "1234" || receipt.Jobs[0].JobID != "job-42" {
		t.Fatalf("unexpected receipt detail: %#v", receipt)
	}
	if receipt.Jobs[0].FinishedAt != finishedAt {
		t.Fatalf("unexpected finish time: %s", receipt.Jobs[0].FinishedAt)
	}
	job := receipt.Jobs[0]
	if job.JobWorkflowRef != "ArborealMgmt/MaynardApp/.github/workflows/tests.yml@refs/pull/1/merge" ||
		job.JobDisplayName != "Unit Tests (1/3)" ||
		len(job.RequestLabels) != 1 || job.RequestLabels[0] != "outrunner-gcp-linux-x64" {
		t.Fatalf("unexpected GitHub job identity: %#v", job)
	}
	if job.QueueTime != queueTime || job.ScaleSetAssignTime != scaleSetAssignTime ||
		job.RunnerAssignTime != runnerAssignTime {
		t.Fatalf("unexpected GitHub job timing: %#v", job)
	}
	info, err := os.Stat(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("expected mode 0600, got %o", info.Mode().Perm())
	}

	s.Shutdown(context.Background())
}

func TestReceiptQueueTimeUsesConservativeAvailableBound(t *testing.T) {
	completed := time.Date(2026, 9, 18, 15, 0, 0, 0, time.UTC)
	started := completed.Add(time.Second)
	assigned := started.Add(time.Second)
	cases := []struct {
		name      string
		completed time.Time
		started   time.Time
		expected  time.Time
	}{
		{name: "completed message", completed: completed, started: started, expected: completed},
		{name: "started message", started: started, expected: started},
		{name: "assignment fallback", expected: assigned},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual := receiptQueueTime(tc.completed, tc.started, assigned)
			if actual != tc.expected {
				t.Fatalf("expected %s, got %s", tc.expected, actual)
			}
		})
	}
}

func TestExternalDrainWritesZeroJobReceiptAtomically(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	receiptPath := filepath.Join(t.TempDir(), "drain-receipt.json")
	runner := &RunnerConfig{
		MaxJobs: 1,
		DrainReceipt: &DrainReceiptConfig{
			Path: receiptPath,
			Identity: map[string]string{
				"provider":    "gcp",
				"instance_id": "1234",
			},
		},
		Docker: &DockerImage{Image: "test:latest"},
	}
	s := NewScaler(noopLogger(), client, 38, 1, "receipt-test", runner, prov)

	if err := s.RequestDrain(); err != nil {
		t.Fatalf("RequestDrain: %v", err)
	}
	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("scaler did not report externally drained")
	}
	if count, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount after external drain: %v", err)
	} else if count != 0 {
		t.Fatalf("external drain admitted %d runners", count)
	}

	data, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatalf("read drain receipt: %v", err)
	}
	var receipt DrainReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatalf("parse drain receipt: %v", err)
	}
	if receipt.Version != 2 || receipt.Reason != "external" {
		t.Fatalf("unexpected external receipt: %#v", receipt)
	}
	if receipt.CompletedJobs != 0 || len(receipt.Jobs) != 0 {
		t.Fatalf("zero-job drain recorded jobs: %#v", receipt)
	}
	var rawReceipt map[string]any
	if err := json.Unmarshal(data, &rawReceipt); err != nil {
		t.Fatalf("parse raw drain receipt: %v", err)
	}
	if jobs, ok := rawReceipt["jobs"].([]any); !ok || len(jobs) != 0 {
		t.Fatalf("zero-job drain must encode jobs as an empty array, got %#v", rawReceipt["jobs"])
	}
	if client.nextID != 1 {
		t.Fatalf("zero-job drain generated a JIT runner, next ID is %d", client.nextID)
	}

	s.Shutdown(context.Background())
}

func TestExternalDrainWaitsForTrackedRunner(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	receiptPath := filepath.Join(t.TempDir(), "drain-receipt.json")
	runner := &RunnerConfig{
		MaxJobs:      1,
		DrainReceipt: &DrainReceiptConfig{Path: receiptPath},
		Docker:       &DockerImage{Image: "test:latest"},
	}
	s := NewScaler(noopLogger(), client, 38, 1, "receipt-test", runner, prov)
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	name := s.Runners()[0].Name

	if err := s.RequestDrain(); err != nil {
		t.Fatalf("RequestDrain: %v", err)
	}
	select {
	case <-s.Drained():
		t.Fatal("external drain completed before tracked runner cleanup")
	default:
	}
	if count, err := s.HandleDesiredRunnerCount(context.Background(), 2); err != nil {
		t.Fatalf("HandleDesiredRunnerCount while externally draining: %v", err)
	} else if count != 1 {
		t.Fatalf("expected only tracked runner while draining, got %d", count)
	}

	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{
		RunnerID:   1,
		RunnerName: name,
		Result:     "succeeded",
	}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("external drain did not finish after tracked runner cleanup")
	}

	data, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatalf("read drain receipt: %v", err)
	}
	var receipt DrainReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatalf("parse drain receipt: %v", err)
	}
	if receipt.Version != 3 || receipt.Reason != "external" || receipt.CompletedJobs != 1 {
		t.Fatalf("unexpected external receipt after job: %#v", receipt)
	}

	s.Shutdown(context.Background())
}

func TestReceiptFailureDoesNotReportDrained(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	runner := &RunnerConfig{
		MaxJobs: 1,
		DrainReceipt: &DrainReceiptConfig{
			Path: filepath.Join(t.TempDir(), "directory"),
		},
		Docker: &DockerImage{Image: "test:latest"},
	}
	if err := os.Mkdir(runner.DrainReceipt.Path, 0o700); err != nil {
		t.Fatal(err)
	}
	s := NewScaler(noopLogger(), client, 1, 1, "test", runner, prov)
	_, _ = s.HandleDesiredRunnerCount(context.Background(), 1)
	time.Sleep(50 * time.Millisecond)
	name := s.Runners()[0].Name
	_ = s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{
		RunnerID:   1,
		RunnerName: name,
		Result:     "succeeded",
	})
	time.Sleep(100 * time.Millisecond)

	select {
	case <-s.Drained():
		t.Fatal("reported drained after receipt write failure")
	default:
	}
	if count, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount while failed-draining: %v", err)
	} else if count != 0 {
		t.Fatalf("expected admission to remain closed, got %d runners", count)
	}

	s.Shutdown(context.Background())
}

func TestStopFailureDoesNotWriteReceipt(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	prov.stopErr = errors.New("container still running")
	receiptPath := filepath.Join(t.TempDir(), "drain-receipt.json")
	runner := &RunnerConfig{
		MaxJobs:      1,
		DrainReceipt: &DrainReceiptConfig{Path: receiptPath},
		Docker:       &DockerImage{Image: "test:latest"},
	}
	s := NewScaler(noopLogger(), client, 1, 1, "test", runner, prov)
	_, _ = s.HandleDesiredRunnerCount(context.Background(), 1)
	time.Sleep(50 * time.Millisecond)
	name := s.Runners()[0].Name
	_ = s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{
		RunnerID:   1,
		RunnerName: name,
		Result:     "succeeded",
	})
	time.Sleep(100 * time.Millisecond)

	select {
	case <-s.Drained():
		t.Fatal("reported drained after container stop failure")
	default:
	}
	if _, err := os.Stat(receiptPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("receipt must not exist after container stop failure: %v", err)
	}
	if client.removeCount.Load() != 0 {
		t.Fatalf("runner must not deregister after failed stop, got %d calls", client.removeCount.Load())
	}

	s.Shutdown(context.Background())
}

func TestDuplicateCompletionCountsOnce(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	runner := &RunnerConfig{
		MaxJobs: 2,
		Docker:  &DockerImage{Image: "test:latest"},
	}
	s := NewScaler(noopLogger(), client, 1, 1, "test", runner, prov)
	_, _ = s.HandleDesiredRunnerCount(context.Background(), 1)
	time.Sleep(50 * time.Millisecond)
	name := s.Runners()[0].Name
	completion := &scaleset.JobCompleted{RunnerName: name, Result: "succeeded"}
	_ = s.HandleJobCompleted(context.Background(), completion)
	_ = s.HandleJobCompleted(context.Background(), completion)
	time.Sleep(100 * time.Millisecond)

	select {
	case <-s.Drained():
		t.Fatal("duplicate completion incorrectly exhausted max_jobs")
	default:
	}
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("replacement admission after first job: %v", err)
	}

	s.Shutdown(context.Background())
}

func TestMaxJobsDoesNotReportDrainedWhenDeregistrationFails(t *testing.T) {
	client := newMockClient()
	client.removeErr = errors.New("GitHub unavailable")
	prov := newMockProvisioner()
	receiptPath := filepath.Join(t.TempDir(), "drain-receipt.json")
	runner := &RunnerConfig{
		MaxJobs:      1,
		DrainReceipt: &DrainReceiptConfig{Path: receiptPath},
		Docker:       &DockerImage{Image: "test:latest"},
	}
	s := NewScaler(noopLogger(), client, 1, 1, "test", runner, prov)
	_, _ = s.HandleDesiredRunnerCount(context.Background(), 1)
	time.Sleep(50 * time.Millisecond)
	name := s.Runners()[0].Name
	_ = s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{
		RunnerName: name,
		Result:     "succeeded",
	})
	time.Sleep(100 * time.Millisecond)

	select {
	case <-s.Drained():
		t.Fatal("reported drained after deregistration failure")
	default:
	}
	if count, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount while failed-draining: %v", err)
	} else if count != 1 {
		t.Fatalf("expected failed runner to remain tracked, got %d runners", count)
	}
	if _, err := os.Stat(receiptPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("receipt must not exist after deregistration failure: %v", err)
	}

	s.Shutdown(context.Background())
}

func TestProvisioningFailure(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	prov.startErr = context.DeadlineExceeded
	s := newTestScaler(client, prov)

	count, err := s.HandleDesiredRunnerCount(context.Background(), 1)
	if err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count 1 (provisioning), got %d", count)
	}

	// Wait for goroutine to detect failure and clean up
	time.Sleep(100 * time.Millisecond)

	runners := s.Runners()
	if len(runners) != 0 {
		t.Errorf("expected 0 runners after failure, got %d", len(runners))
	}

	// RemoveRunner should be called (deregistration)
	if client.removeCount.Load() != 1 {
		t.Errorf("expected 1 RemoveRunner call, got %d", client.removeCount.Load())
	}

	// Start can fail after creating the environment; cleanup is required.
	stopped := prov.stoppedNames()
	if len(stopped) != 1 {
		t.Errorf("expected cleanup after failed Start, got %v", stopped)
	}

	s.Shutdown(context.Background())
}

func TestShutdownDuringProvisioning(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	prov.startCh = make(chan struct{}) // Start blocks
	s := newTestScaler(client, prov)

	_, _ = s.HandleDesiredRunnerCount(context.Background(), 1)

	// Runner is provisioning
	time.Sleep(50 * time.Millisecond)
	runners := s.Runners()
	if len(runners) != 1 {
		t.Fatalf("expected 1 runner, got %d", len(runners))
	}

	// Shutdown cancels lifecycle context, which cancels Start
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.Shutdown(ctx)

	// Runner should be cleaned up
	runners = s.Runners()
	if len(runners) != 0 {
		t.Errorf("expected 0 runners after shutdown, got %d", len(runners))
	}

	// RemoveRunner should be called
	if client.removeCount.Load() != 1 {
		t.Errorf("expected 1 RemoveRunner call, got %d", client.removeCount.Load())
	}
}

func TestCountAccuracy(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	s := newTestScaler(client, prov)

	// Request 3 runners
	count, err := s.HandleDesiredRunnerCount(context.Background(), 3)
	if err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected count 3, got %d", count)
	}

	// Requesting 3 again should not create more (already at 3)
	count, err = s.HandleDesiredRunnerCount(context.Background(), 3)
	if err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected count 3, got %d", count)
	}

	s.Shutdown(context.Background())
}

func TestShutdownWaitsForGoroutines(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	prov.startCh = make(chan struct{}) // Start blocks
	s := newTestScaler(client, prov)

	_, _ = s.HandleDesiredRunnerCount(context.Background(), 2)
	time.Sleep(50 * time.Millisecond)

	// Shutdown should block until goroutines finish
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.Shutdown(ctx)
	elapsed := time.Since(start)

	// Should complete quickly (Start is cancelled by lifecycle context)
	if elapsed > 3*time.Second {
		t.Errorf("Shutdown took too long: %v", elapsed)
	}

	runners := s.Runners()
	if len(runners) != 0 {
		t.Errorf("expected 0 runners, got %d", len(runners))
	}
}

func TestConcurrentRunners(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	s := newTestScaler(client, prov)

	// Start 3 runners
	_, _ = s.HandleDesiredRunnerCount(context.Background(), 3)
	time.Sleep(50 * time.Millisecond)

	runners := s.Runners()
	if len(runners) != 3 {
		t.Fatalf("expected 3 runners, got %d", len(runners))
	}

	// Complete them in reverse order
	for i := len(runners) - 1; i >= 0; i-- {
		_ = s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{
			RunnerName: runners[i].Name,
			Result:     "succeeded",
		})
	}

	time.Sleep(200 * time.Millisecond)

	remaining := s.Runners()
	if len(remaining) != 0 {
		t.Errorf("expected 0 runners, got %d", len(remaining))
	}

	stopped := prov.stoppedNames()
	if len(stopped) != 3 {
		t.Errorf("expected 3 Stop calls, got %d", len(stopped))
	}

	s.Shutdown(context.Background())
}

func TestMaxRunnersClamping(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	s := NewScaler(noopLogger(), client, 1, 2, "test",
		&RunnerConfig{Docker: &DockerImage{Image: "test:latest"}}, prov)

	// Request more than max
	count, err := s.HandleDesiredRunnerCount(context.Background(), 10)
	if err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	if count != 2 {
		t.Errorf("expected count clamped to 2, got %d", count)
	}

	s.Shutdown(context.Background())
}

func TestDesiredCountZero(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	s := newTestScaler(client, prov)

	count, err := s.HandleDesiredRunnerCount(context.Background(), 0)
	if err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	if count != 0 {
		t.Errorf("expected count 0, got %d", count)
	}

	runners := s.Runners()
	if len(runners) != 0 {
		t.Errorf("expected 0 runners, got %d", len(runners))
	}

	s.Shutdown(context.Background())
}

func TestJitConfigError(t *testing.T) {
	client := newMockClient()
	client.jitErr = errors.New("GitHub API unavailable")
	prov := newMockProvisioner()
	s := newTestScaler(client, prov)

	count, err := s.HandleDesiredRunnerCount(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error from GenerateJitRunnerConfig")
	}
	if count != 0 {
		t.Errorf("expected count 0 on JIT error, got %d", count)
	}

	runners := s.Runners()
	if len(runners) != 0 {
		t.Errorf("expected 0 runners on JIT error, got %d", len(runners))
	}

	s.Shutdown(context.Background())
}

func TestJobCompletedUnknownRunner(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	s := newTestScaler(client, prov)

	err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{
		RunnerName: "nonexistent-runner",
		Result:     "succeeded",
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	s.Shutdown(context.Background())
}

func TestJobStartedUnknownRunner(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	s := newTestScaler(client, prov)

	err := s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
		RunnerName: "nonexistent-runner",
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	s.Shutdown(context.Background())
}

func TestJobCompletedDuringProvisioning(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	prov.startCh = make(chan struct{})
	s := newTestScaler(client, prov)

	_, _ = s.HandleDesiredRunnerCount(context.Background(), 1)
	time.Sleep(50 * time.Millisecond)

	runners := s.Runners()
	if len(runners) != 1 {
		t.Fatalf("expected 1 runner, got %d", len(runners))
	}
	name := runners[0].Name

	// Job completes while still provisioning
	_ = s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{
		RunnerName: name,
		Result:     "succeeded",
	})

	// Unblock Start
	close(prov.startCh)
	time.Sleep(100 * time.Millisecond)

	// Runner should be cleaned up
	runners = s.Runners()
	if len(runners) != 0 {
		t.Errorf("expected 0 runners, got %d", len(runners))
	}

	// Stop should still be called
	stopped := prov.stoppedNames()
	if len(stopped) != 1 {
		t.Errorf("expected 1 Stop call, got %d", len(stopped))
	}

	s.Shutdown(context.Background())
}

func TestJobStartedDuringProvisioning(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	prov.startCh = make(chan struct{})
	s := newTestScaler(client, prov)

	_, _ = s.HandleDesiredRunnerCount(context.Background(), 1)
	time.Sleep(50 * time.Millisecond)

	runners := s.Runners()
	name := runners[0].Name

	// Job starts while provisioning
	_ = s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
		RunnerName: name,
	})

	// Phase should be Running even though Start hasn't returned
	runners = s.Runners()
	if runners[0].Phase != RunnerRunning {
		t.Errorf("expected Running, got %s", runners[0].Phase)
	}

	// Unblock Start
	close(prov.startCh)
	time.Sleep(50 * time.Millisecond)

	// Should stay Running (not reset to Idle)
	runners = s.Runners()
	if runners[0].Phase != RunnerRunning {
		t.Errorf("expected Running after Start, got %s", runners[0].Phase)
	}

	s.Shutdown(context.Background())
}

func TestShutdownWithNoRunners(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	s := newTestScaler(client, prov)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	s.Shutdown(ctx)
}

func TestRunnerNamePrefix(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	s := NewScaler(noopLogger(), client, 1, 10, "my-scale-set",
		&RunnerConfig{Docker: &DockerImage{Image: "test:latest"}}, prov)

	_, _ = s.HandleDesiredRunnerCount(context.Background(), 1)
	time.Sleep(50 * time.Millisecond)

	runners := s.Runners()
	if len(runners) != 1 {
		t.Fatalf("expected 1 runner, got %d", len(runners))
	}

	name := runners[0].Name
	if len(name) < len("my-scale-set-") {
		t.Fatalf("runner name too short: %s", name)
	}
	prefix := name[:len("my-scale-set-")]
	if prefix != "my-scale-set-" {
		t.Errorf("expected prefix 'my-scale-set-', got %q", prefix)
	}

	s.Shutdown(context.Background())
}

type watchedProvisioner struct {
	*mockProvisioner
	exit     chan struct{}
	watching chan struct{}
}

func (p *watchedProvisioner) Wait(ctx context.Context, _ string) error {
	close(p.watching)
	select {
	case <-p.exit:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestExitedContainerDoesNotStrandDrain(t *testing.T) {
	for _, stopFails := range []bool{false, true} {
		t.Run(fmt.Sprint(stopFails), func(t *testing.T) {
			prov := &watchedProvisioner{newMockProvisioner(), make(chan struct{}), make(chan struct{})}
			if stopFails {
				prov.stopErr = errors.New("docker unavailable")
			}
			client := newMockClient()
			s := newTestScaler(client, prov)
			s.exitGrace = time.Millisecond
			defer s.Shutdown(context.Background())
			if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
				t.Fatal(err)
			}
			<-prov.watching
			if err := s.RequestDrain(); err != nil {
				t.Fatal(err)
			}
			close(prov.exit) // no JobStarted or JobCompleted will ever arrive
			if stopFails {
				s.lifecycleCancel()
			}
			done := make(chan struct{})
			go func() { s.wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("dead container stranded drain")
			}
			select {
			case <-s.Drained():
				if stopFails {
					t.Fatal("cleanup failure produced drain proof")
				}
			default:
				if !stopFails {
					t.Fatal("cleaned container did not drain")
				}
			}
			if s.completedJobs != 0 {
				t.Fatal("invented a completed job")
			}
			if !stopFails && client.removeCount.Load() != 1 {
				t.Fatal("runner was not deregistered")
			}
		})
	}
}

func TestPartialStartFailureRequiresCleanup(t *testing.T) {
	prov := newMockProvisioner()
	prov.startErr = errors.New("container created but start failed")
	prov.stopErr = errors.New("cannot remove")
	client := newMockClient()
	s := newTestScaler(client, prov)
	defer s.Shutdown(context.Background())
	_, _ = s.HandleDesiredRunnerCount(context.Background(), 1)
	s.lifecycleCancel()
	s.wg.Wait()
	if len(prov.stoppedNames()) != 1 {
		t.Fatal("partial start was not cleaned up")
	}
	if client.removeCount.Load() != 0 {
		t.Fatal("deregistered without cleanup proof")
	}
	if err := s.RequestDrain(); err == nil {
		t.Fatal("unsafe drain succeeded")
	}
}

func TestDrainClosesAdmissionBeforeRunnerCleanup(t *testing.T) {
	for _, reason := range []string{"external", "max_jobs"} {
		t.Run(reason, func(t *testing.T) {
			prov := newMockProvisioner()
			prov.startCh = make(chan struct{})
			s := newTestScaler(newMockClient(), prov)
			s.runner.MaxJobs = 1
			defer s.Shutdown(context.Background())
			_, _ = s.HandleDesiredRunnerCount(context.Background(), 1)
			name := s.Runners()[0].Name
			if reason == "external" {
				_ = s.RequestDrain()
			} else {
				_ = s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: name})
			}
			select {
			case <-s.AdmissionClosed():
			default:
				t.Fatal("listener still accepting assignments")
			}
			select {
			case <-s.Drained():
				t.Fatal("drain completed before teardown")
			default:
			}
			count, err := s.HandleDesiredRunnerCount(context.Background(), 2)
			if err != nil || count != 1 {
				t.Fatalf("admitted replacement while draining: %d %v", count, err)
			}
		})
	}
}

type transientStopProvisioner struct {
	*mockProvisioner
	attempts atomic.Int32
}

func (p *transientStopProvisioner) Stop(ctx context.Context, name string) error {
	if p.attempts.Add(1) == 1 {
		return errors.New("temporary Docker failure")
	}
	return p.mockProvisioner.Stop(ctx, name)
}
func TestTransientCleanupFailureRecoversWithoutLosingDrainProof(t *testing.T) {
	prov := &transientStopProvisioner{mockProvisioner: newMockProvisioner()}
	s := newTestScaler(newMockClient(), prov)
	s.cleanupRetry = time.Millisecond
	defer s.Shutdown(context.Background())
	_, _ = s.HandleDesiredRunnerCount(context.Background(), 1)
	name := s.Runners()[0].Name
	_ = s.RequestDrain()
	_ = s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: name})
	select {
	case <-s.Drained():
	case <-time.After(time.Second):
		t.Fatal("transient cleanup failure wedged drain")
	}
	if prov.attempts.Load() != 2 {
		t.Fatal("cleanup did not retry")
	}
}

func TestMissingJITIdentityDoesNotProvisionUnrecoverableRunner(t *testing.T) {
	client := newMockClient()
	client.nextID = 0
	prov := newMockProvisioner()
	s := newTestScaler(client, prov)
	defer s.Shutdown(context.Background())
	count, err := s.HandleDesiredRunnerCount(context.Background(), 1)
	if err == nil || count != 0 || len(s.Runners()) != 0 || len(prov.started) != 0 {
		t.Fatalf("admitted runner without cleanup identity: count=%d err=%v", count, err)
	}
}

// waitForRunnerCount polls until the scaler tracks want runners.
func waitForRunnerCount(t *testing.T, s *Scaler, want int) []RunnerSnapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		runners := s.Runners()
		if len(runners) == want {
			return runners
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected %d runners, got %d", want, len(runners))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCleanupRefillsAssignmentWithoutWaitingForNextMessage(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	prov.stopCh = make(chan struct{}) // cleanup finishes only after the statistics below
	s := NewScaler(noopLogger(), client, 1, 1, "test",
		&RunnerConfig{Docker: &DockerImage{Image: "test:latest"}}, prov)
	defer s.Shutdown(context.Background())

	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	first := waitForRunnerCount(t, s, 1)[0].Name

	// The completion message's statistics already count the next assignment,
	// but the finished runner still holds the only slot when they arrive.
	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: first, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	if count, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	} else if count != 1 {
		t.Fatalf("expected the finishing runner to hold the slot, got %d", count)
	}
	close(prov.stopCh)

	// No further listener message: cleanup alone must provision the next runner.
	deadline := time.Now().Add(2 * time.Second)
	for {
		runners := s.Runners()
		if len(runners) == 1 && runners[0].Name != first {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("assigned job was not refilled after cleanup; runners %v", runners)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if client.nextID != 3 {
		t.Fatalf("expected exactly two JIT runners, next ID is %d", client.nextID)
	}
}

func TestCleanupDoesNotRefillWithoutAssignment(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	s := NewScaler(noopLogger(), client, 1, 1, "test",
		&RunnerConfig{Docker: &DockerImage{Image: "test:latest"}}, prov)
	defer s.Shutdown(context.Background())

	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	first := waitForRunnerCount(t, s, 1)[0].Name
	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: first, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	waitForRunnerCount(t, s, 0)
	time.Sleep(50 * time.Millisecond)
	if n := len(s.Runners()); n != 0 {
		t.Fatalf("expected no refill without an assignment, got %d runners", n)
	}
	if client.nextID != 2 {
		t.Fatalf("expected exactly one JIT runner, next ID is %d", client.nextID)
	}
}

func TestSynchronizedDrainRefillsAcceptedAssignmentThenDrains(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	s := NewScaler(noopLogger(), client, 1, 1, "test",
		&RunnerConfig{MaxJobs: 2, Docker: &DockerImage{Image: "test:latest"}}, prov,
		WithAdmissionSynchronization())
	defer s.Shutdown(context.Background())
	session := NewDrainSession(nil, s)

	if _, err := session.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	first := waitForRunnerCount(t, s, 1)[0].Name
	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: first, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	// An earlier positive-capacity poll accepted a second job.
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	var second string
	deadline := time.Now().Add(2 * time.Second)
	for second == "" {
		if runners := s.Runners(); len(runners) == 1 && runners[0].Name != first {
			second = runners[0].Name
		}
		if time.Now().After(deadline) {
			t.Fatal("accepted assignment was not refilled after cleanup")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: second, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	session.zeroCapacityPoll = true
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	waitForRunnerCount(t, s, 0)
	// A zero-capacity poll processed after cleanup completes the drain.
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("scaler did not drain after serving the accepted assignment")
	}
	if client.nextID != 3 {
		t.Fatalf("expected exactly two JIT runners, next ID is %d", client.nextID)
	}
}

func TestSynchronizedDrainWaitsForGitHubCountNotLocalCompletion(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	s := NewScaler(noopLogger(), client, 1, 1, "test",
		&RunnerConfig{MaxJobs: 1, Docker: &DockerImage{Image: "test:latest"}}, prov,
		WithAdmissionSynchronization())
	defer s.Shutdown(context.Background())
	session := NewDrainSession(nil, s)

	if _, err := session.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	first := waitForRunnerCount(t, s, 1)[0].Name
	// A processed zero-capacity poll while the job still runs: GitHub counts it.
	session.zeroCapacityPoll = true
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: first, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	waitForRunnerCount(t, s, 0)

	// Local completion accounting must not stand in for GitHub's statistics:
	// an assignment accepted by an earlier poll may still be on its way.
	select {
	case <-s.Drained():
		t.Fatal("drain proof published before GitHub reported zero assigned jobs")
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("scaler did not drain after GitHub reported zero assigned jobs")
	}
}

// Cleanup that beats the listener's next statistics must subtract the job it
// just served: stale assigned=1 is that job, so nothing is pending.
func TestCleanupBeforeStatisticsDoesNotRefillServedJob(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	s := NewScaler(noopLogger(), client, 1, 1, "test",
		&RunnerConfig{Docker: &DockerImage{Image: "test:latest"}}, prov)
	defer s.Shutdown(context.Background())

	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	first := waitForRunnerCount(t, s, 1)[0].Name
	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: first, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	waitForRunnerCount(t, s, 0)
	time.Sleep(50 * time.Millisecond)
	if n := len(s.Runners()); n != 0 {
		t.Fatalf("stale statistics refilled the served job: %d runners", n)
	}
	if client.nextID != 2 {
		t.Fatalf("expected exactly one JIT runner, next ID is %d", client.nextID)
	}
}

// Stale assigned=2 with one completion still leaves one queued job to serve.
func TestCleanupBeforeStatisticsRefillsRemainingAssignment(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	s := NewScaler(noopLogger(), client, 1, 1, "test",
		&RunnerConfig{Docker: &DockerImage{Image: "test:latest"}}, prov)
	defer s.Shutdown(context.Background())

	if _, err := s.HandleDesiredRunnerCount(context.Background(), 2); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	first := waitForRunnerCount(t, s, 1)[0].Name
	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: first, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if runners := s.Runners(); len(runners) == 1 && runners[0].Name != first {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("remaining assignment was not refilled; runners %v", s.Runners())
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if client.nextID != 3 {
		t.Fatalf("expected exactly two JIT runners, next ID is %d", client.nextID)
	}
}

// With two slots, two stale assignments and one completion, the busy runner
// already covers the remaining job, so refill adds nothing.
func TestCleanupBeforeStatisticsCountsBusyRunners(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	s := NewScaler(noopLogger(), client, 1, 2, "test",
		&RunnerConfig{Docker: &DockerImage{Image: "test:latest"}}, prov)
	defer s.Shutdown(context.Background())

	if _, err := s.HandleDesiredRunnerCount(context.Background(), 2); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	runners := waitForRunnerCount(t, s, 2)
	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: runners[0].Name, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	remaining := waitForRunnerCount(t, s, 1)
	time.Sleep(50 * time.Millisecond)
	if got := s.Runners(); len(got) != 1 || got[0].Name != remaining[0].Name {
		t.Fatalf("expected only the busy runner, got %v", got)
	}
	if client.nextID != 3 {
		t.Fatalf("expected exactly two JIT runners, next ID is %d", client.nextID)
	}
}

// Shutdown must cancel a refill's in-flight JIT request rather than queue
// behind the lock the refill holds.
func TestShutdownCancelsRefillJITRequest(t *testing.T) {
	client := newMockClient()
	client.jitBlock = make(chan struct{})
	defer close(client.jitBlock)
	prov := newMockProvisioner()
	s := NewScaler(noopLogger(), client, 1, 1, "test",
		&RunnerConfig{Docker: &DockerImage{Image: "test:latest"}}, prov)

	if _, err := s.HandleDesiredRunnerCount(context.Background(), 2); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	first := waitForRunnerCount(t, s, 1)[0].Name
	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: first, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for client.jitCalls.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("refill never requested a JIT config")
		}
		time.Sleep(5 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	s.Shutdown(ctx)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Shutdown waited %s behind the refill's JIT request", elapsed)
	}
}

// newDrainingPair builds a synchronized-drain scaler with two slots and
// max_jobs 1: one runner serves the only job, the other is spawned for a
// second assignment that has not reached it.
func newDrainingPair(t *testing.T, client *mockClient, prov Provisioner) (*Scaler, *DrainSession, RunnerSnapshot, RunnerSnapshot) {
	t.Helper()
	s := NewScaler(noopLogger(), client, 1, 2, "test",
		&RunnerConfig{MaxJobs: 1, Docker: &DockerImage{Image: "test:latest"}}, prov,
		WithAdmissionSynchronization())
	session := NewDrainSession(nil, s)
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 2); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	waitForIdle(t, s, 2)
	// Either runner can take the job; call the one that does "busy".
	runners := s.Runners()
	busy, spare := runners[0], runners[1]
	if err := s.HandleJobStarted(context.Background(), &scaleset.JobStarted{RunnerName: busy.Name}); err != nil {
		t.Fatalf("HandleJobStarted: %v", err)
	}
	return s, session, busy, spare
}

// waitForIdle polls until want runners have finished provisioning.
func waitForIdle(t *testing.T, s *Scaler, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		idle := 0
		for _, r := range s.Runners() {
			if r.Phase == RunnerIdle {
				idle++
			}
		}
		if idle == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected %d idle runners, got %d", want, idle)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestDrainReapsIdleRunnerWhoseAssignmentWentAway(t *testing.T) {
	client := newMockClient()
	s, session, busy, _ := newDrainingPair(t, client, newMockProvisioner())
	defer s.Shutdown(context.Background())

	// The only job completes and max_jobs closes admission. GitHub then
	// reports zero assigned jobs on a zero-capacity poll: the second
	// assignment was cancelled or went elsewhere.
	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: busy.Name, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	session.zeroCapacityPoll = true
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatalf("draining host never drained; runners %v", s.Runners())
	}
	if n := len(s.Runners()); n != 0 {
		t.Fatalf("expected the idle runner to be reaped, %d runners remain", n)
	}
}

func TestDrainKeepsIdleRunnerWhileAnAssignmentIsPending(t *testing.T) {
	client := newMockClient()
	s, session, busy, spare := newDrainingPair(t, client, newMockProvisioner())
	defer s.Shutdown(context.Background())

	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: busy.Name, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	// GitHub still counts the second assignment: the spare must serve it.
	session.zeroCapacityPoll = true
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	waitForRunnerCount(t, s, 1)
	time.Sleep(50 * time.Millisecond)
	if got := s.Runners(); len(got) != 1 || got[0].Name != spare.Name {
		t.Fatalf("expected the spare runner to stay for its assignment, got %v", got)
	}
	select {
	case <-s.Drained():
		t.Fatal("drained while an assignment was pending")
	default:
	}
}

func TestDrainKeepsRunnerThatTookAJobBeforeReap(t *testing.T) {
	client := newMockClient()
	var refused atomic.Bool
	var spareID atomic.Int64
	client.removeFn = func(id int64) error {
		if id == spareID.Load() && refused.CompareAndSwap(false, true) {
			return fmt.Errorf("remove: %w", scaleset.JobStillRunningError)
		}
		return nil
	}
	prov := newMockProvisioner()
	s, session, busy, spare := newDrainingPair(t, client, prov)
	defer s.Shutdown(context.Background())
	spareID.Store(int64(spare.RunnerID))

	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: busy.Name, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	session.zeroCapacityPoll = true
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !refused.Load() {
		if time.Now().After(deadline) {
			t.Fatal("reap was never attempted")
		}
		time.Sleep(5 * time.Millisecond)
	}
	waitForRunnerCount(t, s, 1)
	time.Sleep(50 * time.Millisecond)
	if got := s.Runners(); len(got) != 1 || got[0].Name != spare.Name {
		t.Fatalf("a runner GitHub says is busy must not be stopped; got %v", got)
	}
	for _, name := range prov.stoppedNames() {
		if name == spare.Name {
			t.Fatal("stopped the container of a runner GitHub reported busy")
		}
	}

	// It really did take the job; once that completes the host drains.
	if err := s.HandleJobStarted(context.Background(), &scaleset.JobStarted{RunnerName: spare.Name}); err != nil {
		t.Fatalf("HandleJobStarted: %v", err)
	}
	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: spare.Name, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	waitForRunnerCount(t, s, 0)
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("host did not drain after the late job completed")
	}
}

func TestIdleRunnerIsNotReapedBeforeDrain(t *testing.T) {
	client := newMockClient()
	s := NewScaler(noopLogger(), client, 1, 1, "test",
		&RunnerConfig{Docker: &DockerImage{Image: "test:latest"}}, newMockProvisioner(),
		WithAdmissionSynchronization())
	defer s.Shutdown(context.Background())
	session := NewDrainSession(nil, s)
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	waitForIdle(t, s, 1)
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if n := len(s.Runners()); n != 1 || client.removeCount.Load() != 0 {
		t.Fatalf("admission is open: expected the idle runner untouched, runners=%d removes=%d", n, client.removeCount.Load())
	}
}

func TestExternalDrainReapsIdleRunner(t *testing.T) {
	client := newMockClient()
	s := NewScaler(noopLogger(), client, 1, 1, "test",
		&RunnerConfig{MaxJobs: 5, Docker: &DockerImage{Image: "test:latest"}}, newMockProvisioner(),
		WithAdmissionSynchronization())
	defer s.Shutdown(context.Background())
	session := NewDrainSession(nil, s)
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	waitForIdle(t, s, 1)
	// The assignment went away before the job started.
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	if err := s.RequestDrain(); err != nil {
		t.Fatalf("RequestDrain: %v", err)
	}
	waitForRunnerCount(t, s, 0)
	session.zeroCapacityPoll = true
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("external drain never completed with an idle runner")
	}
}

// watchingProvisioner is a mockProvisioner whose containers never exit on
// their own, so only an explicit stop ends a runner.
type watchingProvisioner struct{ *mockProvisioner }

func (w watchingProvisioner) Wait(ctx context.Context, _ string) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestReapAfterProvisioningFinishesOnAClosedHost(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	prov.startCh = make(chan struct{})
	s := NewScaler(noopLogger(), client, 1, 1, "test",
		&RunnerConfig{MaxJobs: 5, Docker: &DockerImage{Image: "test:latest"}}, prov,
		WithAdmissionSynchronization())
	defer s.Shutdown(context.Background())
	session := NewDrainSession(nil, s)

	if _, err := session.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	waitForRunnerCount(t, s, 1)
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	if err := s.RequestDrain(); err != nil {
		t.Fatalf("RequestDrain: %v", err)
	}
	session.zeroCapacityPoll = true
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	if client.removeCount.Load() != 0 {
		t.Fatal("reaped a runner that was still provisioning")
	}
	// Provisioning finishes after admission closed: no further message.
	close(prov.startCh)
	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatalf("runner provisioned after drain was never reaped; runners %v", s.Runners())
	}
}

func TestReapIssuesOneRequestPerRunner(t *testing.T) {
	client := newMockClient()
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var spareID atomic.Int64
	var spareRemoves atomic.Int32
	client.removeFn = func(id int64) error {
		if id == spareID.Load() {
			spareRemoves.Add(1)
			<-release
		}
		return nil
	}
	s, session, busy, spare := newDrainingPair(t, client, newMockProvisioner())
	defer s.Shutdown(context.Background())
	spareID.Store(int64(spare.RunnerID))

	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: busy.Name, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	waitForRunnerCount(t, s, 1)
	session.zeroCapacityPoll = true
	for range 3 {
		if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
			t.Fatalf("HandleDesiredRunnerCount: %v", err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	if n := spareRemoves.Load(); n != 1 {
		t.Fatalf("expected one in-flight deregistration for the spare, got %d", n)
	}
	releaseOnce.Do(func() { close(release) })
	waitForRunnerCount(t, s, 0)
}

func TestReapedRunnerRefillsALateAssignment(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	s, session, busy, _ := newDrainingPair(t, client, prov)
	defer s.Shutdown(context.Background())

	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: busy.Name, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	waitForRunnerCount(t, s, 1)
	// Hold the reaped container's stop so the late assignment arrives while
	// it is still tracked.
	gate := make(chan struct{})
	prov.mu.Lock()
	prov.stopCh = gate
	prov.mu.Unlock()
	session.zeroCapacityPoll = true
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for client.removeCount.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("spare was never reaped")
		}
		time.Sleep(5 * time.Millisecond)
	}
	before := client.jitIssued()
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	close(gate)
	deadline = time.Now().Add(2 * time.Second)
	for client.jitIssued() == before {
		if time.Now().After(deadline) {
			t.Fatal("late assignment waited for another message after the reap")
		}
		time.Sleep(5 * time.Millisecond)
	}
	waitForIdle(t, s, 1)
}

func TestReapNotFoundWithoutWatcherCountsAsReaped(t *testing.T) {
	client := newMockClient()
	client.removeFn = func(int64) error { return fmt.Errorf("remove: %w", scaleset.RunnerNotFoundError) }
	s, session, busy, _ := newDrainingPair(t, client, newMockProvisioner())
	defer s.Shutdown(context.Background())

	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: busy.Name, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	session.zeroCapacityPoll = true
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatalf("not-found reap did not drain; runners %v", s.Runners())
	}
}

func TestReapNotFoundWithWatcherWaitsForExit(t *testing.T) {
	client := newMockClient()
	var spareID atomic.Int64
	client.removeFn = func(id int64) error {
		if id == spareID.Load() {
			return fmt.Errorf("remove: %w", scaleset.RunnerNotFoundError)
		}
		return nil
	}
	s, session, busy, spare := newDrainingPair(t, client, watchingProvisioner{newMockProvisioner()})
	defer s.Shutdown(context.Background())
	spareID.Store(int64(spare.RunnerID))

	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: busy.Name, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	session.zeroCapacityPoll = true
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	waitForRunnerCount(t, s, 1)
	time.Sleep(50 * time.Millisecond)
	// The runner may have served a job whose messages are late: only its own
	// exit or completion may end it.
	if got := s.Runners(); len(got) != 1 || got[0].Name != spare.Name {
		t.Fatalf("expected the spare kept until its container exits, got %v", got)
	}
	select {
	case <-s.Drained():
		t.Fatal("drained before the not-found runner's exit was observed")
	default:
	}
}

func newPrespawnScaler(client *mockClient, prov *mockProvisioner, runner *RunnerConfig) (*Scaler, *DrainSession) {
	if runner.Docker == nil {
		runner.Docker = &DockerImage{Image: "test:latest"}
	}
	if runner.IdleRunners == 0 {
		runner.IdleRunners = 1
	}
	s := NewScaler(noopLogger(), client, 1, 1, "test", runner, prov, WithAdmissionSynchronization())
	return s, NewDrainSession(nil, s)
}

func TestIdleRunnersPrespawnsWithoutAnAssignment(t *testing.T) {
	client := newMockClient()
	s, session := newPrespawnScaler(client, newMockProvisioner(), &RunnerConfig{})
	defer s.Shutdown(context.Background())

	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	waitForIdle(t, s, 1)
	// An assignment lands on the spare: no second runner beyond max_runners.
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	if n := len(s.Runners()); n != 1 || client.nextID != 2 {
		t.Fatalf("expected the spare to take the assignment, runners=%d nextID=%d", n, client.nextID)
	}
}

func TestIdleRunnersTopsUpAfterAJobWithoutANewMessage(t *testing.T) {
	client := newMockClient()
	s, session := newPrespawnScaler(client, newMockProvisioner(), &RunnerConfig{})
	defer s.Shutdown(context.Background())

	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	waitForIdle(t, s, 1)
	first := s.Runners()[0].Name
	if err := s.HandleJobStarted(context.Background(), &scaleset.JobStarted{RunnerName: first}); err != nil {
		t.Fatalf("HandleJobStarted: %v", err)
	}
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: first, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if r := s.Runners(); len(r) == 1 && r[0].Name != first && r[0].Phase == RunnerIdle {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no spare after the job; runners %v", s.Runners())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestIdleRunnersStayWithinTheMaxJobsBudget(t *testing.T) {
	client := newMockClient()
	s, session := newPrespawnScaler(client, newMockProvisioner(), &RunnerConfig{MaxJobs: 2})
	defer s.Shutdown(context.Background())

	serve := func() string {
		t.Helper()
		waitForIdle(t, s, 1)
		name := s.Runners()[0].Name
		if err := s.HandleJobStarted(context.Background(), &scaleset.JobStarted{RunnerName: name}); err != nil {
			t.Fatalf("HandleJobStarted: %v", err)
		}
		if _, err := session.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
			t.Fatalf("HandleDesiredRunnerCount: %v", err)
		}
		if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: name, Result: "succeeded"}); err != nil {
			t.Fatalf("HandleJobCompleted: %v", err)
		}
		return name
	}
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	first := serve()
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	// One job left in the budget: one spare for it.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if r := s.Runners(); len(r) == 1 && r[0].Name != first && r[0].Phase == RunnerIdle {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no spare for the last job; runners %v", s.Runners())
		}
		time.Sleep(5 * time.Millisecond)
	}
	serve()
	// max_jobs reached: no spare, and the host drains.
	waitForRunnerCount(t, s, 0)
	session.zeroCapacityPoll = true
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatalf("host did not drain at max_jobs; runners %v", s.Runners())
	}
	if client.nextID != 3 {
		t.Fatalf("expected exactly two JIT runners for two jobs, next ID is %d", client.nextID)
	}
}

func TestIdleRunnersSpareIsReapedOnExternalDrain(t *testing.T) {
	client := newMockClient()
	s, session := newPrespawnScaler(client, newMockProvisioner(), &RunnerConfig{MaxJobs: 5})
	defer s.Shutdown(context.Background())

	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	waitForIdle(t, s, 1)
	if err := s.RequestDrain(); err != nil {
		t.Fatalf("RequestDrain: %v", err)
	}
	waitForRunnerCount(t, s, 0)
	session.zeroCapacityPoll = true
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("external drain never completed with a spare runner")
	}
	if n := len(s.Runners()); n != 0 {
		t.Fatalf("drain must not top the spare back up, %d runners", n)
	}
}

func TestIdleRunnersSpareDoesNotBlockIdleLinger(t *testing.T) {
	client := newMockClient()
	s := NewScaler(noopLogger(), client, 1, 1, "test",
		&RunnerConfig{IdleRunners: 1, IdleDrainAfter: 150 * time.Millisecond, Docker: &DockerImage{Image: "test:latest"}},
		newMockProvisioner())
	defer s.Shutdown(context.Background())

	if _, err := s.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	waitForIdle(t, s, 1)
	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatalf("a parked spare kept the host from idle drain; runners %v", s.Runners())
	}
	if n := len(s.Runners()); n != 0 {
		t.Fatalf("expected the spare reaped on idle drain, %d runners", n)
	}
}

func TestIdleRunnersAssignedSpareBlocksIdleLinger(t *testing.T) {
	client := newMockClient()
	s := NewScaler(noopLogger(), client, 1, 1, "test",
		&RunnerConfig{IdleRunners: 1, IdleDrainAfter: 100 * time.Millisecond, Docker: &DockerImage{Image: "test:latest"}},
		newMockProvisioner())
	defer s.Shutdown(context.Background())

	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	waitForIdle(t, s, 1)
	select {
	case <-s.Drained():
		t.Fatal("idle linger drained a host whose runner covers an assignment")
	case <-time.After(400 * time.Millisecond):
	}
}

func TestIdleRunnersNoSpareBeyondTheLastJobWithTwoSlots(t *testing.T) {
	client := newMockClient()
	s := NewScaler(noopLogger(), client, 1, 2, "test",
		&RunnerConfig{IdleRunners: 1, MaxJobs: 1, Docker: &DockerImage{Image: "test:latest"}},
		newMockProvisioner(), WithAdmissionSynchronization())
	defer s.Shutdown(context.Background())
	session := NewDrainSession(nil, s)

	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	waitForIdle(t, s, 1)
	name := s.Runners()[0].Name
	if err := s.HandleJobStarted(context.Background(), &scaleset.JobStarted{RunnerName: name}); err != nil {
		t.Fatalf("HandleJobStarted: %v", err)
	}
	// The host's only job is running: a second slot is free, but max_jobs
	// leaves nothing for a spare to serve.
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if n := len(s.Runners()); n != 1 || client.nextID != 2 {
		t.Fatalf("spawned a spare past max_jobs: runners=%d nextID=%d", n, client.nextID)
	}
}

func TestIdleRunnersSpareBesideARunningJobIsReapedOnDrain(t *testing.T) {
	client := newMockClient()
	s := NewScaler(noopLogger(), client, 1, 2, "test",
		&RunnerConfig{IdleRunners: 1, Docker: &DockerImage{Image: "test:latest"}},
		newMockProvisioner(), WithAdmissionSynchronization())
	defer s.Shutdown(context.Background())
	session := NewDrainSession(nil, s)

	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	waitForIdle(t, s, 1)
	busy := s.Runners()[0].Name
	if err := s.HandleJobStarted(context.Background(), &scaleset.JobStarted{RunnerName: busy}); err != nil {
		t.Fatalf("HandleJobStarted: %v", err)
	}
	// The job runs on the first spare; a second slot holds the next spare.
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	waitForIdle(t, s, 1)
	waitForRunnerCount(t, s, 2)

	if err := s.RequestDrain(); err != nil {
		t.Fatalf("RequestDrain: %v", err)
	}
	// The running job keeps its runner; only the spare goes.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if r := s.Runners(); len(r) == 1 && r[0].Name == busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected only the busy runner after drain began; runners %v", s.Runners())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: busy, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	waitForRunnerCount(t, s, 0)
	session.zeroCapacityPoll = true
	if _, err := session.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("host did not drain")
	}
}

func TestIdleRunnersProvisioningSpareDoesNotBlockIdleLinger(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	prov.startCh = make(chan struct{})
	s := NewScaler(noopLogger(), client, 1, 1, "test",
		&RunnerConfig{IdleRunners: 1, IdleDrainAfter: 100 * time.Millisecond, Docker: &DockerImage{Image: "test:latest"}}, prov)
	defer s.Shutdown(context.Background())

	if _, err := s.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	waitForRunnerCount(t, s, 1)
	select {
	case <-s.AdmissionClosed():
	case <-time.After(2 * time.Second):
		t.Fatal("a spare still provisioning kept the host from idle drain")
	}
	close(prov.startCh)
	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatalf("spare was not reaped after it provisioned; runners %v", s.Runners())
	}
}

func TestIdleRunnersHostRetiresAfterServingAJob(t *testing.T) {
	client := newMockClient()
	s := NewScaler(noopLogger(), client, 1, 1, "test",
		&RunnerConfig{IdleRunners: 1, IdleDrainAfter: 150 * time.Millisecond, Docker: &DockerImage{Image: "test:latest"}},
		newMockProvisioner())
	defer s.Shutdown(context.Background())

	if _, err := s.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	waitForIdle(t, s, 1)
	name := s.Runners()[0].Name
	if err := s.HandleJobStarted(context.Background(), &scaleset.JobStarted{RunnerName: name}); err != nil {
		t.Fatalf("HandleJobStarted: %v", err)
	}
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	// The job outlasts the linger; the expiry it skips must not be the last.
	time.Sleep(300 * time.Millisecond)
	select {
	case <-s.AdmissionClosed():
		t.Fatal("idle linger closed admission mid-job")
	default:
	}
	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: name, Result: "succeeded"}); err != nil {
		t.Fatalf("HandleJobCompleted: %v", err)
	}
	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatalf("host never retired after its job; runners %v", s.Runners())
	}
}

func TestIdleRunnersBrokenSpareDoesNotKeepHostAwake(t *testing.T) {
	client := newMockClient()
	prov := newMockProvisioner()
	prov.startErr = errors.New("docker unavailable")
	s := NewScaler(noopLogger(), client, 1, 1, "test",
		&RunnerConfig{IdleRunners: 1, IdleDrainAfter: 300 * time.Millisecond, Docker: &DockerImage{Image: "test:latest"}}, prov)
	defer s.Shutdown(context.Background())

	// Each listener poll re-spawns the spare, and each one fails to start.
	deadline := time.Now().Add(3 * time.Second)
	for {
		select {
		case <-s.Drained():
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("failing spares kept restarting the idle clock")
		}
		if _, err := s.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
			t.Fatalf("HandleDesiredRunnerCount: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestIdleRunnersCancelledAssignmentRestartsLinger(t *testing.T) {
	client := newMockClient()
	s := NewScaler(noopLogger(), client, 1, 1, "test",
		&RunnerConfig{IdleRunners: 1, IdleDrainAfter: 150 * time.Millisecond, Docker: &DockerImage{Image: "test:latest"}},
		newMockProvisioner())
	defer s.Shutdown(context.Background())

	// The spare covers an assignment when the linger expires...
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	waitForIdle(t, s, 1)
	time.Sleep(300 * time.Millisecond)
	// ...then GitHub cancels it. Nothing else will ever remove a runner.
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("host never retired after its assignment was cancelled")
	}
}

func TestIdleRunnersSpareJITFailureIsNotFatal(t *testing.T) {
	client := newMockClient()
	client.jitErr = errors.New("GitHub unavailable")
	s := NewScaler(noopLogger(), client, 1, 1, "test",
		&RunnerConfig{IdleRunners: 1, Docker: &DockerImage{Image: "test:latest"}}, newMockProvisioner())
	defer s.Shutdown(context.Background())

	if _, err := s.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatalf("a failed spare must not end the listener: %v", err)
	}
	// An assignment without a runner is still an error the listener sees.
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err == nil {
		t.Fatal("expected an error when an assigned job cannot get a runner")
	}
}
