package outrunner

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/actions/scaleset"
)

type drainPollClient struct {
	get func(context.Context, int, int) (*scaleset.RunnerScaleSetMessage, error)
}

func (c *drainPollClient) GetMessage(ctx context.Context, id, capacity int) (*scaleset.RunnerScaleSetMessage, error) {
	return c.get(ctx, id, capacity)
}
func (*drainPollClient) DeleteMessage(context.Context, int) error { return nil }
func (*drainPollClient) Session() scaleset.RunnerScaleSetSession {
	return scaleset.RunnerScaleSetSession{}
}

func assertNotDrained(t *testing.T, s *Scaler) {
	t.Helper()
	select {
	case <-s.Drained():
		t.Fatal("published drain before assignment handoff completed")
	default:
	}
}
func waitRunner(t *testing.T, s *Scaler, previous string) (string, int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		for name, state := range s.runners {
			if name != previous {
				id := state.RunnerID
				s.mu.Unlock()
				return name, id
			}
		}
		s.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatal("runner was not provisioned")
	return "", 0
}

func TestDrainWaitsForInFlightAssignmentAndZeroCapacityPoll(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "receipt.json")
	s := NewScaler(noopLogger(), newMockClient(), 1, 1, "test", &RunnerConfig{MaxJobs: 1, DrainReceipt: &DrainReceiptConfig{Path: path}}, newMockProvisioner(), WithAdmissionSynchronization())
	defer s.Shutdown(ctx)
	entered, release := make(chan struct{}), make(chan struct{})
	client := &drainPollClient{get: func(_ context.Context, _, capacity int) (*scaleset.RunnerScaleSetMessage, error) {
		if capacity != 1 {
			t.Errorf("initial capacity=%d", capacity)
		}
		close(entered)
		<-release
		return &scaleset.RunnerScaleSetMessage{Statistics: &scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 1}}, nil
	}}
	d := NewDrainSession(client, s)
	polled := make(chan struct{})
	go func() { _, _ = d.GetMessage(ctx, 0, 1); close(polled) }()
	<-entered
	if err := s.RequestDrain(); err != nil {
		t.Fatal(err)
	}
	assertNotDrained(t, s)
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("receipt exists before poll returns")
	}
	close(release)
	<-polled
	if _, err := d.HandleDesiredRunnerCount(ctx, 1); err != nil {
		t.Fatal(err)
	}
	name, id := waitRunner(t, s, "")
	assertNotDrained(t, s)
	client.get = func(_ context.Context, _, capacity int) (*scaleset.RunnerScaleSetMessage, error) {
		if capacity != 0 {
			t.Errorf("draining capacity=%d", capacity)
		}
		return nil, nil
	}
	_, _ = d.GetMessage(ctx, 1, 1)
	_, _ = d.HandleDesiredRunnerCount(ctx, 1)
	assertNotDrained(t, s)
	_ = d.HandleJobCompleted(ctx, &scaleset.JobCompleted{RunnerName: name, RunnerID: id, Result: "succeeded"})
	_, _ = d.HandleDesiredRunnerCount(ctx, 0)
	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("drain never completed")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var receipt DrainReceipt
	if err = json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Version != 4 || receipt.CompletedJobs != 1 || receipt.Reason != "external" {
		t.Fatalf("bad receipt: %+v", receipt)
	}
}

func TestMaxJobsDrainHonorsAssignmentBeyondThreshold(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "receipt.json")
	s := NewScaler(noopLogger(), newMockClient(), 1, 1, "test", &RunnerConfig{MaxJobs: 1, DrainReceipt: &DrainReceiptConfig{Path: path}}, newMockProvisioner(), WithAdmissionSynchronization())
	defer s.Shutdown(ctx)
	d := NewDrainSession(&drainPollClient{get: func(_ context.Context, _, capacity int) (*scaleset.RunnerScaleSetMessage, error) {
		if capacity != 0 {
			t.Errorf("draining capacity=%d", capacity)
		}
		return nil, nil
	}}, s)
	_, _ = d.HandleDesiredRunnerCount(ctx, 1)
	name, id := waitRunner(t, s, "")
	_ = d.HandleJobCompleted(ctx, &scaleset.JobCompleted{RunnerName: name, RunnerID: id, Result: "succeeded"})
	// The same message reports another accepted assignment. Local cleanup may
	// finish before the next callback; neither ordering may publish a receipt.
	_, _ = d.HandleDesiredRunnerCount(ctx, 1)
	assertNotDrained(t, s)
	_, _ = d.GetMessage(ctx, 1, 1)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, _ = d.HandleDesiredRunnerCount(ctx, 1)
		s.mu.Lock()
		_, oldPresent := s.runners[name]
		current := len(s.runners)
		s.mu.Unlock()
		if !oldPresent && current == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	next, nextID := waitRunner(t, s, name)
	assertNotDrained(t, s)
	_ = d.HandleJobCompleted(ctx, &scaleset.JobCompleted{RunnerName: next, RunnerID: nextID, Result: "succeeded"})
	_, _ = d.HandleDesiredRunnerCount(ctx, 0)
	select {
	case <-s.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("drain never completed")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var receipt DrainReceipt
	if err = json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Version != 4 || receipt.CompletedJobs != 2 || receipt.MaxJobs != 1 || receipt.Reason != "max_jobs" {
		t.Fatalf("bad receipt: %+v", receipt)
	}
}

func TestFailedZeroCapacityPollCannotProveDrain(t *testing.T) {
	ctx := context.Background()
	s := NewScaler(noopLogger(), newMockClient(), 1, 1, "test", &RunnerConfig{}, newMockProvisioner(), WithAdmissionSynchronization())
	defer s.Shutdown(ctx)
	d := NewDrainSession(&drainPollClient{get: func(context.Context, int, int) (*scaleset.RunnerScaleSetMessage, error) {
		return nil, errors.New("network unavailable")
	}}, s)
	_ = s.RequestDrain()
	if _, err := d.GetMessage(ctx, 0, 1); err == nil {
		t.Fatal("expected poll failure")
	}
	_, _ = d.HandleDesiredRunnerCount(ctx, 0)
	assertNotDrained(t, s)
}
