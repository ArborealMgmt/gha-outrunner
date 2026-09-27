package outrunner

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/actions/scaleset"
)

func idleTestScaler(t *testing.T, prov *mockProvisioner) (*Scaler, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "receipt.json")
	s := NewScaler(noopLogger(), newMockClient(), 1, 1, "idle-test", &RunnerConfig{
		IdleDrainAfter: time.Minute, MaxJobs: 5,
		DrainReceipt: &DrainReceiptConfig{Path: path},
	}, prov)
	t.Cleanup(func() { s.Shutdown(context.Background()) })
	return s, path
}

func readIdleReceipt(t *testing.T, path string) DrainReceipt {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var r DrainReceipt
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatal(err)
	}
	if r.Reason != "idle" {
		t.Fatalf("reason = %q", r.Reason)
	}
	return r
}

func TestIdleDrainRetiresZeroJobHost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, path := idleTestScaler(t, newMockProvisioner())
		time.Sleep(time.Minute - time.Second)
		synctest.Wait()
		select {
		case <-s.Drained():
			t.Fatal("early drain")
		default:
		}
		time.Sleep(time.Second)
		synctest.Wait()
		select {
		case <-s.Drained():
		default:
			t.Fatal("not drained")
		}
		r := readIdleReceipt(t, path)
		if r.Version != 2 || r.CompletedJobs != 0 || len(r.Jobs) != 0 {
			t.Fatalf("receipt: %+v", r)
		}
		if n, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil || n != 0 {
			t.Fatalf("admission reopened: %d, %v", n, err)
		}
	})
}

func TestIdleDrainWaitsForTrackedRunnerAndRestartsAfterJob(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, path := idleTestScaler(t, newMockProvisioner())
		time.Sleep(50 * time.Second)
		if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		select {
		case <-s.Drained():
			t.Fatal("drained with live JIT runner")
		default:
		}
		name := s.Runners()[0].Name
		if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: name}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		// An old callback must observe the new completion time.
		s.checkIdleDrain()
		time.Sleep(time.Minute - time.Second)
		synctest.Wait()
		select {
		case <-s.Drained():
			t.Fatal("did not restart linger")
		default:
		}
		time.Sleep(time.Second)
		synctest.Wait()
		r := readIdleReceipt(t, path)
		if r.Version != 3 || r.CompletedJobs != 1 || len(r.Jobs) != 1 {
			t.Fatalf("receipt: %+v", r)
		}
	})
}

func TestIdleDrainFailureBeforeDeadlineSuppressesReceipt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		prov := newMockProvisioner()
		prov.stopErr = errors.New("stop failed")
		s, path := idleTestScaler(t, prov)
		if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: s.Runners()[0].Name}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		select {
		case <-s.Drained():
			t.Fatal("failed cleanup reported drained")
		default:
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("receipt exists: %v", err)
		}
		if err := s.RequestDrain(); err != nil {
			t.Fatal(err)
		}
		if len(s.Runners()) != 1 {
			t.Fatal("failed cleanup lost ownership")
		}
		select {
		case <-s.Drained():
			t.Fatal("external drain bypassed pending cleanup")
		default:
		}
	})
}

func TestIdleDrainShutdownStopsTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, path := idleTestScaler(t, newMockProvisioner())
		s.Shutdown(context.Background())
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("shutdown published idle receipt: %v", err)
		}
	})
}

func TestIdleDrainAdmissionRace(t *testing.T) {
	for range 100 {
		s, path := idleTestScaler(t, newMockProvisioner())
		s.mu.Lock()
		s.idleSince = time.Now().Add(-2 * time.Minute)
		s.mu.Unlock()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); s.checkIdleDrain() }()
		go func() { defer wg.Done(); _, _ = s.HandleDesiredRunnerCount(context.Background(), 1) }()
		wg.Wait()
		s.mu.Lock()
		draining, count := s.draining, len(s.runners)
		s.mu.Unlock()
		if draining && count != 0 {
			t.Fatal("idle receipt raced admission")
		}
		if draining {
			readIdleReceipt(t, path)
		} else if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("receipt with admitted runner: %v", err)
		}
		s.Shutdown(context.Background())
	}
}

func TestLoadConfigIdleDrainAfter(t *testing.T) {
	for _, tc := range []struct {
		value string
		valid bool
		want  time.Duration
	}{
		{"5m", true, 5 * time.Minute}, {"0s", true, 0}, {"-1s", false, 0}, {"bogus", false, 0},
	} {
		t.Run(tc.value, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yml")
			if err := os.WriteFile(path, []byte("runners:\n  test:\n    labels: [linux]\n    idle_drain_after: "+tc.value+"\n    docker:\n      image: runner:test\n"), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			if !tc.valid {
				if err == nil {
					t.Fatal("invalid duration accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Runners["test"].IdleDrainAfter != tc.want {
				t.Fatal("duration mismatch")
			}
		})
	}
}
