package outrunner

import (
	"fmt"
	"sync"
	"time"
)

// RunnerPhase represents the current lifecycle phase of a runner.
type RunnerPhase int

const (
	RunnerProvisioning RunnerPhase = iota
	RunnerIdle
	RunnerRunning
	RunnerStopping
)

func (p RunnerPhase) String() string {
	switch p {
	case RunnerProvisioning:
		return "provisioning"
	case RunnerIdle:
		return "idle"
	case RunnerRunning:
		return "running"
	case RunnerStopping:
		return "stopping"
	default:
		return fmt.Sprintf("unknown(%d)", int(p))
	}
}

// RunnerState holds the full state of a single runner instance.
type RunnerState struct {
	Name      string
	RunnerID  int // from GenerateJitRunnerConfig().Runner.ID
	Phase     RunnerPhase
	CreatedAt time.Time
	StartedAt time.Time // when Start() completed (provisioning finished)

	// reaping is set while an idle runner's deregistration is in flight and
	// stays set once GitHub accepts it. reaped records that acceptance, so the
	// runner's removal refills like a served one. Callers hold the scaler's
	// mutex.
	reaping bool
	reaped  bool

	done     chan struct{}
	doneOnce sync.Once
}

// SignalDone closes the done channel, signaling the runner goroutine to stop.
// Safe to call multiple times.
func (r *RunnerState) SignalDone() {
	r.doneOnce.Do(func() { close(r.done) })
}
