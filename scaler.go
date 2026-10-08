package outrunner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
	"github.com/google/uuid"
)

// ScaleSetClient is the subset of scaleset.Client that Scaler uses.
// Extracted as an interface for testability.
type ScaleSetClient interface {
	GenerateJitRunnerConfig(ctx context.Context, setting *scaleset.RunnerScaleSetJitRunnerSetting, scaleSetID int) (*scaleset.RunnerScaleSetJitRunnerConfig, error)
	RemoveRunner(ctx context.Context, runnerID int64) error
}

// Scaler implements listener.Scaler by provisioning real runner environments.
// Each runner gets its own goroutine that manages the full lifecycle:
// provisioning, waiting for job completion, stopping, and deregistration.
type Scaler struct {
	logger      *slog.Logger
	client      ScaleSetClient
	scaleSetID  int
	maxRunners  int
	namePrefix  string
	runner      *RunnerConfig
	provisioner Provisioner

	mu                    sync.Mutex
	runners               map[string]*RunnerState
	completedRunners      map[string]bool
	jobQueueTimes         map[string]time.Time
	completedJobInfo      []DrainJob
	completedJobs         int
	draining              bool
	drainReason           string
	drainFailed           bool
	drained               chan struct{}
	admissionClosed       chan struct{}
	drainedOnce           sync.Once
	idleSince             time.Time
	idleTimer             *time.Timer
	exitGrace             time.Duration
	cleanupRetry          time.Duration
	synchronizeAdmission  bool
	admissionSynchronized bool
	assignedJobs          int
	// servedSinceStatistics counts completions since assignedJobs was last
	// replaced. Only refill subtracts it; drain proof uses GitHub's figure.
	// The listener replaces assignedJobs in the same message as a completion,
	// so this matters only if cleanup wins that race.
	servedSinceStatistics int

	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
	wg              sync.WaitGroup
}

var _ listener.Scaler = (*Scaler)(nil)

// ScalerOption configures coordination with the message listener.
type ScalerOption func(*Scaler)

// WithAdmissionSynchronization requires a processed zero-capacity poll and no
// outstanding GitHub assignments before publishing a drain receipt.
func WithAdmissionSynchronization() ScalerOption {
	return func(s *Scaler) { s.synchronizeAdmission = true }
}

func NewScaler(logger *slog.Logger, client ScaleSetClient, scaleSetID, maxRunners int, namePrefix string, runner *RunnerConfig, prov Provisioner, options ...ScalerOption) *Scaler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Scaler{
		logger:           logger,
		client:           client,
		scaleSetID:       scaleSetID,
		maxRunners:       maxRunners,
		namePrefix:       namePrefix,
		runner:           runner,
		provisioner:      prov,
		runners:          make(map[string]*RunnerState),
		completedRunners: make(map[string]bool),
		jobQueueTimes:    make(map[string]time.Time),
		drained:          make(chan struct{}),
		admissionClosed:  make(chan struct{}),
		lifecycleCtx:     ctx,
		lifecycleCancel:  cancel,
		idleSince:        time.Now(),
		exitGrace:        30 * time.Second,
		cleanupRetry:     time.Second,
	}
	for _, option := range options {
		option(s)
	}
	// NewScaler is called after scale-set registration, including on zero-job hosts.
	s.mu.Lock()
	s.armIdleTimerLocked()
	s.mu.Unlock()
	return s
}

func (s *Scaler) HandleDesiredRunnerCount(ctx context.Context, count int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.assignedJobs = count
	s.servedSinceStatistics = 0
	current, err := s.reconcileLocked(ctx, count)
	s.reapIdleLocked()
	return current, err
}

// reconcileLocked spawns runners until the tracked set covers count assigned
// jobs, capped at maxRunners. Callers hold s.mu.
func (s *Scaler) reconcileLocked(ctx context.Context, count int) (int, error) {
	if s.draining && !s.synchronizeAdmission {
		return len(s.runners), nil
	}

	target := s.targetLocked(count)
	current := len(s.runners)

	s.logger.Debug("Desired runner count",
		slog.Int("requested", count),
		slog.Int("target", target),
		slog.Int("current", current),
	)

	for range target - current {
		name := fmt.Sprintf("%s-%s", s.namePrefix, uuid.NewString()[:8])

		jit, err := s.client.GenerateJitRunnerConfig(ctx,
			&scaleset.RunnerScaleSetJitRunnerSetting{Name: name},
			s.scaleSetID,
		)
		if err != nil {
			return len(s.runners), fmt.Errorf("generate JIT config: %w", err)
		}

		if jit == nil || jit.Runner == nil || jit.Runner.ID <= 0 || jit.EncodedJITConfig == "" {
			return len(s.runners), fmt.Errorf("JIT response is missing runner identity or configuration")
		}

		state := &RunnerState{
			Name:      name,
			RunnerID:  jit.Runner.ID,
			Phase:     RunnerProvisioning,
			CreatedAt: time.Now(),
			done:      make(chan struct{}),
		}
		s.runners[name] = state

		req := &RunnerRequest{
			Name:      name,
			JITConfig: jit.EncodedJITConfig,
			Runner:    s.runner,
		}

		s.logger.Info("Spawning runner",
			slog.String("name", name),
			slog.Int("runnerID", state.RunnerID),
		)

		s.wg.Add(1)
		go s.runRunnerLifecycle(state, req)
	}

	return len(s.runners), nil
}

func (s *Scaler) HandleJobStarted(ctx context.Context, jobInfo *scaleset.JobStarted) error {
	s.mu.Lock()
	state, exists := s.runners[jobInfo.RunnerName]
	if exists {
		state.Phase = RunnerRunning
		if !jobInfo.QueueTime.IsZero() {
			s.jobQueueTimes[jobInfo.RunnerName] = jobInfo.QueueTime
		}
	}
	s.mu.Unlock()

	s.logger.Info("Job started",
		slog.String("runnerName", jobInfo.RunnerName),
		slog.Int64("requestId", jobInfo.RunnerRequestID),
	)
	return nil
}

func (s *Scaler) HandleJobCompleted(ctx context.Context, jobInfo *scaleset.JobCompleted) error {
	s.logger.Info("Job completed",
		slog.String("runnerName", jobInfo.RunnerName),
		slog.Int64("runnerRequestId", jobInfo.RunnerRequestID),
		slog.String("jobId", jobInfo.JobID),
		slog.Int64("workflowRunId", jobInfo.WorkflowRunID),
		slog.String("result", jobInfo.Result),
	)

	s.mu.Lock()
	state, exists := s.runners[jobInfo.RunnerName]
	if !exists && jobInfo.RunnerName != "" {
		s.logger.Warn("Completion received for untracked runner; receipt accounting may be incomplete",
			slog.String("runnerName", jobInfo.RunnerName), slog.Int64("workflowRunId", jobInfo.WorkflowRunID))
	}
	if exists && !s.completedRunners[jobInfo.RunnerName] {
		queueTime := receiptQueueTime(
			jobInfo.QueueTime,
			s.jobQueueTimes[jobInfo.RunnerName],
			jobInfo.ScaleSetAssignTime,
		)
		s.completedRunners[jobInfo.RunnerName] = true
		s.completedJobInfo = append(s.completedJobInfo, DrainJob{
			RunnerName:         jobInfo.RunnerName,
			RunnerID:           jobInfo.RunnerID,
			RunnerRequestID:    jobInfo.RunnerRequestID,
			JobID:              jobInfo.JobID,
			JobWorkflowRef:     jobInfo.JobWorkflowRef,
			JobDisplayName:     jobInfo.JobDisplayName,
			WorkflowRunID:      jobInfo.WorkflowRunID,
			Repository:         jobInfo.OwnerName + "/" + jobInfo.RepositoryName,
			RequestLabels:      append([]string{}, jobInfo.RequestLabels...),
			Result:             jobInfo.Result,
			QueueTime:          queueTime,
			ScaleSetAssignTime: jobInfo.ScaleSetAssignTime,
			RunnerAssignTime:   jobInfo.RunnerAssignTime,
			FinishedAt:         jobInfo.FinishTime,
		})
		s.completedJobs++
		// The finished job no longer holds an assignment, but assignedJobs
		// predates its completion until the listener applies this message's
		// statistics, which it does right after this callback returns.
		s.servedSinceStatistics++
		s.idleSince = time.Now()
		if s.runner.MaxJobs > 0 && s.completedJobs >= s.runner.MaxJobs && !s.draining {
			s.draining = true
			close(s.admissionClosed)
			s.drainReason = "max_jobs"
			s.logger.Info("Maximum job count reached; stopping admission",
				slog.Int("completedJobs", s.completedJobs),
				slog.Int("maxJobs", s.runner.MaxJobs),
			)
			// No reap here: assignedJobs predates this message's statistics,
			// which HandleDesiredRunnerCount applies (and reaps on) next.
		}
	}
	s.mu.Unlock()

	if exists {
		state.SignalDone()
	}

	return nil
}

func receiptQueueTime(completed, started, assigned time.Time) time.Time {
	if !completed.IsZero() {
		return completed
	}
	if !started.IsZero() {
		return started
	}
	// GitHub can omit queueTime from both lifecycle messages. The scale-set
	// assignment is the latest safe queue bound and keeps the receipt's ordering
	// proof conservative.
	return assigned
}

// RequestDrain stops new admission. With listener synchronization enabled,
// assignments already accepted by GitHub are honored before terminal proof.
func (s *Scaler) RequestDrain() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.draining {
		return nil
	}
	s.draining = true
	close(s.admissionClosed)
	s.drainReason = "external"
	s.logger.Info("External drain requested; stopping admission",
		slog.Int("trackedRunners", len(s.runners)),
	)
	if len(s.runners) == 0 {
		return s.finishDrainLocked()
	}
	s.reapIdleLocked()
	return nil
}

// armIdleTimerLocked starts or resumes the remaining linger interval. Callers
// hold s.mu, the same lock used for admission and the terminal idle decision.
func (s *Scaler) armIdleTimerLocked() {
	if s.runner.IdleDrainAfter <= 0 || s.draining || s.drainFailed || s.lifecycleCtx.Err() != nil || s.workingLocked() != 0 {
		return
	}
	remaining := max(time.Duration(0), s.runner.IdleDrainAfter-time.Since(s.idleSince))
	if s.idleTimer != nil {
		s.idleTimer.Stop()
	}
	s.idleTimer = time.AfterFunc(remaining, s.checkIdleDrain)
}

func (s *Scaler) checkIdleDrain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runner.IdleDrainAfter <= 0 || s.draining || s.drainFailed || s.lifecycleCtx.Err() != nil || s.workingLocked() != 0 {
		return
	}
	// A previously scheduled callback may race a new job and its cleanup.
	// Always check the current clock under the admission lock.
	if time.Since(s.idleSince) < s.runner.IdleDrainAfter {
		s.armIdleTimerLocked()
		return
	}
	s.draining = true
	close(s.admissionClosed)
	s.drainReason = "idle"
	s.logger.Info("Idle linger expired; stopping admission")
	if len(s.runners) == 0 {
		_ = s.finishDrainLocked()
		return
	}
	// Pre-spawned spares are deregistered; the last removal finishes drain.
	s.reapIdleLocked()
}

// AdmissionClosed instructs the listener to advertise zero capacity. It does
// not mean an in-flight positive-capacity poll has finished yet.
func (s *Scaler) AdmissionClosed() <-chan struct{} { return s.admissionClosed }

// Drained closes after admission stops and every tracked runner has been
// successfully stopped and deregistered, with any configured receipt durable.
func (s *Scaler) Drained() <-chan struct{} {
	return s.drained
}

// Shutdown cancels all runner goroutines and waits for them to finish.
func (s *Scaler) Shutdown(ctx context.Context) {
	// Cancel before taking the lock: a refill holds s.mu across its JIT
	// request, and cancellation is what ends that request promptly.
	s.lifecycleCancel()
	s.mu.Lock()
	if s.idleTimer != nil {
		s.idleTimer.Stop()
	}
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info("All runners shut down")
	case <-ctx.Done():
		s.logger.Warn("Shutdown timed out, some runners may not be fully cleaned up")
	}
}

// RunnerSnapshot is a point-in-time copy of a runner's state, safe to read
// without holding the scaler's mutex.
type RunnerSnapshot struct {
	Name      string
	RunnerID  int
	Phase     RunnerPhase
	CreatedAt time.Time
	StartedAt time.Time
}

// Runners returns a snapshot of current runner states.
func (s *Scaler) Runners() []RunnerSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshots := make([]RunnerSnapshot, 0, len(s.runners))
	for _, state := range s.runners {
		snapshots = append(snapshots, RunnerSnapshot{
			Name:      state.Name,
			RunnerID:  state.RunnerID,
			Phase:     state.Phase,
			CreatedAt: state.CreatedAt,
			StartedAt: state.StartedAt,
		})
	}
	return snapshots
}

// runRunnerLifecycle manages the full lifecycle of a single runner.
// Runs in its own goroutine. Handles provisioning, waiting, stopping,
// deregistration, and map cleanup.
func (s *Scaler) runRunnerLifecycle(state *RunnerState, req *RunnerRequest) {
	defer s.wg.Done()
	cleaned := false
	defer func() { s.removeRunner(state.Name, cleaned) }()

	name := state.Name

	// 1. Provision
	if err := s.provisioner.Start(s.lifecycleCtx, req); err != nil {
		s.logger.Error("Failed to start runner",
			slog.String("name", name),
			slog.String("error", err.Error()),
		)
		// Start can fail after creation. Keep ownership until cleanup is proven.
		cleaned = s.cleanupRunner(state)
		return
	}

	s.mu.Lock()
	if state.Phase == RunnerRunning {
		// HandleJobStarted arrived during provisioning
		s.logger.Info("Runner provisioned (job already assigned)", slog.String("name", name))
	} else {
		state.Phase = RunnerIdle
		s.logger.Info("Runner provisioned", slog.String("name", name))
	}
	state.StartedAt = time.Now()
	// Admission may have closed while this runner was provisioning.
	s.reapIdleLocked()
	s.mu.Unlock()

	// The environment may disappear without any GitHub completion message.
	// Keep watching separate from the listener so a drained host cannot hold
	// a dead runner indefinitely. Cancellation joins the watcher before Stop.
	watchCtx, watchCancel := context.WithCancel(s.lifecycleCtx)
	var exited <-chan error
	watchDone := make(chan struct{})
	if watcher, ok := s.provisioner.(ExitWatcher); ok {
		result := make(chan error, 1)
		exited = result
		go func() {
			defer close(watchDone)
			result <- watcher.Wait(watchCtx, name)
		}()
	} else {
		close(watchDone)
	}
	// 2. Wait for completion signal, environment exit, or shutdown.
	select {
	case err := <-exited:
		if err != nil && s.lifecycleCtx.Err() == nil {
			// An API failure is not evidence of container exit. Continue waiting
			// for the listener; Docker's watcher retries transient failures.
			s.logger.Error("Runner exit observation failed", slog.String("name", name), slog.Any("error", err))
			select {
			case <-state.done:
			case <-s.lifecycleCtx.Done():
			}
		} else if s.lifecycleCtx.Err() == nil {
			s.logger.Warn("Runner environment exited", slog.String("name", name))
			// Normal ephemeral exits can arrive before JobCompleted. Give the
			// listener time to record the real job result; never invent one.
			timer := time.NewTimer(s.exitGrace)
			select {
			case <-state.done:
			case <-timer.C:
			case <-s.lifecycleCtx.Done():
			}
			timer.Stop()
		}
	case <-state.done:
		s.logger.Debug("Runner signaled done", slog.String("name", name))
	case <-s.lifecycleCtx.Done():
		s.logger.Debug("Runner shutdown requested", slog.String("name", name))
	}

	watchCancel()
	<-watchDone

	cleaned = s.cleanupRunner(state)
}

// Keep failed cleanup tracked and retry transient Docker/GitHub failures. Only
// shutdown can abandon the proof, in which case removeRunner fails the drain.
func (s *Scaler) cleanupRunner(state *RunnerState) bool {
	s.mu.Lock()
	state.Phase = RunnerStopping
	s.mu.Unlock()
	delay := s.cleanupRetry
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		err := s.provisioner.Stop(ctx, state.Name)
		cancel()
		if err == nil && s.deregisterRunner(state.Name, state.RunnerID) {
			return true
		}
		s.logger.Warn("Runner cleanup incomplete; retaining ownership for retry",
			slog.String("name", state.Name), slog.Any("error", err))
		timer := time.NewTimer(delay)
		select {
		case <-s.lifecycleCtx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
		delay = min(30*time.Second, delay*2)
	}
}

func (s *Scaler) removeRunner(name string, cleaned bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	servedJob := s.completedRunners[name]
	reaped := false
	if state, ok := s.runners[name]; ok {
		reaped = state.reaped
	}
	delete(s.runners, name)
	s.idleSince = time.Now()
	delete(s.completedRunners, name)
	delete(s.jobQueueTimes, name)
	if !cleaned {
		s.drainFailed = true
	}
	// A reaped runner provisioned successfully, so refilling after it cannot
	// spin the way a failed start could.
	if cleaned && (servedJob || reaped) {
		s.refillLocked()
	}
	s.armIdleTimerLocked()
	if s.draining && !s.drainFailed && len(s.runners) == 0 {
		_ = s.finishDrainLocked()
	}
}

// targetLocked is how many runners to track for count assigned jobs: one per
// assignment plus, while admission is open, idle_runners pre-registered
// spares so the next job starts without waiting for a container and runner
// registration. Spares never exceed what max_jobs can still use, so a host's
// last job does not leave an idle runner behind. Callers hold s.mu.
func (s *Scaler) targetLocked(count int) int {
	extra := 0
	if !s.draining && s.runner.IdleRunners > 0 {
		extra = s.runner.IdleRunners
		if s.runner.MaxJobs > 0 {
			extra = min(extra, max(0, s.runner.MaxJobs-s.completedJobs-count))
		}
	}
	return min(s.maxRunners, count+extra)
}

// pendingLocked is the number of assignments not yet started on a runner:
// GitHub's latest count, less completions since it arrived and runners with
// a job running. Callers hold s.mu.
func (s *Scaler) pendingLocked() int {
	running := 0
	for _, state := range s.runners {
		if state.Phase == RunnerRunning && !s.completedRunners[state.Name] {
			running++
		}
	}
	return max(0, s.assignedJobs-s.servedSinceStatistics-running)
}

// spareRunnersLocked splits the runners waiting for work into those no
// pending assignment needs. Ready idle runners cover assignments first, since
// GitHub hands a job to a runner that is already online. It returns how many
// provisioning runners are spare, and the spare idle runners. Callers hold s.mu.
func (s *Scaler) spareRunnersLocked() (provisioning int, idle []*RunnerState) {
	var ready []*RunnerState
	for _, state := range s.runners {
		if s.completedRunners[state.Name] || state.reaping {
			continue
		}
		switch state.Phase {
		case RunnerProvisioning:
			provisioning++
		case RunnerIdle:
			ready = append(ready, state)
		}
	}
	pending := s.pendingLocked()
	covered := min(pending, len(ready))
	return max(0, provisioning-(pending-covered)), ready[covered:]
}

// workingLocked counts runners that hold the host awake for idle linger:
// everything except spare runners when idle_runners pre-spawns them. Without
// idle_runners, any tracked runner counts, as before. Callers hold s.mu.
func (s *Scaler) workingLocked() int {
	if s.runner.IdleRunners <= 0 {
		return len(s.runners)
	}
	provisioning, idle := s.spareRunnersLocked()
	return len(s.runners) - provisioning - len(idle)
}

// refillTimeout bounds the JIT request a refill makes while holding s.mu.
const refillTimeout = 30 * time.Second

// refillLocked serves assignments that arrived while a runner that just
// finished a job still held the last slot. The listener re-evaluates capacity
// only when a message arrives, and GitHub's long poll can hold the next one
// for ~50s, so without this a job assigned during cleanup waits out the poll
// on an otherwise idle host. Like the listener's own nil-message path, this
// relies on GitHub's TotalAssignedJobs excluding completed jobs; refill only
// acts on those statistics ~50s sooner. Failed provisioning never refills here: the next
// listener message retries it, so a broken image cannot spin. Callers hold
// s.mu inside a tracked lifecycle goroutine, so the wait group is positive
// when Add runs.
func (s *Scaler) refillLocked() {
	if s.lifecycleCtx.Err() != nil || s.drainFailed {
		return
	}
	if s.draining && !s.synchronizeAdmission {
		return
	}
	pending := max(0, s.assignedJobs-s.servedSinceStatistics)
	if s.targetLocked(pending) <= len(s.runners) {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.lifecycleCtx.Err() != nil {
			return
		}
		pending := max(0, s.assignedJobs-s.servedSinceStatistics)
		ctx, cancel := context.WithTimeout(s.lifecycleCtx, refillTimeout)
		defer cancel()
		if _, err := s.reconcileLocked(ctx, pending); err != nil {
			// The next listener message retries with fresh statistics.
			s.logger.Warn("Refill after cleanup failed", slog.String("error", err.Error()))
		}
	}()
}

// reapTimeout bounds one idle runner's deregistration request.
const reapTimeout = 10 * time.Second

// reapIdleLocked deregisters provisioned runners that no assignment needs once
// admission has closed. Nothing else removes an idle runner, so without this a
// draining host whose assignment was cancelled or requeued elsewhere never
// drains. GitHub refuses to remove a runner that has taken a job
// (JobStillRunningError), so deregistration comes first and the container is
// stopped only once GitHub agrees the runner is idle. Callers hold s.mu.
func (s *Scaler) reapIdleLocked() {
	if !s.draining || s.drainFailed || s.lifecycleCtx.Err() != nil {
		return
	}
	_, idle := s.spareRunnersLocked()
	for _, state := range idle {
		state.reaping = true
		s.wg.Add(1)
		go s.reapRunner(state)
	}
}

func (s *Scaler) reapRunner(state *RunnerState) {
	defer s.wg.Done()
	ctx, cancel := context.WithTimeout(s.lifecycleCtx, reapTimeout)
	err := s.client.RemoveRunner(ctx, int64(state.RunnerID))
	cancel()

	s.mu.Lock()
	defer s.mu.Unlock()
	_, watched := s.provisioner.(ExitWatcher)
	switch {
	case err == nil:
		if state.Phase != RunnerIdle {
			// JobStarted arrived while the request was in flight. GitHub
			// accepted anyway; let the job's own messages and the exit
			// watcher finish this runner rather than stopping it here.
			s.logger.Warn("Reaped runner had already started a job", slog.String("name", state.Name))
			return
		}
		s.logger.Info("Reaped idle runner on a draining host", slog.String("name", state.Name))
		// Stays reaping so no later pass counts it as idle again. The
		// lifecycle goroutine stops the container; its own deregistration
		// then finds the runner gone, which counts as success.
		state.reaped = true
		state.Phase = RunnerStopping
		state.SignalDone()
	case errors.Is(err, scaleset.RunnerNotFoundError) && watched:
		// Either an earlier request succeeded and its response was lost, or
		// the ephemeral runner served a job and deregistered itself before
		// its messages arrived. Its container exits on its own either way;
		// the exit watcher and the job's messages record what happened.
		s.logger.Info("Idle runner already deregistered; waiting for its exit", slog.String("name", state.Name))
	case errors.Is(err, scaleset.RunnerNotFoundError):
		state.reaped = true
		state.Phase = RunnerStopping
		state.SignalDone()
	case errors.Is(err, scaleset.JobStillRunningError):
		state.reaping = false
		s.logger.Info("Idle runner took a job before it could be reaped", slog.String("name", state.Name))
	default:
		state.reaping = false
		// The next listener message retries.
		s.logger.Warn("Idle runner reap failed", slog.String("name", state.Name), slog.String("error", err.Error()))
	}
}

// finishDrainLocked publishes the terminal proof while s.mu is held.
func (s *Scaler) finishDrainLocked() error {
	if s.synchronizeAdmission && (!s.admissionSynchronized || s.assignedJobs != 0 || len(s.runners) != 0) {
		return nil
	}

	if s.drainFailed {
		return fmt.Errorf("drain cannot prove successful cleanup")
	}
	if s.runner.DrainReceipt != nil {
		version := drainReceiptVersionMaxJobs
		reason := ""
		if s.synchronizeAdmission {
			version = 4
			reason = s.drainReason
		}
		if s.drainReason == "external" || s.drainReason == "idle" {
			reason = s.drainReason
			if s.completedJobs == 0 && !s.synchronizeAdmission {
				version = drainReceiptVersionExternal
			}
		}
		receipt := DrainReceipt{
			Version:       version,
			Status:        "drained",
			ScaleSet:      s.namePrefix,
			CompletedJobs: s.completedJobs,
			MaxJobs:       s.runner.MaxJobs,
			DrainedAt:     time.Now().UTC(),
			Identity:      s.runner.DrainReceipt.Identity,
			Jobs:          append([]DrainJob{}, s.completedJobInfo...),
			Reason:        reason,
		}
		if err := writeDrainReceipt(s.runner.DrainReceipt, receipt); err != nil {
			s.drainFailed = true
			s.logger.Error("Failed to write drain receipt", slog.String("error", err.Error()))
			return err
		}
		s.logger.Info("Drain receipt written", slog.String("path", s.runner.DrainReceipt.Path))
	}
	s.drainedOnce.Do(func() { close(s.drained) })
	return nil
}

func (s *Scaler) deregisterRunner(name string, runnerID int) bool {
	if runnerID == 0 {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.client.RemoveRunner(ctx, int64(runnerID)); err != nil && !errors.Is(err, scaleset.RunnerNotFoundError) {
		s.logger.Warn("Failed to deregister runner",
			slog.String("name", name),
			slog.Int("runnerID", runnerID),
			slog.String("error", err.Error()),
		)
		return false
	}
	return true
}
