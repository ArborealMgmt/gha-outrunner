package outrunner

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	drainReceiptVersionMaxJobs  = 3
	drainReceiptVersionExternal = 2
)

// DrainJob identifies one completed GitHub job represented by a drain receipt.
type DrainJob struct {
	RunnerName         string    `json:"runner_name"`
	RunnerID           int       `json:"runner_id"`
	RunnerRequestID    int64     `json:"runner_request_id"`
	JobID              string    `json:"job_id"`
	JobWorkflowRef     string    `json:"job_workflow_ref"`
	JobDisplayName     string    `json:"job_display_name"`
	WorkflowRunID      int64     `json:"workflow_run_id"`
	Repository         string    `json:"repository"`
	RequestLabels      []string  `json:"request_labels"`
	Result             string    `json:"result"`
	QueueTime          time.Time `json:"queue_time"`
	ScaleSetAssignTime time.Time `json:"scale_set_assign_time"`
	RunnerAssignTime   time.Time `json:"runner_assign_time"`
	FinishedAt         time.Time `json:"finished_at"`
}

// DrainReceipt is written only after admission is closed and all tracked
// runners have been successfully stopped and deregistered.
type DrainReceipt struct {
	Version       int               `json:"version"`
	Status        string            `json:"status"`
	ScaleSet      string            `json:"scale_set"`
	CompletedJobs int               `json:"completed_jobs"`
	MaxJobs       int               `json:"max_jobs"`
	DrainedAt     time.Time         `json:"drained_at"`
	Identity      map[string]string `json:"identity,omitempty"`
	Jobs          []DrainJob        `json:"jobs"`
	Reason        string            `json:"reason,omitempty"`
}

// PrepareDrainReceipts removes receipts from an earlier process before any
// listener starts. This prevents a restarted service from exposing stale proof.
func PrepareDrainReceipts(config *Config) error {
	for name, runner := range config.Runners {
		if runner.DrainReceipt == nil {
			continue
		}
		path := runner.DrainReceipt.Path
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect drain receipt for runner %q: %w", name, err)
		}
		if info.IsDir() {
			return fmt.Errorf("drain receipt for runner %q is a directory", name)
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove stale drain receipt for runner %q: %w", name, err)
		}
	}
	return nil
}

func writeDrainReceipt(config *DrainReceiptConfig, receipt DrainReceipt) error {
	directory := filepath.Dir(config.Path)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("create drain receipt directory: %w", err)
	}

	temporary, err := os.CreateTemp(directory, ".drain-receipt-*")
	if err != nil {
		return fmt.Errorf("create drain receipt: %w", err)
	}
	temporaryName := temporary.Name()
	keep := false
	defer func() {
		_ = temporary.Close()
		if !keep {
			_ = os.Remove(temporaryName)
		}
	}()

	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("set drain receipt permissions: %w", err)
	}
	encoder := json.NewEncoder(temporary)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(receipt); err != nil {
		return fmt.Errorf("encode drain receipt: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync drain receipt: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close drain receipt: %w", err)
	}
	if err := os.Rename(temporaryName, config.Path); err != nil {
		return fmt.Errorf("publish drain receipt: %w", err)
	}
	keep = true

	dir, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open drain receipt directory: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync drain receipt directory: %w", err)
	}
	return nil
}
