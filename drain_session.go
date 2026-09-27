package outrunner

import (
	"context"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
)

// DrainSession binds a listener's polls and scaler callbacks. The SDK invokes
// these serially. In-flight polls retain their original capacity; their accepted
// assignments must still be served. Only a later successful zero-capacity poll,
// processed through HandleDesiredRunnerCount, fences terminal drain proof.
type DrainSession struct {
	listener.Client
	*Scaler
	zeroCapacityPoll bool
}

func NewDrainSession(client listener.Client, scaler *Scaler) *DrainSession {
	return &DrainSession{Client: client, Scaler: scaler}
}

func (d *DrainSession) GetMessage(ctx context.Context, lastID, capacity int) (*scaleset.RunnerScaleSetMessage, error) {
	select {
	case <-d.AdmissionClosed():
		capacity = 0
	default:
	}
	message, err := d.Client.GetMessage(ctx, lastID, capacity)
	if err == nil && capacity == 0 {
		d.zeroCapacityPoll = true
	}
	return message, err
}

func (d *DrainSession) HandleDesiredRunnerCount(ctx context.Context, count int) (int, error) {
	current, err := d.Scaler.HandleDesiredRunnerCount(ctx, count)
	if err != nil {
		return current, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.zeroCapacityPoll {
		d.admissionSynchronized = true
	}
	if d.draining && len(d.runners) == 0 {
		err = d.finishDrainLocked()
	}
	return current, err
}
