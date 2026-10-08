package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/actions/scaleset"
)

func TestLabelsMatch(t *testing.T) {
	label := func(names ...string) []scaleset.Label {
		var labels []scaleset.Label
		for _, n := range names {
			labels = append(labels, scaleset.Label{Name: n, Type: "User"})
		}
		return labels
	}

	tests := []struct {
		name     string
		existing []scaleset.Label
		desired  []scaleset.Label
		want     bool
	}{
		{"identical", label("linux", "x64"), label("linux", "x64"), true},
		{"same set different order", label("x64", "linux"), label("linux", "x64"), true},
		{"different labels", label("linux"), label("windows"), false},
		{"extra label", label("linux", "x64"), label("linux"), false},
		{"missing label", label("linux"), label("linux", "x64"), false},
		{"both empty", nil, nil, true},
		{"existing empty", nil, label("linux"), false},
		{"desired empty", label("linux"), nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := labelsMatch(tt.existing, tt.desired)
			if got != tt.want {
				t.Errorf("labelsMatch() = %v, want %v", got, tt.want)
			}
		})
	}
}

type fakeRegistry struct {
	ref       *scaleset.RunnerReference
	lookupErr error
	removeErr error
	removed   []int64
}

func (f *fakeRegistry) GetRunnerByName(context.Context, string) (*scaleset.RunnerReference, error) {
	return f.ref, f.lookupErr
}

func (f *fakeRegistry) RemoveRunner(_ context.Context, id int64) error {
	f.removed = append(f.removed, id)
	return f.removeErr
}

func TestOrphanReleaser(t *testing.T) {
	ref := &scaleset.RunnerReference{ID: 42, Name: "set-abc"}
	cases := []struct {
		name        string
		registry    fakeRegistry
		wantRelease bool
		wantErr     bool
		wantRemoved bool
	}{
		{name: "not registered", registry: fakeRegistry{}, wantRelease: true},
		{name: "deregistered while idle", registry: fakeRegistry{ref: ref}, wantRelease: true, wantRemoved: true},
		{name: "already gone", registry: fakeRegistry{ref: ref, removeErr: fmt.Errorf("x: %w", scaleset.RunnerNotFoundError)}, wantRelease: true, wantRemoved: true},
		{name: "holds a job", registry: fakeRegistry{ref: ref, removeErr: fmt.Errorf("x: %w", scaleset.JobStillRunningError)}, wantRemoved: true},
		{name: "remove fails", registry: fakeRegistry{ref: ref, removeErr: errors.New("unavailable")}, wantErr: true, wantRemoved: true},
		{name: "lookup fails", registry: fakeRegistry{lookupErr: errors.New("unavailable")}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			release, err := orphanReleaser(&tc.registry)(context.Background(), "set-abc")
			if release != tc.wantRelease || (err != nil) != tc.wantErr {
				t.Fatalf("release=%v err=%v, want release=%v err=%v", release, err, tc.wantRelease, tc.wantErr)
			}
			if removed := len(tc.registry.removed) == 1 && tc.registry.removed[0] == 42; removed != tc.wantRemoved {
				t.Fatalf("removed=%v, want %v", tc.registry.removed, tc.wantRemoved)
			}
		})
	}
}
