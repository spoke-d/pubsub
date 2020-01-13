package pubsub

import (
	"testing"
	"time"
)

//go:generate mockgen -package pubsub -destination task_mock_test.go github.com/spoke-d/task BackoffOptions

func waitForEventToBeSent(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("event was not dispatched in time")
	}
}
