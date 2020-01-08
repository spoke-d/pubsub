package pubsub

import (
	"testing"
	"time"
)

func waitForEventToBeSent(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("event was not dispatched in time")
	}
}
