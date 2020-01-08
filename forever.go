package pubsub

import (
	"time"

	"github.com/spoke-d/task"
)

const (
	// Interval describes how frequently we should be scanning for new messages
	// from the hub.
	Interval time.Duration = time.Millisecond * 10
)

// Forever will attempt subscribe to a hub forever.
//
// This is a convenience around starting a task that will always consume
// messages when publishing events.
func Forever(topic string, handler func(string, interface{})) (*Hub, func(time.Duration) error) {
	hub := New()
	sub := hub.Subscribe(topic, handler)
	stop, _ := task.Start(sub.Run(Interval))
	return hub, stop
}
