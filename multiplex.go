package pubsub

import (
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spoke-d/task"
)

// Sub defines a type that the multiplexer can run.
type Sub interface {

	// Run creates a task and a schedule to perform the consumption of messages
	// sent to the subscriber from the origin.
	Run(interval time.Duration) (task.Func, task.Schedule)
}

// Multiplexer forwards multiple subscribers to one singular hub.
func Multiplexer(hub *Hub, subs ...Sub) func(time.Duration) error {
	cleanups := make([]func(time.Duration) error, len(subs))
	for i, sub := range subs {
		cleanups[i], _ = task.Start(sub.Run(Interval))
	}
	return func(dur time.Duration) error {
		var errs []string
		for _, cancel := range cleanups {
			if err := cancel(dur); err != nil {
				errs = append(errs, err.Error())
			}
		}
		if len(errs) == 0 {
			return nil
		}
		return errors.Errorf(strings.Join(errs, ", "))
	}
}

// Forward all messages from one hub to another.
// Useful to cross boundaries.
func Forward(hub *Hub, other *Hub) func(time.Duration) error {
	sub := hub.SubscribeMatch(Any(), func(topic string, data interface{}) {
		done := other.Publish(topic, data)
		select {
		case <-done:
		case <-time.After(time.Millisecond * 10):
		}
	})

	cleanup, _ := task.Start(sub.Run(Interval))
	return cleanup
}
