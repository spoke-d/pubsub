package pubsub

import (
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spoke-d/task"
)

// Multiplexer forwards multiple subscribers to one singular hub.
func Multiplexer(hub *Hub, subs ...*Subscriber) func(time.Duration) error {
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
