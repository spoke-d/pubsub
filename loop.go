package pubsub

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spoke-d/task"
	"github.com/spoke-d/task/tomb"
)

// Loop takes a task.Func and iterates until cancel is called.
// This is useful for exhausting a subscriber without caring about a
// schedule or interval.
func Loop(fn task.Func) (func() error, error) {
	tomb := tomb.New()
	if err := tomb.Go(func(ctx context.Context) error {
		for {
			if err := fn(ctx); err != nil {
				return errors.WithStack(err)
			}
		}
	}); err != nil {
		return func() error { return nil }, errors.WithStack(err)
	}
	return func() error {
		return tomb.Kill(nil)
	}, nil
}
