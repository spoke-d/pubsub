package pubsub

import (
	"fmt"
	"strings"
	"testing"
	"testing/quick"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/spoke-d/task"
)

func TestHub(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		hub := New()
		defer hub.Close()

		done := hub.Publish("topic", struct{}{})
		waitForEventToBeSent(t, done)
	})
}

func TestHubClose(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		hub := New()

		hub.Close()

		done := hub.Publish("topic", struct{}{})
		waitForEventToBeSent(t, done)
	})

	t.Run("close after subscribe", func(t *testing.T) {
		var called bool

		hub := New()

		sub := hub.Subscribe("topic", func(topic string, data interface{}) {
			called = true
		})

		stop, _ := task.Start(sub.Run(time.Millisecond))
		defer stop(time.Second)

		hub.Close()

		done := hub.Publish("topic", struct{}{})
		waitForEventToBeSent(t, done)

		if expected, actual := false, called; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestHubSubscribe(t *testing.T) {
	t.Parallel()

	t.Run("one subscriber", func(t *testing.T) {
		var called bool

		hub := New()
		defer hub.Close()

		sub := hub.Subscribe("topic", func(topic string, data interface{}) {
			if expected, actual := "topic", topic; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if expected, actual := true, data == nil; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			called = true
		})

		stop, _ := task.Start(sub.Run(time.Millisecond))
		defer stop(time.Second)

		done := hub.Publish("topic", nil)
		waitForEventToBeSent(t, done)

		if expected, actual := true, called; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("publish completer waits", func(t *testing.T) {
		wait := make(chan struct{})

		hub := New()
		defer hub.Close()

		sub := hub.Subscribe("topic", func(topic string, data interface{}) {
			<-wait
		})

		stop, _ := task.Start(sub.Run(time.Millisecond))
		defer stop(time.Second)

		done := hub.Publish("topic", nil)

		select {
		case <-done:
			t.Fatal("waiting for publish failed")
		case <-time.After(time.Millisecond * 10):
			// Should be enough time to wait for the handler
		}

		close(wait)
		waitForEventToBeSent(t, done)
	})

	t.Run("publish not blocked by subscribers", func(t *testing.T) {
		wait := make(chan struct{})

		hub := New()
		defer hub.Close()

		sub := hub.Subscribe("topic", func(topic string, data interface{}) {
			<-wait
		})

		stop, _ := task.Start(sub.Run(time.Millisecond))
		defer stop(time.Second)

		var last <-chan struct{}
		for i := 0; i < 100; i++ {
			last = hub.Publish(fmt.Sprintf("topic.%d", i), nil)
		}

		close(wait)
		waitForEventToBeSent(t, last)
	})

	t.Run("publish executes in order", func(t *testing.T) {
		var calls []string

		hub := New()
		defer hub.Close()

		sub := hub.SubscribeMatch(StartsWith("topic."), func(topic string, data interface{}) {
			calls = append(calls, topic)
		})

		stop, _ := task.Start(sub.Run(time.Millisecond))
		defer stop(time.Second)

		var last <-chan struct{}
		for i := 0; i < 4; i++ {
			last = hub.Publish(fmt.Sprintf("topic.%d", i), nil)
		}

		waitForEventToBeSent(t, last)
		if expected, actual := 4, len(calls); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		for i := 0; i < 4; i++ {
			if expected, actual := fmt.Sprintf("topic.%d", i), calls[i]; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
		}
	})

	t.Run("publish executes all", func(t *testing.T) {
		var calls [2][]string

		hub := New()
		defer hub.Close()

		for i := 0; i < 2; i++ {
			stop := func(i int) func(time.Duration) error {
				sub := hub.SubscribeMatch(StartsWith("topic."), func(topic string, data interface{}) {
					calls[i] = append(calls[i], topic)
				})

				stop, _ := task.Start(sub.Run(time.Millisecond))
				return stop
			}(i)
			defer stop(time.Second)
		}

		var last <-chan struct{}
		for i := 0; i < 4; i++ {
			last = hub.Publish(fmt.Sprintf("topic.%d", i), nil)
		}

		waitForEventToBeSent(t, last)
		if expected, actual := 2, len(calls); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		for i := 0; i < 2; i++ {
			for k := 0; k < 4; k++ {
				if expected, actual := fmt.Sprintf("topic.%d", k), calls[i][k]; expected != actual {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}
			}
		}
	})

	t.Run("publish with no consequences", func(t *testing.T) {
		hub := New()
		defer hub.Close()

		var called int
		sub := hub.SubscribeMatch(Any(), func(topic string, data interface{}) {
			called++
		})
		stop, _ := task.Start(sub.Run(time.Millisecond))
		defer stop(time.Second)

		var failed int
		var last <-chan struct{}
		for n := 0; n < 10; n++ {
			last = hub.Publish("topic", nil)
			go func(done <-chan struct{}) {
				select {
				case <-done:
				case <-time.After(time.Second):
					failed++
				}
			}(last)
		}

		waitForEventToBeSent(t, last)

		time.Sleep(time.Second)

		if expected, actual := 10, called; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := 0, failed; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestHubUnsubscribe(t *testing.T) {
	t.Parallel()

	t.Run("unsubscribe", func(t *testing.T) {
		var called bool

		hub := New()
		defer hub.Close()

		sub := hub.Subscribe("topic", func(topic string, data interface{}) {
			called = true
		})

		stop, _ := task.Start(sub.Run(time.Millisecond))
		defer stop(time.Second)

		err := sub.Unsubscribe()
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		done := hub.Publish("topic", struct{}{})
		waitForEventToBeSent(t, done)

		if expected, actual := false, called; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("unsubscribe twice", func(t *testing.T) {
		var called bool

		hub := New()
		defer hub.Close()

		sub := hub.Subscribe("topic", func(topic string, data interface{}) {
			called = true
		})

		stop, _ := task.Start(sub.Run(time.Millisecond))
		defer stop(time.Second)

		err := sub.Unsubscribe()
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		err = sub.Unsubscribe()
		if expected, actual := true, strings.Contains(err.Error(), "is not found"); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		done := hub.Publish("topic", struct{}{})
		waitForEventToBeSent(t, done)

		if expected, actual := false, called; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestHubWithPublishAndSubscriberDoesMatch(t *testing.T) {
	f := func(topic string) bool {
		var called bool

		hub := New()
		defer hub.Close()

		sub := hub.Subscribe(topic, func(newTopic string, data interface{}) {
			if expected, actual := topic, newTopic; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			called = true
		})

		stop, _ := task.Start(sub.Run(time.Millisecond))
		defer stop(time.Second)

		done := hub.Publish(topic, nil)
		waitForEventToBeSent(t, done)

		return called
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestHubWithPublishAndSubscriberDoesNotMatch(t *testing.T) {
	f := func(topic1, topic2 string) bool {
		// If both topic1 and topic2 match, then return true, so to skip the
		// checking. This is the best we can do unfortunately.
		if topic1 == topic2 {
			return true
		}

		var called bool

		hub := New()
		defer hub.Close()

		sub := hub.Subscribe(topic1, func(newTopic string, data interface{}) {
			called = true
		})

		stop, _ := task.Start(sub.Run(time.Millisecond))
		defer stop(time.Second)

		done := hub.Publish(topic2, nil)
		waitForEventToBeSent(t, done)

		return !called
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestExponential(t *testing.T) {
	t.Run("run once", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var backoffFunc task.BackoffFunc

		mock := NewMockBackoffOptions(ctrl)
		mock.EXPECT().SetBackoff(gomock.Any()).Do(func(args ...interface{}) {
			backoffFunc = args[0].(task.BackoffFunc)
		})

		fn := Exponential(time.Second)
		fn(mock)

		result := backoffFunc(1, time.Millisecond)
		if expected, actual := time.Millisecond, result; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("ensure max limit", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var backoffFunc task.BackoffFunc

		mock := NewMockBackoffOptions(ctrl)
		mock.EXPECT().SetBackoff(gomock.Any()).Do(func(args ...interface{}) {
			backoffFunc = args[0].(task.BackoffFunc)
		})

		fn := Exponential(time.Second)
		fn(mock)

		for i := 0; i < 100; i++ {
			result := backoffFunc(i+1, time.Millisecond)
			if expected, actual := time.Second, result; actual > expected {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
		}
	})

	t.Run("ensure backoff times", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var backoffFunc task.BackoffFunc

		mock := NewMockBackoffOptions(ctrl)
		mock.EXPECT().SetBackoff(gomock.Any()).Do(func(args ...interface{}) {
			backoffFunc = args[0].(task.BackoffFunc)
		})

		fn := Exponential(time.Second)
		fn(mock)

		expected := []time.Duration{
			time.Millisecond * 1,
			time.Millisecond * 2,
			time.Millisecond * 4,
			time.Millisecond * 8,
			time.Millisecond * 16,
			time.Millisecond * 32,
			time.Millisecond * 64,
			time.Millisecond * 128,
			time.Millisecond * 256,
			time.Millisecond * 512,
			time.Millisecond * 1000,
			time.Millisecond * 1000,
		}
		for i := 0; i < len(expected); i++ {
			result := backoffFunc(i+1, time.Millisecond)
			if expected, actual := expected[i], result; actual > expected {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
		}
	})
}
