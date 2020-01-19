package pubsub

import (
	"testing"
	"time"

	"github.com/spoke-d/task"
)

func benchmarkPublish(i int, b *testing.B) {
	b.StopTimer()

	hub := New()
	defer hub.Close()

	var called int
	sub := hub.SubscribeMatch(Any(), func(topic string, data interface{}) {
		called++
	})
	stop, _ := task.Start(sub.Run(time.Nanosecond))
	defer stop(time.Millisecond)

	b.StartTimer()

	var failed int
	for n := 0; n < b.N; n++ {
		done := hub.Publish("topic", nil)

		select {
		case <-done:
		case <-time.After(time.Second):
			failed++
		}
	}

	if expected, actual := b.N, called; expected != actual {
		b.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := 0, failed; expected != actual {
		b.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func BenchmarkPublish1(b *testing.B)    { benchmarkPublish(1, b) }
func BenchmarkPublish10(b *testing.B)   { benchmarkPublish(10, b) }
func BenchmarkPublish100(b *testing.B)  { benchmarkPublish(100, b) }
func BenchmarkPublish1000(b *testing.B) { benchmarkPublish(1000, b) }

func createHubWithSubscribers(b *testing.B, t int) (*Hub, func()) {
	b.StopTimer()

	hub := New()

	stops := make([]func(time.Duration) error, t)
	for i := 0; i < t; i++ {
		sub := hub.SubscribeMatch(Any(), func(topic string, data interface{}) {})
		stop, _ := task.Start(sub.Run(time.Nanosecond))
		stops[i] = stop
	}

	b.StartTimer()

	return hub, func() {
		for _, stop := range stops {
			stop(time.Second)
		}
		hub.Close()
	}
}

func benchmarkPublishToSubsribers(hub *Hub, i int, b *testing.B) {
	var failed int
	for n := 0; n < b.N; n++ {
		done := hub.Publish("topic", nil)

		select {
		case <-done:
		case <-time.After(time.Second):
			failed++
		}
	}

	if expected, actual := 0, failed; expected != actual {
		b.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func BenchmarkPublish_1(b *testing.B) {
	b.Run("10", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 10)
		defer close()

		benchmarkPublishToSubsribers(hub, 1, b)
	})

	b.Run("100", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 100)
		defer close()

		benchmarkPublishToSubsribers(hub, 1, b)
	})

	b.Run("1000", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 1000)
		defer close()

		benchmarkPublishToSubsribers(hub, 1, b)
	})
}

func BenchmarkPublish_10(b *testing.B) {
	b.Run("10", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 10)
		defer close()

		benchmarkPublishToSubsribers(hub, 10, b)
	})

	b.Run("100", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 100)
		defer close()

		benchmarkPublishToSubsribers(hub, 10, b)
	})

	b.Run("1000", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 1000)
		defer close()

		benchmarkPublishToSubsribers(hub, 10, b)
	})
}

func BenchmarkPublish_100(b *testing.B) {
	b.Run("10", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 10)
		defer close()

		benchmarkPublishToSubsribers(hub, 100, b)
	})

	b.Run("100", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 100)
		defer close()

		benchmarkPublishToSubsribers(hub, 100, b)
	})

	b.Run("1000", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 1000)
		defer close()

		benchmarkPublishToSubsribers(hub, 100, b)
	})
}

func BenchmarkPublish_1000(b *testing.B) {
	b.Run("10", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 10)
		defer close()

		benchmarkPublishToSubsribers(hub, 1000, b)
	})

	b.Run("100", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 100)
		defer close()

		benchmarkPublishToSubsribers(hub, 1000, b)
	})

	b.Run("1000", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 1000)
		defer close()

		benchmarkPublishToSubsribers(hub, 1000, b)
	})
}

func benchmarkPublishToSubsribersNoWaiting(hub *Hub, i int, b *testing.B) {
	var failed int
	var last <-chan struct{}
	for n := 0; n < b.N; n++ {
		last = hub.Publish("topic", nil)
	}

	select {
	case <-last:
	case <-time.After(time.Second):
		b.Fatal("event was not dispatched in time")
	}

	if expected, actual := 0, failed; expected != actual {
		b.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func BenchmarkPublishNoWaiting_1(b *testing.B) {
	b.Run("10", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 10)
		defer close()

		benchmarkPublishToSubsribersNoWaiting(hub, 1, b)
	})

	b.Run("100", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 100)
		defer close()

		benchmarkPublishToSubsribersNoWaiting(hub, 1, b)
	})

	b.Run("1000", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 1000)
		defer close()

		benchmarkPublishToSubsribersNoWaiting(hub, 1, b)
	})
}

func BenchmarkPublishNoWaiting_10(b *testing.B) {
	b.Run("10", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 10)
		defer close()

		benchmarkPublishToSubsribersNoWaiting(hub, 10, b)
	})

	b.Run("100", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 100)
		defer close()

		benchmarkPublishToSubsribersNoWaiting(hub, 10, b)
	})

	b.Run("1000", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 1000)
		defer close()

		benchmarkPublishToSubsribersNoWaiting(hub, 10, b)
	})
}

func BenchmarkPublishNoWaiting_100(b *testing.B) {
	b.Run("10", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 10)
		defer close()

		benchmarkPublishToSubsribersNoWaiting(hub, 100, b)
	})

	b.Run("100", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 100)
		defer close()

		benchmarkPublishToSubsribersNoWaiting(hub, 100, b)
	})

	b.Run("1000", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 1000)
		defer close()

		benchmarkPublishToSubsribersNoWaiting(hub, 100, b)
	})
}

func BenchmarkPublishNoWaiting_1000(b *testing.B) {
	b.Run("10", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 10)
		defer close()

		benchmarkPublishToSubsribersNoWaiting(hub, 1000, b)
	})

	b.Run("100", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 100)
		defer close()

		benchmarkPublishToSubsribersNoWaiting(hub, 1000, b)
	})

	b.Run("1000", func(b *testing.B) {
		hub, close := createHubWithSubscribers(b, 1000)
		defer close()

		benchmarkPublishToSubsribersNoWaiting(hub, 1000, b)
	})
}
