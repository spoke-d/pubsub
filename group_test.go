package pubsub

import (
	"testing"
	"time"
)

func TestGroup(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		group := NewGroup(time.Millisecond)
		defer group.Close(time.Second)

		done := group.Publish("topic", struct{}{})
		waitForEventToBeSent(t, done)
	})
}

func TestGroupClose(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		group := NewGroup(time.Millisecond)

		group.Close(time.Second)

		done := group.Publish("topic", struct{}{})
		waitForEventToBeSent(t, done)
	})

	t.Run("close after subscribe", func(t *testing.T) {
		var called bool

		group := NewGroup(time.Millisecond)

		group.Subscribe("topic", func(topic string, data interface{}) {
			called = true
		})

		err := group.Start()
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		group.Close(time.Second)

		done := group.Publish("topic", struct{}{})
		waitForEventToBeSent(t, done)

		if expected, actual := false, called; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestGroupSubscribe(t *testing.T) {
	t.Parallel()

	t.Run("one subscriber", func(t *testing.T) {
		var called bool

		group := NewGroup(time.Millisecond)
		defer group.Close(time.Second)

		group.Subscribe("topic", func(topic string, data interface{}) {
			if expected, actual := "topic", topic; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if expected, actual := true, data == nil; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			called = true
		})

		err := group.Start()
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		done := group.Publish("topic", nil)
		waitForEventToBeSent(t, done)

		if expected, actual := true, called; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("close subscriber", func(t *testing.T) {
		var called bool

		group := NewGroup(time.Millisecond)

		sub := group.Subscribe("topic", func(topic string, data interface{}) {
			called = true
		})

		err := group.Start()
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		sub.Close()

		done := group.Publish("topic", nil)
		waitForEventToBeSent(t, done)

		if expected, actual := false, called; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestGroupUnsubscribe(t *testing.T) {
	t.Parallel()

	t.Run("unsubscribe", func(t *testing.T) {
		var called bool

		group := NewGroup(time.Millisecond)
		defer group.Close(time.Second)

		sub := group.Subscribe("topic", func(topic string, data interface{}) {
			called = true
		})

		err := group.Start()
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		err = sub.Unsubscribe()
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		done := group.Publish("topic", struct{}{})
		waitForEventToBeSent(t, done)

		if expected, actual := false, called; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}
