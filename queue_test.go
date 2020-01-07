package pubsub

import (
	"testing"
	"testing/quick"
)

func TestQueueLen(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		q := NewQueue()
		if expected, actual := 0, q.Len(); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("push node", func(t *testing.T) {
		q := NewQueue()
		q.Push(Node{})
		if expected, actual := 1, q.Len(); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("push multiple nodes", func(t *testing.T) {
		q := NewQueue()
		for i := 0; i < 10; i++ {
			q.Push(Node{})
			if expected, actual := i+1, q.Len(); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
		}
	})

	t.Run("push and pop", func(t *testing.T) {
		q := NewQueue()
		for i := 0; i < 10; i++ {
			q.Push(Node{})
			if expected, actual := i+1, q.Len(); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
		}

		_, ok := q.Pop()
		if expected, actual := false, ok; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := 9, q.Len(); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		index := q.Len() - 1
		for _, ok := q.Pop(); !ok; _, ok = q.Pop() {
			if expected, actual := index, q.Len(); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			index--
		}

		if expected, actual := 0, q.Len(); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})
}

func TestQueuePop(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		q := NewQueue()
		node, ok := q.Pop()
		if expected, actual := true, ok; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := node.event, (Event{}); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestQueuePushReturnsValidNode(t *testing.T) {
	f := func(topic string, payload string) bool {
		q := NewQueue()
		if expected, actual := 0, q.Len(); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		node := Node{
			event: Event{
				topic:   topic,
				payload: payload,
			},
		}
		q.Push(node)
		if expected, actual := 1, q.Len(); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		other, ok := q.Pop()
		return !ok && node.event == other.event
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestQueuePushAmountHasValidLen(t *testing.T) {
	f := func(amount uint8) bool {
		q := NewQueue()
		if expected, actual := 0, q.Len(); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		for i := 0; i < int(amount); i++ {
			q.Push(Node{})
		}
		return int(amount) == q.Len()
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}
