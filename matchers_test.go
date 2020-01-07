package pubsub

import (
	"testing"
	"testing/quick"
)

func TestMatch(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		ok := Match("")("")
		if expected, actual := true, ok; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("same topic", func(t *testing.T) {
		f := func(topic string) bool {
			return Match(topic)(topic)
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("alternative topic", func(t *testing.T) {
		f := func(topic, alt string) bool {
			// If both topic and alt match, then return true, so to skip the
			// checking. This is the best we can do unfortunately.
			if topic == alt {
				return true
			}
			return !Match(topic)(alt)
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestAny(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		ok := Any()("")
		if expected, actual := true, ok; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("any topic", func(t *testing.T) {
		f := func(topic string) bool {
			return Any()(topic)
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestNever(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		ok := Never()("")
		if expected, actual := false, ok; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("any topic", func(t *testing.T) {
		f := func(topic string) bool {
			return !Never()(topic)
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
}
