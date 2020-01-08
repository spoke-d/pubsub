package pubsub

import (
	"fmt"
	"testing"
	"testing/quick"
)

func TestMatch(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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

func TestStartsWith(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		ok := StartsWith("")("")
		if expected, actual := true, ok; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("same topic", func(t *testing.T) {
		f := func(topic string) bool {
			return StartsWith(topic)(topic)
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("same topic with suffix", func(t *testing.T) {
		f := func(topic, suffix string) bool {
			return StartsWith(topic)(fmt.Sprintf("%s%s", topic, suffix))
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("alternative topic", func(t *testing.T) {
		f := func(prefix, topic string) bool {
			if prefix == topic || prefix == "" {
				return true
			}
			return !StartsWith(prefix)(topic)
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestEndsWith(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		ok := EndsWith("")("")
		if expected, actual := true, ok; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("same topic", func(t *testing.T) {
		f := func(topic string) bool {
			return EndsWith(topic)(topic)
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("same topic with prefix", func(t *testing.T) {
		f := func(topic, prefix string) bool {
			return EndsWith(topic)(fmt.Sprintf("%s%s", prefix, topic))
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("alternative topic", func(t *testing.T) {
		f := func(suffix, topic string) bool {
			if suffix == topic || suffix == "" {
				return true
			}
			return !EndsWith(suffix)(topic)
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestContains(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		ok := Contains("")("")
		if expected, actual := true, ok; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("same topic", func(t *testing.T) {
		f := func(topic string) bool {
			return Contains(topic)(topic)
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("same topic with prefix and suffix", func(t *testing.T) {
		f := func(topic, prefix, suffix string) bool {
			return Contains(topic)(fmt.Sprintf("%s%s%s", prefix, topic, suffix))
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
}
