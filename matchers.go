package pubsub

import (
	"strings"
)

// Match with attempt to match a topic with another topic using
// exactly the same topic.
func Match(topic string) func(string) bool {
	return func(other string) bool {
		return topic == other
	}
}

// Any will always return true when matching a topic
func Any() func(string) bool {
	return func(other string) bool {
		return true
	}
}

// Never will never match a topic
func Never() func(string) bool {
	return func(other string) bool {
		return false
	}
}

// StartsWith will match a topic that starts with a certain string
func StartsWith(prefix string) func(string) bool {
	return func(other string) bool {
		return strings.HasPrefix(other, prefix)
	}
}

// EndsWith will match a topic that ends with a certain string
func EndsWith(suffix string) func(string) bool {
	return func(other string) bool {
		return strings.HasSuffix(other, suffix)
	}
}

// Contains will match a topic that contains a certain string
func Contains(value string) func(string) bool {
	return func(other string) bool {
		return strings.Contains(other, value)
	}
}
