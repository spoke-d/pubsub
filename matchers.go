package pubsub

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
