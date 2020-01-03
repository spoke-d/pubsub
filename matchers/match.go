package matchers

// Match with attempt to match a topic with another topic using
// exactly the same topic.
func Match(topic string) func(string) bool {
	return func(other string) bool {
		return topic == other
	}
}
