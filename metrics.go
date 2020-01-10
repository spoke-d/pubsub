package pubsub

// Metrics defines an interface for implementing different metric aggregators
// for a hub.
//
// The interface allows the implementation of various metrics, either no-op
// metrics or prometheus.
type Metrics interface {

	// Subscribe is called if a subscription to the hub is called.
	Subscribe()

	// Unsubscribe is called when a subscription is removed from the hub.
	Unsubscribe()

	// EventPublished reports when an event topic is published.
	//
	// The event topic can be used for a label value to allow pivoting of the
	// data. Alternatively it can just be used as a gauged counter.
	EventPublished(string)

	// EventHandled reports when an event topic is handled by the subscriber.
	//
	// The event topic can be used for a label value to allow pivoting of the
	// data. Alternatively it can just be used as a gauged counter.
	EventHandled(string)

	// RunDuration identifies how long a run takes in seconds.
	RunDuration(float64)
}

// nopMetrics is used as a default metric implementation.
// It performs no reporting and just sinks all things to nothing.
type nopMetrics struct{}

func (nopMetrics) Subscribe()            {}
func (nopMetrics) Unsubscribe()          {}
func (nopMetrics) EventPublished(string) {}
func (nopMetrics) EventHandled(string)   {}
func (nopMetrics) RunDuration(float64)   {}
