package pubsub

import (
	"time"

	"github.com/spoke-d/task"
)

// Group represents a group where all subscribers are treated fairly in terms
// of task consumption.
type Group struct {
	hub      *Hub
	group    *task.Group
	interval time.Duration
}

// NewGroup creates a new hub where all subscribers are treated equally in terms
// of fairness.
func NewGroup(interval time.Duration) *Group {
	return &Group{
		hub:      New(),
		group:    task.NewGroup(),
		interval: interval,
	}
}

// Publish will notify all the subscribers that are interested by calling
// their handler function.
//
// The data is passed through to each Subscriber untouched. Note that all
// subscribers are notified in parallel, and that no modification should be
// done to the data or data races will occur.
//
// The channel return value is closed when all the subscribers have been
// notified of the event.
func (g *Group) Publish(topic string, data interface{}) <-chan struct{} {
	return g.hub.Publish(topic, data)
}

// Subscribe to a topic with a handler function. If the topic is the same
// as the published topic, the handler function is called with the
// published topic and the associated data.
//
// The return value is a Subscriber that will unsubscribe the caller from
// the hub, for this subscription.
func (g *Group) Subscribe(topic string, handler func(string, interface{})) *GroupSubscriber {
	return g.SubscribeMatch(Match(topic), handler)
}

// SubscribeMatch takes a function that determins whether the topic matches,
// and a handler function. If the matcher matches the published topic, the
// handler function is called with the published topic and the associated
// data.
//
// The return value is a Subscriber that will unsubscribe the caller from
// the hub, for this subscription.
func (g *Group) SubscribeMatch(matcher TopicMatcher, handler func(string, interface{})) *GroupSubscriber {
	sub := g.hub.SubscribeMatch(matcher, handler)
	_ = g.group.Add(sub.Run(g.interval))
	return &GroupSubscriber{
		sub: sub,
	}
}

// Start all the tasks in the group.
func (g *Group) Start() error {
	return g.group.Start()
}

// Stop all tasks in the group.
//
// In case the given timeout expires before all tasks complete, this method
// exits immediately and returns an error, otherwise it returns nil.
func (g *Group) Stop(timeout time.Duration) error {
	return g.group.Stop(timeout)
}

// GroupSubscriber represents a subscription to the hub.
type GroupSubscriber struct {
	sub *Subscriber
}

// Unsubscribe attempts to unsubscribe from the hub, if the subscriber
// is found within the hub, then a error is returned.
func (s *GroupSubscriber) Unsubscribe() error {
	return s.sub.Unsubscribe()
}

// Close will ensure that any pending events will be closed out and the
// associated blocking actions are collapsed.
func (s *GroupSubscriber) Close() {
	s.sub.Close()
}
