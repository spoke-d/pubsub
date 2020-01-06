package pubsub

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spoke-d/task"
	"github.com/spoke-d/task/tomb"
)

// TopicMatcher defines a type that can be used for matching topics when
// dispatching through the hub.
type TopicMatcher func(string) bool

// Hub provides the base functionality of dealing with subscribers,
// and the notification of subscribers of events.
type Hub struct {
	mutex       sync.Mutex
	subscribers []*subscriber
	index       int
}

// New creates a Hub for others to utilise.
func New() *Hub {
	return &Hub{}
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
func (h *Hub) Publish(topic string, data interface{}) <-chan struct{} {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	var wait sync.WaitGroup

	event := Event{
		topic:   topic,
		payload: data,
	}
	done := make(chan struct{})

	for _, s := range h.subscribers {
		if s.topicMatcher(topic) {
			wait.Add(1)
			s.dispatch(event, wait.Done)
		}
	}

	go func() {
		wait.Wait()
		close(done)
	}()

	return done
}

// Subscribe to a topic with a handler function. If the topic is the same
// as the published topic, the handler function is called with the
// published topic and the associated data.
//
// The return value is a Subscriber that will unsubscribe the caller from
// the hub, for this subscription.
func (h *Hub) Subscribe(topic string, handler func(string, interface{})) *Subscriber {
	return h.SubscribeMatch(Match(topic), handler)
}

// SubscribeMatch takes a function that determins whether the topic matches,
// and a handler function. If the matcher matches the published topic, the
// handler function is called with the published topic and the associated
// data.
//
// The return value is a Subscriber that will unsubscribe the caller from
// the hub, for this subscription.
func (h *Hub) SubscribeMatch(matcher TopicMatcher, handler func(string, interface{})) *Subscriber {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	sub := &subscriber{
		id:           h.index,
		queue:        NewQueue(),
		topicMatcher: matcher,
		handler:      handler,
		data:         make(chan struct{}, 1),
		done:         make(chan struct{}),
	}
	// Ensure we bump the index of the hub to track subscribers
	h.index++
	h.subscribers = append(h.subscribers, sub)
	return &Subscriber{
		hub: h,
		sub: sub,
	}
}

func (h *Hub) unsubscribe(id int) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	for i, sub := range h.subscribers {
		if sub.id == id {
			sub.close()
			h.subscribers = append(h.subscribers[0:i], h.subscribers[i+1:]...)
			return nil
		}
	}

	return errors.Errorf("%v is not found", id)
}

// Subscriber represents a subscription to the hub.
type Subscriber struct {
	hub *Hub
	sub *subscriber
}

// Run creates a task and a schedule to perform the consumption of messages
// sent to the subscriber from the origin.
// The interval parameter allows for prioritization of each subscriber
// independently using the internval time duration. The aim it to provide
// fair-ness or unfair-ness at a user defined API level. The downside to all of
// this, is that management of a subscriber is then put on the onus of the
// callee.
func (s *Subscriber) Run(interval time.Duration) (task.Func, task.Schedule) {
	return s.sub.Run(interval)
}

// Unsubscribe attempts to unsubscribe from the hub, if the subscriber
// is found within the hub, then a error is returned.
func (s *Subscriber) Unsubscribe() error {
	return s.hub.unsubscribe(s.sub.id)
}

// Close will ensure that any pending events will be closed out and the
// associated blocking actions are collapsed.
func (s *Subscriber) Close() {
	s.sub.close()
}

type subscriber struct {
	mutex sync.Mutex
	id    int
	queue *Queue

	topicMatcher TopicMatcher
	handler      func(string, interface{})

	data chan struct{}
	done chan struct{}
}

// Run creates a task and a schedule to perform the consumption of messages
// sent to the subscriber from the origin.
// The interval parameter allows for prioritization of each subscriber
// independently using the internval time duration. The aim it to provide
// fair-ness or unfair-ness at a user defined API level. The downside to all of
// this, is that management of a subscriber is then put on the onus of the
// callee.
func (s *subscriber) Run(interval time.Duration) (task.Func, task.Schedule) {
	worker := func(ctx context.Context) {
		t := tomb.New()
		t.Go(func() error {
			s.run(ctx)
			return nil
		})
		select {
		case <-t.Dead():
		case <-ctx.Done():
		}
	}

	schedule := task.Every(interval)
	return worker, schedule
}

func (s *subscriber) run(context.Context) {
	select {
	case <-s.done:
		return
	case <-s.data:
	}

	node, empty := s.popNode()
	if empty {
		return
	}

	evt := node.event
	s.handler(evt.topic, evt.payload)
	node.done()
}

func (s *subscriber) dispatch(event Event, done func()) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queue.Push(Node{
		event: event,
		done:  done,
	})
	if s.queue.Len() == 1 {
		s.data <- struct{}{}
	}
}

func (s *subscriber) close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for event, ok := s.queue.Pop(); ok; event, ok = s.queue.Pop() {
		event.done()
	}
	close(s.done)
}

func (s *subscriber) popNode() (Node, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	node, ok := s.queue.Pop()
	if !ok {
		return Node{}, true
	}
	return node, s.queue.Len() == 0
}

// Event represents a typed message when is dispatched with in the hub
// to a set of subscribers of the hub.
type Event struct {
	topic   string
	payload interface{}
}
