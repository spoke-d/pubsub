package pubsub

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spoke-d/task"
	"github.com/spoke-d/task/tomb"
)

var (
	// ErrComplete is used as a sentinel error to identify when a worker has
	// been run.
	ErrComplete = errors.New("completed run")

	// ErrTimeout is a sentinel error to identify when a worker has timed out.
	ErrTimeout = errors.New("timed out")
)

// TopicMatcher defines a type that can be used for matching topics when
// dispatching through the hub.
type TopicMatcher func(string) bool

// Hub provides the base functionality of dealing with subscribers,
// and the notification of subscribers of events.
type Hub struct {
	mutex       sync.Mutex
	subscribers []*subscriber
	metrics     Metrics
	index       int
}

// New creates a Hub for others to utilise.
func New(options ...HubOption) *Hub {
	opt := new(hub)
	for _, option := range options {
		option(opt)
	}
	return &Hub{
		metrics: opt.Metrics(),
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
		if s.isAlive() && s.topicMatcher(topic) {
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
		done:         make(chan struct{}),
		life:         Alive,
		metrics:      h.metrics,
	}
	// Ensure we bump the index of the hub to track subscribers
	h.index++
	h.subscribers = append(h.subscribers, sub)
	h.metrics.Subscribe()
	return &Subscriber{
		hub: h,
		sub: sub,
	}
}

// Close will close any outstanding subscriber messages and shut down each
// subscriber.
//
// At the end, the hub will have no subscribers listening and can be seen as
// invalid for use.
func (h *Hub) Close() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	for _, sub := range h.subscribers {
		sub.close()
	}
	h.subscribers = make([]*subscriber, 0)
}

func (h *Hub) unsubscribe(id int) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	for i, sub := range h.subscribers {
		if sub.id == id {
			sub.close()
			h.subscribers = append(h.subscribers[0:i], h.subscribers[i+1:]...)
			h.metrics.Unsubscribe()
			return nil
		}
	}

	return errors.Errorf("%v is not found", id)
}

// HubOptions represents a way to set optional values to a hub option.
// The HubOptions shows what options are available to change.
type HubOptions interface {
	SetMetrics(Metrics)
}

// HubOption captures a tweak that can be applied to the Hub.
type HubOption func(HubOptions)

// Captures options for the Hub.
type hub struct {
	metrics Metrics
}

func (h *hub) SetMetrics(m Metrics) {
	h.metrics = m
}

func (h *hub) Metrics() Metrics {
	if h.metrics == nil {
		return nopMetrics{}
	}
	return h.metrics
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

// Life describes the life cycle of a subscriber, to prevent a subscriber being
// used when it's dead.
type Life int

const (
	// Alive states that the subscriber is available to use.
	Alive Life = iota

	// Dead states that the subscriber is dead and shouldn't be used any more.
	Dead
)

type subscriber struct {
	mutex   sync.RWMutex
	id      int
	queue   *Queue
	metrics Metrics

	topicMatcher TopicMatcher
	handler      func(string, interface{})

	done chan struct{}
	life Life
}

// Run creates a task and a schedule to perform the consumption of messages
// sent to the subscriber from the origin.
// The interval parameter allows for prioritization of each subscriber
// independently using the internval time duration. The aim it to provide
// fair-ness or unfair-ness at a user defined API level. The downside to all of
// this, is that management of a subscriber is then put on the onus of the
// callee.
func (s *subscriber) Run(interval time.Duration) (task.Func, task.Schedule) {
	worker := func(ctx context.Context) error {
		defer func(begin time.Time) {
			s.metrics.RunDuration(time.Since(begin).Seconds())
		}(time.Now())

		// If the subscriber is dead we shouldn't go any further.
		if !s.isAlive() {
			return errors.Errorf("subscriber is dead")
		}

		t := tomb.New()
		if err := t.Go(s.run); err != nil {
			return err
		}

		select {
		case <-t.Dead():
		case <-ctx.Done():
			t.Kill(ErrTimeout)
		}
		return t.Err()
	}

	schedule := task.Backoff(interval, Exponential(interval*10))
	return worker, schedule
}

// Exponential describes a backoff function that grows exponentially with time.
//
// The max time duration is to limit the duration to an upper cap, to prevent
// stalling the hub completely.
var Exponential = func(max time.Duration) func(task.BackoffOptions) {
	return func(backoff task.BackoffOptions) {
		amount := 1
		backoff.SetBackoff(func(n int, t time.Duration) time.Duration {
			// If it's the first time the backoff has been called, then just
			// carry on regardless.
			if n <= 1 {
				amount = n
				return t
			}
			// Apply the exponential backoff dependant on the number of times
			// the backoff has been called.
			base := 2.0
			b := t * time.Duration(math.Pow(base, float64(amount)))
			if b >= max {
				return max
			}
			amount = n
			return b
		})
	}
}

// run will consume all the messages with in the queue, until it's empty. Once
// it's empty it will ask the runner to backoff. If multiple backoffs are
// triggered then the number of times a run is called, is reduced.
//
// Returning an error tells the task what to do and how to perform.
func (s *subscriber) run() error {
	var consumed bool
	for {
		select {
		case <-s.done:
			return task.ErrTerminate
		default:
		}

		node, empty := s.popNode()
		if empty {
			// Only use the backoff error, when nothing is consumed from the
			// queue. If there are events to be handled, then return back to
			// indicate that it was successful and a backoff is not required.
			if !consumed {
				return task.ErrBackoff
			}
			return nil
		}

		evt := node.event
		s.handler(evt.topic, evt.payload)
		s.metrics.EventHandled(evt.topic)
		node.done()

		// We've consumed an event, we can tell the subscriber to not trigger
		// a backoff clause.
		consumed = true
	}
}

func (s *subscriber) dispatch(event Event, done func()) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queue.Push(Node{
		event: event,
		done:  done,
	})
	s.metrics.EventPublished(event.topic)
}

func (s *subscriber) close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for event, empty := s.queue.Pop(); !empty; event, empty = s.queue.Pop() {
		event.done()
	}

	close(s.done)
	s.life = Dead
}

func (s *subscriber) popNode() (Node, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	node, empty := s.queue.Pop()
	if empty {
		return Node{}, true
	}
	return node, false
}

func (s *subscriber) isAlive() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.life == Alive
}

// Event represents a typed message when is dispatched with in the hub
// to a set of subscribers of the hub.
type Event struct {
	topic   string
	payload interface{}
}
