package pubsub

// Node represents a type queue node. In other languages this would be
// simply a generic tuple of type <T>
type Node struct {
	event Event
	done  func()
}

// Queue represents a single instance of a queue structure.
// Under the hood, it implements a first in, first out (FIFO), data
// structure.
//
// No concurrency guarantees are created for the queue structure, it
// is expected to roll your own when interacting with the Queue.
type Queue struct {
	nodes []Node
}

// NewQueue creates a queue.
func NewQueue() *Queue {
	return &Queue{}
}

// Push appends an element to the back of the queue. Implements FIFO when
// elements are removed with Pop
func (f *Queue) Push(event Node) {
	f.nodes = append(f.nodes, event)
}

// Pop removes and returns the element from the front of the queue.
// Implements FIFO when used with Push().
//
// Returns false if the queue is empty
func (f *Queue) Pop() (Node, bool) {
	if len(f.nodes) == 0 {
		return Node{}, true
	}

	var node Node
	node, f.nodes = f.nodes[0], f.nodes[1:]
	return node, false
}

// Len returns the number of elements currently stored in the queue.
func (f *Queue) Len() int {
	return len(f.nodes)
}
