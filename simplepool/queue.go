package simplepool

import (
	"sync"
)

type entry[E any] struct {
	req  E
	next *entry[E]
}

type Queue[E any] struct {
	lock sync.Mutex
	head *entry[E]
	tail *entry[E]
}

func (q *Queue[E]) push(req E) {
	e := &entry[E]{req, nil}
	if q.tail != nil {
		q.tail.next = e
	} else {
		q.head = e
	}
	q.tail = e
}

func (q *Queue[E]) Push(req E) {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.push(req)
}

func (q *Queue[E]) pull() (E, bool) {
	var _nil E
	e := q.head
	if e == nil {
		return _nil, false
	}
	if e == q.tail {
		q.tail = nil
	}
	q.head = e.next
	return e.req, true
}

func (q *Queue[E]) Pull() (E, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()

	return q.pull()
}
