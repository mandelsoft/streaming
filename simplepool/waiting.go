package simplepool

import (
	"context"
	"sync"
)

type Future[E any] struct {
	ctx   context.Context
	lock  sync.Mutex
	value chan E
	done  bool
}

func NewFuture[E any](ctx context.Context) *Future[E] {
	return &Future[E]{ctx: ctx, value: make(chan E, 1)}
}

func (b *Future[E]) Provide(req E) bool {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.done {
		return false
	}
	select {
	case b.value <- req:
		return true
	default:
		// channel closed for canceled Wait.
		return false
	}
}

func (b *Future[E]) Wait(ctx context.Context) (E, bool) {
	var e E
	ok := false
	done := true

	if ctx != nil {
		if b.ctx != nil {
			select {
			case e, ok = <-b.value:
			case _, done = <-b.ctx.Done():
			case _, done = <-ctx.Done():
			}
		} else {
			select {
			case e, ok = <-b.value:
			case _, done = <-ctx.Done():
			}
		}
	} else {
		if b.ctx != nil {
			select {
			case e, ok = <-b.value:
			case _, done = <-b.ctx.Done():
			}
		} else {
			e, ok = <-b.value
		}
	}

	// handle trace condition between unsynchronized cancel and a
	// Provide call
	b.lock.Lock()
	defer b.lock.Unlock()
	b.done = true
	if !done {
		select {
		case e, ok = <-b.value:
		default:
		}
	}
	// fmt.Printf("close %v (%v) done %t, ok %t\n", b, e, done, ok)
	close(b.value)
	return e, ok
}

type Waiting[E any] struct {
	waiting Queue[*Future[E]]
}

func (w *Waiting[E]) Future(ctx context.Context) *Future[E] {
	f := NewFuture[E](ctx)
	w.waiting.Push(f)
	return f
}

func (w *Waiting[E]) Provide(req E) bool {
	for {
		f, ok := w.waiting.Pull()
		if !ok {
			return false
		}
		// fmt.Printf("provide %v (%v)\n", f, req)
		if f.Provide(req) {
			return true
		}
		// cleanup wait queue from cancelled waits
	}
}
