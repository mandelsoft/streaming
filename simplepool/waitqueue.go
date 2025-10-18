package simplepool

import "context"

type WaitQueue[E any] struct {
	ctx context.Context
	Queue[E]
	waiting Waiting[E]
}

func NewWaitQueue[E any](ctx context.Context) *WaitQueue[E] {
	return &WaitQueue[E]{ctx: ctx}
}

func (q *WaitQueue[E]) Push(req E) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if !q.waiting.Provide(req) {
		q.Queue.push(req)
	}
}

func (q *WaitQueue[E]) Consume(ctx context.Context) (E, bool) {
	q.lock.Lock()

	r, ok := q.pull()
	if ok {
		q.lock.Unlock()
		return r, ok
	}

	f := q.waiting.Future(q.ctx)
	q.lock.Unlock()
	v, ok := f.Wait(ctx)
	return v, ok
}
