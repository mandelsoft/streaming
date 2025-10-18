package simplepool

import (
	"context"
	"github.com/mandelsoft/streaming/elem"
	"github.com/mandelsoft/streaming/processing"
)

type _pool struct {
	ctx      context.Context
	cancel   context.CancelFunc
	size     int
	requests WaitQueue[processing.Request]
}

var _ processing.Processing = (*_pool)(nil)

// New creates a new Processing with a specified worker count `n` and an
// optional context for cancellation or timeout.
// It does NOT support releasing a processor when the Go routine is blocked,
// Therefore, it is not suited to be shared with nested chain.Parallel
// processing steps
func New(ctx context.Context, n int) processing.Processing {
	if ctx == nil {
		ctx = context.Background()
	}
	p := &_pool{size: n}
	p.ctx, p.cancel = context.WithCancel(ctx)
	for i := 0; i < n; i++ {
		go p.process()
	}
	return p
}

func (p *_pool) GetPool() processing.Processing {
	return p
}

func (p *_pool) Size() int {
	return p.size
}

func (p *_pool) Execute(r processing.Request, s ...string) {
	p.requests.Push(r)
}

func (p *_pool) Close() error {
	p.cancel()
	return nil
}

func (p *_pool) process() {
	for {
		e, ok := p.requests.Consume(p.ctx)
		if !ok {
			return
		}
		e.Execute(p.ctx)
	}
}

func (p *_pool) CreateChannel() processing.Channel {
	return channel(make(chan *elem.Element, p.size))
}

type channel chan *elem.Element

var _ processing.Channel = (channel)(nil)

func (c channel) Send(ctx context.Context, v *elem.Element) {
	c <- v
}

func (c channel) Receive(ctx context.Context) (*elem.Element, bool, error) {
	v, ok := <-c
	return v, ok, nil
}

func (c channel) Close() error {
	close(c)
	return nil
}
