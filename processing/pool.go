package processing

import (
	"context"
	"github.com/mandelsoft/streaming/elem"
)

type Channel interface {
	Send(ctx context.Context, v *elem.Element)
	Receive(ctx context.Context) (*elem.Element, bool, error)
	Close() error
}

type Request interface {
	Execute(ctx context.Context)
}

type Provider interface {
	GetPool() Processing
}

type Processing interface {
	Provider
	Size() int
	Execute(req Request, name ...string)
	Close() error

	CreateChannel() Channel
}
