package untypedchain

import "iter"
import "context"

type Sink[R any] interface {
	Execute(context.Context, iter.Seq[any]) R
}

type Processor[R any] func(context.Context, iter.Seq[any]) R

type ProcessorFactory[R any] func() Processor[R]

type sink[R any] struct {
	chain Chain
	p     ProcessorFactory[R]
}

func NewSink[R any](c Chain, p ProcessorFactory[R]) Sink[R] {
	return &sink[R]{c, p}
}

func (s *sink[R]) Execute(ctx context.Context, in iter.Seq[any]) R {
	out := s.chain.Execute(ctx, in)
	return s.p()(ctx, out)
}
