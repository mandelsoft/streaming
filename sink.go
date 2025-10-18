package streaming

import (
	"context"
	"iter"

	"github.com/mandelsoft/streaming/chain"
)

type Sink[R, I any] interface {
	Execute(context.Context, iter.Seq[I]) R
}

type Processor[R, O any] func(context.Context, iter.Seq[O]) R

type ProcessorFactory[R, O any] func() Processor[R, O]

type sink[R, I, O any] struct {
	chain chain.Chain[I, O]
	p     ProcessorFactory[R, O]
}

func NewSink[R, I, O any](c chain.Chain[I, O], p ProcessorFactory[R, O]) Sink[R, I] {
	return &sink[R, I, O]{c, p}
}

func (s *sink[R, I, O]) Execute(ctx context.Context, in iter.Seq[I]) R {
	out := s.chain.Execute(ctx, in)
	return s.p()(ctx, out)
}
