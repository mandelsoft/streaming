package streaming

import (
	"context"
	"fmt"
	"github.com/mandelsoft/goutils/generics"
	"iter"

	"github.com/mandelsoft/streaming/chain"
)

type Void = interface {
	none()
}

var None Void = none{}

type none struct{}

func (n none) none() {}

////////////////////////////////////////////////////////////////////////////////

type Sink[C, R, I any] interface {
	// Execute executes the sink for the given input sequence
	// providing the result of type R or an error.
	Execute(ctx context.Context, cfg C, in iter.Seq[I]) (R, error)
}

type Processor[R, O any] func(context.Context, iter.Seq[O]) (R, error)

type ProcessorFactory[C, R, O any] interface {
	Processor(cfg C) (Processor[R, O], error)
}

type ProcessorFactoryFunc[C, R, O any] func(cfg C) (Processor[R, O], error)

func (f ProcessorFactoryFunc[C, R, O]) Processor(cfg C) (Processor[R, O], error) {
	return f(cfg)
}

type sink[C, R, I, O any] struct {
	chain chain.Chain[I, O]
	f     ProcessorFactory[C, R, O]
}

// NewSink creates a sink using a chain to process an input sequence passed
// to a processor created with the given ProcessorFactory.
// If the final sink has no configured processor, the result type R must be
// iter.Seq[O] and the result is the output iterator of the given chain.
func NewSink[C, R, I, O any](c chain.Chain[I, O], f ProcessorFactory[C, R, O]) Sink[C, R, I] {
	return &sink[C, R, I, O]{c, f}
}

func (s *sink[C, R, I, O]) Execute(ctx context.Context, cfg C, in iter.Seq[I]) (R, error) {
	var _nil R
	if s.f != nil {
		p, err := s.f.Processor(cfg)
		if err != nil {
			return _nil, err
		}
		out := s.chain.Execute(ctx, in)
		return p(ctx, out)
	}
	if generics.CanAssign[iter.Seq[O], R]() {
		out := s.chain.Execute(ctx, in)
		return generics.Cast[R](out), nil
	} else {
		return _nil, fmt.Errorf("chain result iterator cannot be assigned to result type %s", generics.TypeOf[R]())
	}
}
