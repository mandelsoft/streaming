package untypedchain

import (
	"context"
	"github.com/mandelsoft/goutils/iterutils"
	"github.com/mandelsoft/streaming/chain"
	"github.com/mandelsoft/streaming/processing"
	"iter"
)

type Mapper = chain.Mapper[any, any]
type Exploder = chain.Exploder[any, any]
type Filter = chain.Filter[any]
type Aggregator = chain.Aggregator[any, any]
type CompareFunc = chain.CompareFunc[any]

type Chain = chain.Untyped

func New() Chain {
	return chain.NewUntyped()
}

func Mapped(f Mapper) Chain {
	return New().Map(f)
}

func Exploded(f Exploder) Chain {
	return New().Explode(f)
}

func Filtered(f Filter) Chain {
	return New().Filter(f)
}

func Aggregated(f Aggregator) Chain {
	return New().Aggregate(f)
}

func Sorted(f CompareFunc) Chain {
	return New().Sort(f)
}

func Parallel(f Chain, p processing.Processing) Chain {
	return New().Parallel(f, p)
}

func Stable(f Chain, p processing.Processing) Chain {
	return New().Stable(f, p)
}

// For can be used to execute an untyped chain for a typed
// iterator (`For(it).Execute(chain))
func For[T any](seq iter.Seq[T]) *Executor[T] {
	return &Executor[T]{seq}
}

type Executor[T any] struct {
	seq iter.Seq[T]
}

func (e *Executor[T]) Execute(ctx context.Context, c chain.Chain[any, any]) iter.Seq[any] {
	return c.Execute(ctx, iterutils.Convert[any](e.seq))
}
