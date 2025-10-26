package untypedchain

import (
	"context"
	"iter"

	"github.com/mandelsoft/goutils/iterutils"
	"github.com/mandelsoft/streaming/internal"
	"github.com/mandelsoft/streaming/processing"
)

type Mapper = internal.Mapper
type Exploder = internal.Exploder
type Filter = internal.Filter
type Aggregator = internal.Aggregator
type CompareFunc = internal.CompareFunc
type Condition = internal.Condition

type Chain = internal.Chain

func New() Chain {
	return internal.New()
}

func Mapped(f Mapper, name ...string) Chain {
	return New().Map(f, name...)
}

func Exploded(f Exploder, name ...string) Chain {
	return New().Explode(f, name...)
}

func Filtered(f Filter, name ...string) Chain {
	return New().Filter(f, name...)
}

func Aggregated(f Aggregator, name ...string) Chain {
	return New().Aggregate(f, name...)
}

func Sorted(f CompareFunc, name ...string) Chain {
	return New().Sort(f, name...)
}

func Parallel(f Chain, p processing.Processing, name ...string) Chain {
	return New().Parallel(f, p, name...)
}

func Stable(f Chain, p processing.Processing, name ...string) Chain {
	return New().Stable(f, p, name...)
}

func Conditional(cond Condition, f Chain, name ...string) Chain {
	return New().Conditional(cond, f, name...)
}

// For can be used to execute an untyped chain for a typed
// iterator (`For(it).Execute(chain))
func For[T any](seq iter.Seq[T]) *Executor[T] {
	return &Executor[T]{seq}
}

type Executor[T any] struct {
	seq iter.Seq[T]
}

func (e *Executor[T]) Execute(ctx context.Context, c Chain) iter.Seq[any] {
	return c.Execute(ctx, iterutils.Convert[any](e.seq))
}

func (e *Executor[T]) ExecuteWithConfig(ctx context.Context, cfg any, c Chain) iter.Seq[any] {
	return c.ExecuteWithConfig(ctx, cfg, iterutils.Convert[any](e.seq))
}

func WithConfig(ctx context.Context, cfg any) context.Context {
	return internal.WithConfig(ctx, cfg)
}
