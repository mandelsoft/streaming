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
type Condition = chain.Condition

type Chain = chain.Untyped

func New() Chain {
	return chain.NewUntyped()
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

func (e *Executor[T]) Execute(ctx context.Context, c chain.Chain[any, any]) iter.Seq[any] {
	return c.Execute(ctx, iterutils.Convert[any](e.seq))
}

func (e *Executor[T]) ExecuteWithConfig(ctx context.Context, cfg any, c chain.Chain[any, any]) iter.Seq[any] {
	return c.ExecuteWithConfig(ctx, cfg, iterutils.Convert[any](e.seq))
}

func WithConfig(ctx context.Context, cfg any) context.Context {
	return chain.WithConfig(ctx, cfg)
}
