package internal

import (
	"context"
	"github.com/mandelsoft/streaming/processing"
	"iter"
)

type Implementation interface {
	impl() *chain
}

type Chain interface {
	Implementation

	Map(Mapper, ...string) Chain
	Explode(Exploder, ...string) Chain
	Filter(Filter, ...string) Chain
	Sort(CompareFunc, ...string) Chain
	Aggregate(Aggregator, ...string) Chain
	Sequential(untypedChain Chain, name ...string) Chain
	Parallel(untypedChain Chain, processing processing.Processing, name ...string) Chain
	Stable(untypedChain Chain, processing processing.Processing, name ...string) Chain
	Conditional(c Condition, untypedChain Chain, name ...string) Chain
	Add(n Chain, name ...string) Chain

	Execute(ctx context.Context, seq iter.Seq[any], name ...string) iter.Seq[any]
	ExecuteWithConfig(ctx context.Context, cfg any, seq iter.Seq[any], name ...string) iter.Seq[any]
}
