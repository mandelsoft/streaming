package internal

import (
	"context"
	"github.com/mandelsoft/streaming/processing"
	"iter"
)

type Chain interface {
	impl() *chain

	Map(Mapper, ...string) Chain
	Explode(Exploder, ...string) Chain
	Filter(Filter, ...string) Chain
	Sort(CompareFunc, ...string) Chain
	Transform(Transformer, ...string) Chain
	Aggregate(Aggregator, ...string) Chain
	Sequential(untypedChain Chain, name ...string) Chain
	Parallel(untypedChain Chain, processing processing.Processing, name ...string) Chain
	Stable(untypedChain Chain, processing processing.Processing, name ...string) Chain
	Conditional(c Condition, untypedChain Chain, name ...string) Chain
	Add(n Chain, name ...string) Chain
	Step(s Step) Chain

	Execute(ctx context.Context, seq iter.Seq[any], name ...string) iter.Seq[any]
	ExecuteWithConfig(ctx context.Context, cfg any, seq iter.Seq[any], name ...string) iter.Seq[any]
}
