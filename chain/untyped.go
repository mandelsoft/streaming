package chain

import (
	"github.com/mandelsoft/streaming/processing"
)

type Untyped interface {
	Chain[any, any]

	Map(Mapper[any, any], ...string) Untyped
	Explode(Exploder[any, any], ...string) Untyped
	Filter(Filter[any], ...string) Untyped
	Sort(CompareFunc[any], ...string) Untyped
	Aggregate(Aggregator[any, any], ...string) Untyped
	Sequential(untypedChain Untyped, name ...string) Untyped
	Parallel(untypedChain Untyped, processing processing.Processing, name ...string) Untyped
	Stable(untypedChain Untyped, processing processing.Processing, name ...string) Untyped

	Add(n Untyped, name ...string) Untyped
}

func NewUntyped() Untyped {
	return &chain{}
}
