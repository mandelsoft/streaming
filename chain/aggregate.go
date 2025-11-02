package chain

import (
	"github.com/mandelsoft/goutils/sliceutils"
	"github.com/mandelsoft/streaming/internal"
)

// Aggregation represents a generic interface for performing incremental
// aggregation over input elements.
// The input type is represented by I and the output type by O.
// The Aggregate method processes input elements and optionally finalizes
// aggregation for the current input batch.
// The Finish method finalizes and retrieves still pending aggregated results.
type Aggregation[I, O any] interface {
	internal.GAggregation[I, O]
}

// Aggregator is a factory function creating an aggregation object.
// The generated object typically has its own local state to process the aggregation.
type Aggregator[I, O any] = func() Aggregation[I, O]

////////////////////////////////////////////////////////////////////////////////

func AggregationStep[I, O any](m Aggregator[I, O], name ...string) Step[I, O] {
	return &step[I, O]{internal.AggregationStep(ConvertAggregator[any, any](m), name...)}
}

func Aggregated[I, O any](m Aggregator[I, O], name ...string) Chain[I, O] {
	return AddAggregation[O, I, I](nil, m, name...)
}

func AddAggregation[N, I, O any](base Chain[I, O], m Aggregator[O, N], name ...string) Chain[I, N] {
	c := chainImpl(base).Aggregate(ConvertAggregator[any, any](m), name...)
	return convertChain[I, N](c)
}

////////////////////////////////////////////////////////////////////////////////

func ConvertAggregator[NI, NO, I, O any](in Aggregator[I, O]) internal.GAggregator[NI, NO] {
	return func() internal.GAggregation[NI, NO] {
		return &convertingAggregation[NI, NO, I, O]{in()}
	}
}

type convertingAggregation[NI, NO, I, O any] struct {
	aggregation Aggregation[I, O]
}

var _ Aggregation[any, any] = (*convertingAggregation[any, any, int, int])(nil)

func (c *convertingAggregation[NI, NO, I, O]) convertIn(in NI) I {
	var i any = in
	if i == nil {
		var _nil I
		i = _nil
	}
	return i.(I)
}

func (c *convertingAggregation[NI, NO, I, O]) convertOut(out []O) []NO {
	return sliceutils.Convert[NO](out)
}

func (c *convertingAggregation[NI, NO, I, O]) Aggregate(i NI) []NO {
	return c.convertOut(c.aggregation.Aggregate(c.convertIn(i)))
}

func (c *convertingAggregation[NI, NO, I, O]) Finish() []NO {
	return c.convertOut(c.aggregation.Finish())
}
