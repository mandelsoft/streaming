package chain

import (
	"github.com/mandelsoft/goutils/sliceutils"
	"github.com/mandelsoft/streaming/internal"
)

type Transformer[I, O any] = func(in []I) []O

func TransformStep[I, O any](m Transformer[I, O], name ...string) Step[I, O] {
	return &step[I, O]{internal.TransformStep(ConvertTransformer[any, any](m), name...)}
}

func Transformed[I, O any](m Transformer[I, O], name ...string) Chain[I, O] {
	return AddTransform[O, I, I](nil, m, name...)
}

func AddTransform[N, I, O any](base Chain[I, O], m Transformer[O, N], name ...string) Chain[I, N] {
	f := ConvertTransformer[any, any](m)
	c := chainImpl(base).Transform(f, name...)
	return convertChain[I, N](c)
}

////////////////////////////////////////////////////////////////////////////////

func ConvertTransformer[NI, NO, I, O any](f Transformer[I, O]) Transformer[NI, NO] {
	return func(in []NI) []NO {
		out := f(sliceutils.Convert[I](in))
		return sliceutils.Convert[NO](out)
	}
}
