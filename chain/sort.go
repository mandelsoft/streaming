package chain

import (
	"github.com/mandelsoft/goutils/general"
)

type CompareFunc[I any] = func(a, b I) int

func Sorted[I any](m CompareFunc[I], name ...string) Chain[I, I] {
	return AddSort[I, I](nil, m, name...)
}

func AddSort[I, O any](base Chain[I, O], m CompareFunc[O], name ...string) Chain[I, O] {
	f := ConvertCompareFunc[any, O](m)
	c := chainImpl(base).Sort(f, name...)
	return convertChain[I, O](c)
}

////////////////////////////////////////////////////////////////////////////////

func ConvertCompareFunc[N, I any](f general.CompareFunc[I]) CompareFunc[N] {
	return general.ConvertCompareFunc[N](f)
}
