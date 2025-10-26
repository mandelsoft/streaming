package chain

import (
	"github.com/mandelsoft/streaming/internal"
)

type Filter[I any] internal.GFilter[I]

func Filtered[I any](m Filter[I], name ...string) Chain[I, I] {
	return AddFilter[I, I](nil, m, name...)
}

func AddFilter[I, O any](base Chain[I, O], m Filter[O], name ...string) Chain[I, O] {
	c := chainImpl(base).Filter(ConvertFilter[any](m), name...)
	return convertChain[I, O](c)
}

////////////////////////////////////////////////////////////////////////////////

// ConvertFilter adapts a Filter of type I to a Filter of type N using type
// assertions for compatibility in chains.
func ConvertFilter[N, I any](m Filter[I]) internal.GFilter[N] {
	return func(i N) bool {
		var in any = i
		return m(in.(I))
	}
}
