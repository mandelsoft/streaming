package chain

import (
	"github.com/mandelsoft/goutils/sliceutils"
	"github.com/mandelsoft/streaming/internal"
)

type Exploder[I, O any] internal.GExploder[I, O]

func Exploded[I, O any](m Exploder[I, O], name ...string) Chain[I, O] {
	return AddExplode[O, I, I](nil, m, name...)
}

func AddExplode[N, I, O any](base Chain[I, O], m Exploder[O, N], name ...string) Chain[I, N] {
	c := chainImpl(base).Explode(ConvertExploder[any, any](m), name...)
	return convertChain[I, N](c)
}

////////////////////////////////////////////////////////////////////////////////

// ConvertExploder converts an Exploder of type I to O into an Exploder of type
// NI to NO using type assertions and slice conversion.
func ConvertExploder[NI, NO, I, O any](m Exploder[I, O]) internal.GExploder[NI, NO] {
	return func(i NI) []NO {
		var in any = i
		var out = m(in.(I))
		return sliceutils.Convert[NO](out)
	}
}
