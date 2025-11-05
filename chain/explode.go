package chain

import (
	"context"

	"github.com/mandelsoft/goutils/sliceutils"
	"github.com/mandelsoft/streaming/internal"
)

type Exploder[I, O any] = internal.GExploder[I, O]
type ExploderFactory[I, O any] = internal.GExploderFactory[I, O]

func ExploderFactoryFor[I, O any](e Exploder[I, O]) ExploderFactory[I, O] {
	return func(ctx context.Context) Exploder[I, O] {
		return e
	}
}

func ExplodeStep[I, O any](m Exploder[I, O], name ...string) Step[I, O] {
	return &step[I, I]{internal.ExplodeStep(ConvertExploder[any, any](m), name...)}
}

func ExplodeStepByFactory[I, O any](m ExploderFactory[I, O], name ...string) Step[I, O] {
	return &step[I, I]{internal.ExplodeStepByFactory(ConvertExploderFactory[any, any](m), name...)}
}

func Exploded[I, O any](m Exploder[I, O], name ...string) Chain[I, O] {
	return AddExplode[O, I, I](nil, m, name...)
}

func ExplodedByFactory[I, O any](m ExploderFactory[I, O], name ...string) Chain[I, O] {
	return AddExplodeByFactory[O, I, I](nil, m, name...)
}

func AddExplode[N, I, O any](base Chain[I, O], m Exploder[O, N], name ...string) Chain[I, N] {
	c := chainImpl(base).Explode(ConvertExploder[any, any](m), name...)
	return convertChain[I, N](c)
}

func AddExplodeByFactory[N, I, O any](base Chain[I, O], m ExploderFactory[O, N], name ...string) Chain[I, N] {
	c := chainImpl(base).ExplodeByFactory(ConvertExploderFactory[any, any](m), name...)
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

func ConvertExploderFactory[NI, NO, I, O any](m ExploderFactory[I, O]) internal.GExploderFactory[NI, NO] {
	return func(ctx context.Context) internal.GExploder[NI, NO] {
		return ConvertExploder[NI, NO, I, O](m(ctx))
	}
}
