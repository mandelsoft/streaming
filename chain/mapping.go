package chain

import (
	"github.com/mandelsoft/streaming/internal"
)

type Mapper[I, O any] = internal.GMapper[I, O]

func MappingStep[I, O any](m Mapper[I, O], name ...string) Step[I, O] {
	return &step[I, I]{internal.MappingStep(ConvertMapper[any, any](m), name...)}
}

func Mapped[I, O any](m Mapper[I, O], name ...string) Chain[I, O] {
	return AddMap[O, I, I](nil, m, name...)
}

func AddMap[N, I, O any](base Chain[I, O], m Mapper[O, N], name ...string) Chain[I, N] {
	c := chainImpl(base).Map(ConvertMapper[any, any](m), name...)
	return convertChain[I, N](c)
}

////////////////////////////////////////////////////////////////////////////////

// ConvertMapper is a generic function that adapts a Mapper of one type pair to
// another using type conversions.
func ConvertMapper[NI, NO, I, O any](m Mapper[I, O]) internal.GMapper[NI, NO] {
	return func(i NI) NO {
		var in any = i
		var out any = m(in.(I))
		return out.(NO)
	}
}
