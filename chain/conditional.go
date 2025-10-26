package chain

import (
	"github.com/mandelsoft/streaming/internal"
)

type Condition = internal.Condition

func ConditionalChain[I any](m Condition, n Chain[I, I], name ...string) Chain[I, I] {
	return AddConditional[I, I](nil, m, n, name...)
}

func AddConditional[I, O any](base Chain[I, O], m Condition, n Chain[O, O], name ...string) Chain[I, O] {
	c := chainImpl(base).Conditional(m, n.impl(), name...)
	return convertChain[I, O](c)
}
