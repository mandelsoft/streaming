package chain

import (
	"github.com/mandelsoft/streaming/processing"
)

func Stable[I, O any](p Chain[I, O], pool processing.Processing, name ...string) Chain[I, O] {
	return AddStable[I, I, O](nil, p, pool, name...)
}

func AddStable[I, O, N any](base Chain[I, O], p Chain[O, N], pool processing.Processing, name ...string) Chain[I, N] {
	c := chainImpl(base).Stable(p.impl(), pool, name...)
	return convertChain[I, N](c)
}
