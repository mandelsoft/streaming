package chain

import (
	"github.com/mandelsoft/streaming/processing"
)

func Parallel[I, O any](n Chain[I, O], pool processing.Processing, name ...string) Chain[I, O] {
	return AddParallel[O, I, I](nil, n, pool, name...)
}

func AddParallel[N, I, O any](base Chain[I, O], p Chain[O, N], pool processing.Processing, name ...string) Chain[I, N] {
	c := chainImpl(base).Parallel(p.impl(), pool, name...)
	return convertChain[I, N](c)
}
