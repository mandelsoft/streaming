package chain

import (
	"github.com/mandelsoft/streaming/internal"
	"github.com/mandelsoft/streaming/processing"
)

func ParallelStep[I any](p Chain[I, I], pool processing.Processing, name ...string) Step[I, I] {
	return &step[I, I]{internal.ParallelStep(p.impl(), pool, name...)}
}

func Parallel[I, O any](n Chain[I, O], pool processing.Processing, name ...string) Chain[I, O] {
	return AddParallel[O, I, I](nil, n, pool, name...)
}

func AddParallel[N, I, O any](base Chain[I, O], p Chain[O, N], pool processing.Processing, name ...string) Chain[I, N] {
	c := chainImpl(base).Parallel(p.impl(), pool, name...)
	return convertChain[I, N](c)
}
