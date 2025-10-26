package chain

import (
	"github.com/mandelsoft/streaming/internal"
)

func SequentialStep[I, O any](p Chain[I, O], name ...string) Step[I, O] {
	return &step[I, O]{internal.SequentialStep(p.impl(), name...)}
}

func Sequential[I, O any](p Chain[I, O]) Chain[I, O] {
	return AddSequential[I, I, O](nil, p)
}

func AddSequential[I, O, N any](base Chain[I, O], p Chain[O, N]) Chain[I, N] {
	c := chainImpl(base).Sequential(p.impl())
	return convertChain[I, N](c)
}
