package chain

func Sequential[I, O any](p Chain[I, O]) Chain[I, O] {
	return AddSequential[I, I, O](nil, p)
}

func AddSequential[I, O, N any](base Chain[I, O], p Chain[O, N]) Chain[I, N] {
	c := chainImpl(base).Sequential(p.impl())
	return convertChain[I, N](c)
}
