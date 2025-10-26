package chain

////////////////////////////////////////////////////////////////////////////////

func AddChain[N, I, O any](base Chain[I, O], m Chain[O, N], name ...string) Chain[I, N] {
	c := chainImpl(base).Add(m.impl(), name...)
	return convertChain[I, N](c)
}
