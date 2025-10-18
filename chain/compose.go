package chain

////////////////////////////////////////////////////////////////////////////////

var composeId = newDefaultName("Compose")

func (c *chain) Add(n Untyped, name ...string) Untyped {
	return composeWith(c, n.impl(), name...)
}

func AddChain[N, I, O any](base Chain[I, O], m Chain[O, N], name ...string) Chain[I, N] {
	c := composeWith(base.impl(), m.impl(), name...)
	return convertChain[I, N](c)
}

func composeWith(c *chain, m *chain, name ...string) *chain {
	n := composeId.Name(name...)
	var top *chain = nil
	ref := &top
	for m != nil {
		var s Step = nil
		if m.step != nil {
			s = m.step.Renamed(n)
		}
		n := &chain{nil, s}
		*ref = n
		ref = &n.chain
		m = m.chain
	}
	*ref = c
	return top
}
