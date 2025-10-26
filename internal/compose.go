package internal

////////////////////////////////////////////////////////////////////////////////

var composeId = newDefaultName("Compose")

func (c *chain) Add(n Chain, name ...string) Chain {
	prefix := composeId.Name(name...)
	var top *chain = nil
	ref := &top
	m := n.impl()
	for m != nil {
		var s Step = nil
		if m.step != nil {
			s = m.step.Renamed(prefix + "/" + m.step.GetName())
		}
		n := &chain{nil, s}
		*ref = n
		ref = &n.chain
		m = m.chain
	}
	*ref = c
	return top
}
