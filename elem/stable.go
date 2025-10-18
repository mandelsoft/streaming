package elem

type StableChannel struct {
	c     chan *Element
	elems Elements
	it    Iterator
}

func NewStableChannel(elems Elements) *StableChannel {
	return &StableChannel{make(chan *Element), elems, elems.Iterator()}
}

func (c *StableChannel) SetInputSize(n int) {
	c.elems.SetKnownSize(n)
}

func (c *StableChannel) Get() *Element {
	for {
		avail, done := c.it.HasNext()
		if avail {
			return c.it.Next()
		}
		if done {
			return nil
		}
		next, ok := <-c.c
		if !ok {
			return nil
		}
		c.elems.Add(next)
	}
}

func (c *StableChannel) Put(e *Element) {
	c.c <- e
}
func (c *StableChannel) Iterator(yield func(v *Element) bool) {
	for {
		e := c.Get()
		if e == nil {
			return
		}
		if !yield(e) {
			return
		}
	}
}

func (c *StableChannel) Close() {
	close(c.c)
}
