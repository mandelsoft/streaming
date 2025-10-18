package elem

import "fmt"

type Element struct {
	id      Id
	v       any
	valid   bool
	propMax int
}

func NewElement(id Id, v any) *Element {
	return &Element{id, v, true, 0}
}

func NewPropagationElement(propMax int) *Element {
	return &Element{valid: false, propMax: propMax}
}

func (e *Element) SetInvalid() {
	e.v = nil
	e.valid = false
}

func (e *Element) SetMax(n int) {
	if e == nil || e.id[0].Max() > 0 {
		return
	}
	e.id = e.id.WithMax(n)
}

func (e *Element) Propagation() int {
	return e.propMax
}

func (e *Element) String() string {
	if e.propMax > 0 {
		return fmt.Sprintf("propagate size %d", e.propMax)
	}
	return fmt.Sprintf("%s[%v]", e.id, e.v)
}

func (e *Element) New(v any) *Element {
	if !e.valid {
		v = nil
	}
	return &Element{e.id, v, e.valid, e.propMax}
}

func (e *Element) NewInvalid() *Element {
	return &Element{e.id, nil, false, 0}
}

func (e *Element) IsValid() bool {
	return e.valid
}

func (e *Element) Id() Id {
	return e.id
}

func (e *Element) V() any {
	return e.v
}

func (e *Element) Level() int {
	return len(e.id) - 1
}

////////////////////////////////////////////////////////////////////////////////

type Elements interface {
	SetKnown()
	Elements(yield func(*Element) bool)
	Values(yield func(any) bool)
	IsComplete() bool
	Add(v *Element)
	Iterator() Iterator
	add(i int, v *Element)
	SetKnownSize(n int)
}

type entry struct {
	elem *Element
	sub  *elements
	max  int
}

func newEntry() *entry {
	return &entry{}
}

func (e *entry) IsKnown() bool {
	return e != nil && (e.elem != nil || e.sub != nil)
}

func (e *entry) IsReady() bool {
	return e != nil && e.elem != nil
}

func (e *entry) IsValid() bool {
	return e != nil && e.elem != nil && e.elem.IsValid()
}

func (e *entry) Elements(yield func(v *Element) bool) {
	if (e.elem != nil) && !yield(e.elem) {
		return
	}
	e.sub.Elements(yield)
}

func (e *entry) IsComplete() bool {
	if e == nil || (e.elem == nil && e.sub == nil) {
		return false
	}
	if e.sub == nil {
		return e.elem != nil
	}
	return e.sub.IsComplete()
}

func (e *entry) setMax(n int) *entry {
	if e != nil {
		e.elem.SetMax(n)
		e.max = n
	}
	return e
}
func (e *entry) add(i int, v *Element) {
	if i == v.Level() {
		e.elem = v
		if e.max > 0 {
			e.elem.SetMax(e.max)
		}
	} else {
		if e.sub == nil {
			e.sub = &elements{}
		}
		e.sub.add(i+1, v)
	}
}

////////////////////////////////////////////////////////////////////////////////

type elements struct {
	unknown bool
	sub     []*entry
}

var _ Elements = (*elements)(nil)

func NewElements() Elements {
	return &elements{true, nil}
}

func NewKnownElements(n int) Elements {
	return &elements{false, make([]*entry, n)}
}

func (e *elements) SetKnown() {
	e.unknown = false
}

func (e *elements) SetKnownSize(n int) {
	if e.unknown {
		if len(e.sub) < n {
			r := make([]*entry, n)
			copy(r, e.sub)
			e.sub = r
		}
		for i, v := range e.sub {
			e.sub[i] = v.setMax(n)
		}
		e.SetKnown()
	}
}

func (e *elements) Iterator() Iterator {
	return newIterator(e)
}

func (e *elements) IsComplete() bool {
	if e.sub == nil || e.unknown {
		if e.unknown {
			//fmt.Printf("size unknown\n")
		} else {
			//fmt.Printf("no requests found\n")
		}
		return false
	}
	for i, s := range e.sub {
		if !s.IsComplete() {
			_ = i
			//fmt.Printf("index %d not complete\n", i)
			return false
		}
	}
	return true
}

func (e *elements) Add(v *Element) {
	l := v.Propagation()
	if l > 0 {
		e.SetKnownSize(l)
	} else {
		if e.unknown && v.id[0].Max() > 0 {
			e.SetKnownSize(v.id[0].Max())
		}
		e.add(0, v)
	}
}

func (e *elements) add(i int, v *Element) {
	sublevel := v.id[i]
	if e.sub == nil {
		e.sub = make([]*entry, sublevel.max)
	}

	if e.unknown {
		for sublevel.no >= len(e.sub) {
			e.sub = append(e.sub, nil)
		}
	}

	if e.sub[sublevel.no] == nil {
		e.sub[sublevel.no] = newEntry()
	}
	e.sub[sublevel.no].add(i, v)
}

func (e *elements) Elements(yield func(*Element) bool) {
	for _, s := range e.sub {
		if s.IsValid() || s.sub == nil {
			if !yield(s.elem) {
				return
			}
		} else {
			for n := range s.Elements {
				if n.IsValid() {
					if !yield(n) {
						return
					}
				}
			}
		}
	}
}

func (e *elements) Values(yield func(any) bool) {
	for _, s := range e.sub {
		if s.IsValid() {
			if !yield(s.elem.V()) {
				return
			}
		} else {
			for n := range s.Elements {
				if n.IsValid() {
					if !yield(n.V()) {
						return
					}
				}
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

type level struct {
	no    int
	elems **elements
	done  bool
}

func (l *level) IsKnown() bool {
	return (*l.elems).sub[l.no].IsKnown()
}

func (l *level) IsReady() bool {
	return (*l.elems).sub[l.no].IsReady()
}

func (l *level) IsValid() bool {
	return (*l.elems).sub[l.no].IsValid()
}

func (l *level) Element() *Element {
	return (*l.elems).sub[l.no].elem
}

type state []*level

func (s *state) endReached() bool {
	l := (*s)[len(*s)-1]

	if len(*s) != 1 {
		return false
	}
	if !l.done {
		return false
	}
	fmt.Printf("%d %d\n", l.no, len((*l.elems).sub)-1)
	return l.no == len((*l.elems).sub)-1
}

func (s *state) current() *level {
	if len(*s) == 0 || (*s)[0].no < 0 {
		return nil
	}
	return (*s)[len(*s)-1]
}

func (s *state) next() *level {
	for {
		l := (*s)[len(*s)-1]

		if !l.done {
			// check known
			if !l.IsKnown() {
				return nil
			}
			l.done = true
			if l.IsReady() {
				return l
			}
			// down
			*s = append(*s, &level{-1, &(*l.elems).sub[l.no].sub, true})
			continue
		}
		if len(*s) == 1 && l.no == len((*l.elems).sub)-1 {
			// don't do the last increment to be prepared for unknown top level size
			return nil
		}
		l.no++
		if l.no < len((*l.elems).sub) {
			// further
			if !l.IsKnown() {
				l.done = false
				return nil
			}
			if l.IsReady() {
				l.done = true
				return l
			}
			l.done = false
		} else {
			// up
			*s = (*s)[0 : len(*s)-1]
		}
	}
}

type Iterator interface {
	HasNext() (avail bool, completed bool)
	Next() *Element
	HasEndReached() bool
}

type iterator struct {
	elements *elements
	state    state
	current  *level
}

var _ Iterator = (*iterator)(nil)

func newIterator(e *elements) *iterator {
	return &iterator{e, state{&level{-1, &e, true}}, nil}
}

func (i *iterator) HasEndReached() bool {
	if i.current != nil {
		return false
	}
	return i.state.endReached()
}

func (i *iterator) HasNext() (avail bool, completed bool) {
	for {
		if i.current == nil {
			i.current = i.state.next()
		}
		if i.current == nil {
			return false, !i.elements.unknown && i.HasEndReached()
		}

		if i.current.IsValid() {
			return true, false
		}
		i.current = nil // skip invalid entry
	}
}

func (i *iterator) Next() *Element {
	if i.current == nil {
		return nil
	}
	v := i.current.Element()
	i.current = nil
	return v
}

////////////////////////////////////////////////////////////////////////////////

type single struct {
	base Id
	elem *elements
}

var _ Elements = (*single)(nil)

func NewSingle(base Id) Elements {
	return &single{base, &elements{false, make([]*entry, 1)}}
}

func (s *single) SetKnownSize(n int) {
	if n > 1 {
		panic(fmt.Sprintf("invalid size %d for single", n))
	}
}

func (s *single) SetKnown() {
}

func (s *single) Elements(yield func(*Element) bool) {
	r := s.elem.sub[0]
	if r != nil {
		if r.IsReady() {
			yield(r.elem)
		} else {
			for e := range r.Elements {
				if !yield(e) {
					return
				}
			}
		}
	}
}

func (s *single) Values(yield func(any) bool) {
	r := s.elem.sub[0]
	if r != nil {
		if r.IsReady() {
			if r.IsValid() {
				yield(r.elem.V())
			}
		} else {
			for e := range r.Elements {
				if e.IsValid() && !yield(e.V()) {
					return
				}
			}
		}
	}
}

func (s *single) IsComplete() bool {
	return s.elem.sub[0].IsComplete()
}

func (s *single) Add(v *Element) {
	for i, e := range s.base {
		if (v.id[i].no != e.no) || (v.id[i].max != e.max) {
			panic(fmt.Sprintf("unexpected element %s for base %s", v.id, s.base))
		}
	}
	s.add(len(s.base)-1, v)
}

func (s *single) add(i int, v *Element) {
	if s.elem.sub[0] == nil {
		s.elem.sub[0] = newEntry()
	}
	s.elem.sub[0].add(i, v)
}

func (s *single) Iterator() Iterator {
	return newIterator(s.elem)
}
