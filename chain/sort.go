package chain

import (
	"context"
	"fmt"
	"iter"
	"slices"

	"github.com/mandelsoft/goutils/general"
	"github.com/mandelsoft/goutils/iterutils"
)

type CompareFunc[I any] = func(a, b I) int

type sortStep struct {
	gatherStep
	compare general.CompareFunc[any]
}

func newSortStep(compare general.CompareFunc[any], name ...string) *sortStep {
	s := &sortStep{
		gatherStep: gatherStep{step: sortId.Step(name...)},
		compare:    compare,
	}
	s.all = s.sort
	return s
}

func (s *sortStep) String() string {
	return fmt.Sprintf("sortStep[%s]", s.name)
}

func (s *sortStep) Renamed(name string) Step {
	n := *s
	n.name = name
	n.all = n.sort
	return &n
}

func (s *sortStep) sort(ctx context.Context, in []any) iter.Seq[any] {
	slices.SortStableFunc(in, s.compare)
	return iterutils.For(in...)
}

////////////////////////////////////////////////////////////////////////////////

var sortId = newDefaultName("Sort")

func (c *chain) Sort(m CompareFunc[any], name ...string) Untyped {
	return sortWith(c, m, name...)
}

func AddSort[I, O any](base Chain[I, O], m CompareFunc[O], name ...string) Chain[I, O] {
	f := general.ConvertCompareFunc[any, O](m)
	c := sortWith(chainImpl(base), f, name...)
	return convertChain[I, O](c)
}

func sortWith(c *chain, m general.CompareFunc[any], name ...string) *chain {
	return &chain{c, newSortStep(m, name...)}
}
