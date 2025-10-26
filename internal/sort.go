package internal

import (
	"context"
	"fmt"
	"iter"
	"slices"

	"github.com/mandelsoft/goutils/general"
	"github.com/mandelsoft/goutils/iterutils"
)

type CompareFunc = func(a, b any) int

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

func (c *chain) Sort(m CompareFunc, name ...string) Chain {
	return &chain{c.clean(), newSortStep(m, name...)}
}
