package internal

import (
	"context"
	"fmt"
	"iter"
	"sort"

	"github.com/mandelsoft/goutils/general"
	"github.com/mandelsoft/goutils/iterutils"
)

type CompareFunc = func(a, b any) int

type sortStep struct {
	gatherStep
	compare general.CompareFunc[any]
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
	sort.Stable(&slice{data: in, cmp: s.compare})
	return iterutils.For(in...)
}

type slice struct {
	data []any
	cmp  general.CompareFunc[any]
}

var _ sort.Interface = (*slice)(nil)

func (s *slice) Len() int {
	return len(s.data)
}

func (s *slice) Less(i, j int) bool {
	return s.cmp(s.data[i], s.data[j]) < 0
}

func (s *slice) Swap(i, j int) {
	s.data[i], s.data[j] = s.data[j], s.data[i]
}

////////////////////////////////////////////////////////////////////////////////

var sortId = newDefaultName("Sort")

func SortStep(m CompareFunc, name ...string) Step {
	s := &sortStep{
		gatherStep: gatherStep{step: sortId.Step(name...)},
		compare:    m,
	}
	s.all = s.sort
	return s
}

func (c *chain) Sort(m CompareFunc, name ...string) Chain {
	return c.Step(SortStep(m, name...))
}
