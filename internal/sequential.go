package internal

import (
	"context"
	"fmt"
	"iter"

	"github.com/mandelsoft/goutils/iterutils"
)

type sequentialStep struct {
	gatherStep
	chain *chain
}

var _ Step = (*sequentialStep)(nil)

func (s *sequentialStep) String() string {
	return fmt.Sprintf("sequentialStep[%s]", s.name)
}

func (s *sequentialStep) Renamed(name string) Step {
	n := *s
	n.name = name
	n.all = n.exec
	return &n
}

func (s *sequentialStep) exec(ctx context.Context, in []any) iter.Seq[any] {
	return s.chain.sequential(ctx).Run(ctx, iterutils.For(in...))
}

////////////////////////////////////////////////////////////////////////////////

var sequentialId = newDefaultName("Sequential")

func SequentialStep(n Chain, name ...string) Step {
	s := &sequentialStep{gatherStep{step: sequentialId.Step(name...)}, n.impl()}
	s.all = s.exec
	return s
}

func (c *chain) Sequential(n Chain, name ...string) Chain {
	return c.Step(SequentialStep(n, name...))
}
