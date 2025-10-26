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

func newSequentialStep(chain *chain, name ...string) *sequentialStep {
	s := &sequentialStep{gatherStep{step: sequentialId.Step(name...)}, chain}
	s.all = s.exec
	return s
}

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

func (c *chain) Sequential(n Chain, name ...string) Chain {
	return &chain{c.clean(), newSequentialStep(n.impl(), name...)}
}
