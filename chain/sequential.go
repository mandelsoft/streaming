package chain

import (
	"context"
	"fmt"
	"iter"

	iterutils "github.com/mandelsoft/goutils/iterutils"
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

func (p *sequentialStep) String() string {
	return fmt.Sprintf("sequential")
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

func (c *chain) Sequential(n Untyped, name ...string) Untyped {
	return sequentialWith(c, n.impl())
}

func AddSequential[I, O, N any](base Chain[I, O], p Chain[O, N]) Chain[I, N] {
	c := sequentialWith(chainImpl(base), p.impl())
	return convertChain[I, N](c)
}

func sequentialWith(c *chain, p *chain) *chain {
	return &chain{c, newSequentialStep(p)}
}
