package internal

import (
	"context"
	"fmt"
)

type Condition func(context.Context) bool

type conditionalStep struct {
	step
	cond  Condition
	chain *chain
}

func (s *conditionalStep) String() string {
	return fmt.Sprintf("conditionalStep[%s]", s.name)
}

func (s *conditionalStep) Renamed(name string) Step {
	n := *s
	n.name = name
	return &n
}

func (s *conditionalStep) sequential(ctx context.Context) executor {
	if s.cond != nil && s.cond(ctx) {
		return s.chain.sequential(ctx)
	}
	return nil
}

func (s *conditionalStep) parallel(ctx context.Context, factory executionFactory) executionFactory {
	if s.cond != nil && s.cond(ctx) {
		return s.chain.parallel(ctx, factory)
	}
	return factory
}

////////////////////////////////////////////////////////////////////////////////

var conditionalId = newDefaultName("Conditional")

func (c *chain) Conditional(m Condition, n Chain, name ...string) Chain {
	return &chain{c.clean(), &conditionalStep{conditionalId.Step(name...), m, n.impl()}}
}
