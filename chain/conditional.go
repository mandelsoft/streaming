package chain

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

func (c *chain) Conditional(m Condition, n Untyped, name ...string) Untyped {
	return conditionalWith(c, m, n.impl(), name...)
}

func ConditionalChain[I any](m Condition, n Chain[I, I], name ...string) Chain[I, I] {
	return AddConditional[I, I](nil, m, n, name...)
}

func AddConditional[I, O any](base Chain[I, O], m Condition, n Chain[O, O], name ...string) Chain[I, O] {
	c := conditionalWith(chainImpl(base), m, n.impl(), name...)
	return convertChain[I, O](c)
}

func conditionalWith(c *chain, m Condition, n *chain, name ...string) *chain {
	return &chain{c, &conditionalStep{conditionalId.Step(name...), m, n}}
}

////////////////////////////////////////////////////////////////////////////////
