package chain

import (
	"context"
	"fmt"

	"github.com/mandelsoft/streaming/elem"
	"github.com/mandelsoft/streaming/processing"
)

type stableStep struct {
	nestedChain
}

var _ Step = (*stableStep)(nil)

func (p *stableStep) String() string {
	return fmt.Sprintf("stable")
}

func (s *stableStep) Renamed(name string) Step {
	return &stableStep{*s.nestedChain.Renamed(name)}
}

func (p *stableStep) sequential() executor {
	return newStableStepExecutor(p)
}

func (p *stableStep) parallel(f executionFactory) executionFactory {
	return p.chain.parallel(f)
}

////////////////////////////////////////////////////////////////////////////////

var stableId = newDefaultName("Stable")

func (c *chain) Stable(n Untyped, pool processing.Processing, name ...string) Untyped {
	return stableWith(c, n.impl(), pool, name...)
}

func AddStable[I, O, N any](base Chain[I, O], p Chain[O, N], pool processing.Processing, name ...string) Chain[I, N] {
	c := stableWith(base.impl(), p.impl(), pool, name...)
	return convertChain[I, N](c)
}

func stableWith(c *chain, p *chain, pool processing.Processing, name ...string) *chain {
	return &chain{c, &stableStep{nestedChain{stableId.Step(name...), p, pool}}}
}

func newStableStepExecutor(step *stableStep) *parallelStepExecutor {
	return &parallelStepExecutor{&step.nestedChain, step.pool.CreateChannel(), newStableFactory}
}

////////////////////////////////////////////////////////////////////////////////

type stableFactory struct {
	gatherFactory
}

var _ executionFactory = (*stableFactory)(nil)

func newStableFactory(pool processing.Processing, result processing.Channel) executionFactory {
	return &stableFactory{*_newGatherFactory(pool, result)}
}

func (g *stableFactory) requestExecution(ctx context.Context, v *elem.Element) {
	g.lock.Lock()
	defer g.lock.Unlock()

	// fmt.Printf("gather %s(%s)\n", v.Id(), v.V())
	g.result.Add(v)
	if g.result.IsComplete() {
		for e := range g.result.Elements {
			g.resultChannel.Send(ctx, e)
		}
		// fmt.Printf("close\n")
		g.resultChannel.Close()
	}
}
