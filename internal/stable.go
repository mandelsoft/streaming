package internal

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

func (s *stableStep) String() string {
	return fmt.Sprintf("stableStep[%s]", s.name)
}

func (s *stableStep) Renamed(name string) Step {
	return &stableStep{*s.nestedChain.Renamed(name)}
}

func (s *stableStep) sequential(context.Context) executor {
	return newStableStepExecutor(s)
}

func (s *stableStep) parallel(ctx context.Context, f executionFactory) executionFactory {
	return s.chain.parallel(ctx, f)
}

////////////////////////////////////////////////////////////////////////////////

var stableId = newDefaultName("Stable")

func (c *chain) Stable(n Chain, pool processing.Processing, name ...string) Chain {
	return &chain{c.clean(), &stableStep{nestedChain{stableId.Step(name...), n.impl(), pool}}}
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
