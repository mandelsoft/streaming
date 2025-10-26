package internal

import (
	"context"
	"fmt"
	"github.com/mandelsoft/streaming/elem"
	"github.com/mandelsoft/streaming/processing"
	"iter"
)

type parallelStep struct {
	nestedChain
}

var _ Step = (*parallelStep)(nil)

func (s *parallelStep) String() string {
	return fmt.Sprintf("parallelStep[%s]", s.name)
}

func (s *parallelStep) Renamed(name string) Step {
	return &parallelStep{*s.nestedChain.Renamed(name)}
}

func (s *parallelStep) sequential(context.Context) executor {
	return newParallelStepExecutor(s)
}

func (s *parallelStep) parallel(ctx context.Context, f executionFactory) executionFactory {
	return s.chain.parallel(ctx, f)
}

////////////////////////////////////////////////////////////////////////////////

var parallelId = newDefaultName("Parallel")

func (c *chain) Parallel(n Chain, pool processing.Processing, name ...string) Chain {
	return &chain{c.clean(), &parallelStep{nestedChain{parallelId.Step(name...), n.impl(), pool}}}
}

////////////////////////////////////////////////////////////////////////////////

type parallelStepExecutor struct {
	step    *nestedChain
	result  processing.Channel
	factory func(pool processing.Processing, result processing.Channel) executionFactory
}

func newParallelStepExecutor(step *parallelStep) *parallelStepExecutor {
	return &parallelStepExecutor{&step.nestedChain, step.pool.CreateChannel(), newGatherFactory}
}

func (e *parallelStepExecutor) Run(ctx context.Context, seq iter.Seq[any]) iter.Seq[any] {
	i := 0
	f := e.step.chain.parallel(ctx, e.factory(e.step.pool, e.result))
	for v := range seq {
		f.requestExecution(ctx, elem.NewElement(elem.NewId(i, 0), v))
		i++
	}
	f.requestExecution(ctx, elem.NewPropagationElement(i))

	return func(yield func(any) bool) {
		i := 0
		for {
			// fmt.Printf("receive %d\n", i)
			v, ok, err := e.result.Receive(ctx)
			if !ok || err != nil {
				break
			}
			// fmt.Printf("got %d: %s\n", i, v)
			i++
			if v.IsValid() {
				if !yield(v.V()) {
					return
				}
			}
		}
	}
}
