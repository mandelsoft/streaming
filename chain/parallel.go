package chain

import (
	"context"
	"fmt"
	"iter"
	"sync"

	"github.com/mandelsoft/streaming/elem"
	"github.com/mandelsoft/streaming/processing"
)

type nestedChain struct {
	step
	chain *chain
	pool  processing.Processing
}

func (c *nestedChain) Renamed(name string) *nestedChain {
	n := *c
	n.name = name
	return &n
}

type parallelStep struct {
	nestedChain
}

var _ Step = (*parallelStep)(nil)

func (p *parallelStep) String() string {
	return fmt.Sprintf("parallel")
}

func (s *parallelStep) Renamed(name string) Step {
	return &parallelStep{*s.nestedChain.Renamed(name)}
}

func (p *parallelStep) sequential() executor {
	return newParallelStepExecutor(p)
}

func (p *parallelStep) parallel(f executionFactory) executionFactory {
	return p.chain.parallel(f)
}

////////////////////////////////////////////////////////////////////////////////

var parallelId = newDefaultName("Parallel")

func (c *chain) Parallel(n Untyped, pool processing.Processing, name ...string) Untyped {
	return parallelWith(c, n.impl(), pool, name...)
}

func AddParallel[I, O, N any](base Chain[I, O], p Chain[O, N], pool processing.Processing, name ...string) Chain[I, N] {
	c := parallelWith(base.impl(), p.impl(), pool, name...)
	return convertChain[I, N](c)
}

func parallelWith(c *chain, p *chain, pool processing.Processing, name ...string) *chain {
	return &chain{c, &parallelStep{nestedChain{parallelId.Step(name...), p, pool}}}
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
	f := e.step.chain.parallel(e.factory(e.step.pool, e.result))
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

type executionFactory interface {
	requestExecution(ctx context.Context, v *elem.Element)
	getPool() processing.Processing
}

type gatherFactory struct {
	pool          processing.Processing
	resultChannel processing.Channel
	result        elem.Elements

	lock sync.Mutex
}

var _ executionFactory = (*gatherFactory)(nil)

func newGatherFactory(pool processing.Processing, result processing.Channel) executionFactory {
	return _newGatherFactory(pool, result)
}

func _newGatherFactory(pool processing.Processing, result processing.Channel) *gatherFactory {
	return &gatherFactory{pool: pool, resultChannel: result, result: elem.NewElements()}
}

func (g *gatherFactory) getPool() processing.Processing {
	return g.pool
}

func (g *gatherFactory) requestExecution(ctx context.Context, v *elem.Element) {
	g.lock.Lock()
	defer g.lock.Unlock()

	//fmt.Printf("gather: %s\n", v)
	g.result.Add(v)
	g.resultChannel.Send(ctx, v)
	if g.result.IsComplete() {
		//fmt.Printf("complete -> close\n")
		g.resultChannel.Close()
	} else {
		//fmt.Printf("not complete\n")
	}
}

func (g *gatherFactory) results() processing.Channel {
	return g.resultChannel
}
