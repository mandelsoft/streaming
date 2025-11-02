package internal

import (
	"context"
	"fmt"
	"github.com/mandelsoft/streaming/elem"
	"github.com/mandelsoft/streaming/processing"
	"iter"
	"sync/atomic"

	"github.com/mandelsoft/goutils/general"
)

type executor interface {
	Run(ctx context.Context, seq iter.Seq[any]) iter.Seq[any]
}

type executionFactory interface {
	requestExecution(ctx context.Context, v *elem.Element)
	getPool() processing.Processing
}

////////////////////////////////////////////////////////////////////////////////

type chain struct {
	chain *chain
	step  Step
}

func New() Chain {
	return &chain{}
}

func (c *chain) String() string {
	return fmt.Sprintf("chain[%s<-%s]", c.step, c.chain)
}

func (c *chain) impl() *chain {
	return c
}

func (c *chain) Execute(ctx context.Context, seq iter.Seq[any], name ...string) iter.Seq[any] {
	return func(yield func(any) bool) {
		for e := range c.sequential(ctx).Run(ctx, seq) {
			if !yield(e) {
				return
			}
		}
	}
}

func (c *chain) ExecuteWithConfig(ctx context.Context, cfg any, seq iter.Seq[any], name ...string) iter.Seq[any] {
	return c.sequential(ctx).Run(WithConfig(ctx, cfg), seq)
}

func (c *chain) clean() *chain {
	if c == nil {
		return nil
	}
	if c.chain.clean() == nil && c.step == nil {
		return nil
	}
	return c
}

////////////////////////////////////////////////////////////////////////////////

func (c *chain) sequential(ctx context.Context) executor {
	if c.chain != nil {
		return &sequentialExecutor{c.chain.sequential(ctx), sequentialStepExecutor{c.step}}
	}
	return newSequentialStepExecutor(c.step)
}

func (c *chain) parallel(ctx context.Context, f executionFactory) executionFactory {
	if c.step != nil {
		f = c.step.parallel(ctx, f)
	}
	if c.chain != nil {
		f = c.chain.parallel(ctx, f)
	}
	return f
}

////////////////////////////////////////////////////////////////////////////////

type sequentialStepExecutor struct {
	step Step
}

func newSequentialStepExecutor(s Step) executor {
	return &sequentialStepExecutor{s}
}

func (e *sequentialStepExecutor) Run(ctx context.Context, seq iter.Seq[any]) iter.Seq[any] {

	if e.step != nil {
		exec := e.step.sequential(ctx)
		if exec != nil {
			return e.step.sequential(ctx).Run(ctx, seq)
		}
	}
	return seq
}

type sequentialExecutor struct {
	parent executor
	step   sequentialStepExecutor
}

func (e *sequentialExecutor) Run(ctx context.Context, seq iter.Seq[any]) iter.Seq[any] {
	return e.step.Run(ctx, e.parent.Run(ctx, seq))
}

////////////////////////////////////////////////////////////////////////////////

type defaultName struct {
	id   atomic.Int64
	name string
}

func newDefaultName(name string) *defaultName {
	return &defaultName{name: name}
}

func (d *defaultName) Name(name ...string) string {
	n := general.Optional(name...)
	if len(n) > 0 {
		return n
	}
	return fmt.Sprintf("%s-%d", d.name, d.id.Add(1))
}

func (d *defaultName) Step(name ...string) step {
	return step{general.OptionalDefaulted(d.name, name...), d.Name(name...)}
}
