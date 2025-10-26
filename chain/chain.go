package chain

import (
	"context"
	"fmt"
	"github.com/mandelsoft/goutils/general"
	"iter"
	"sync/atomic"

	"github.com/mandelsoft/goutils/iterutils"
	"github.com/mandelsoft/streaming/processing"
)

type Chain[I, O any] interface {
	impl() *chain

	Execute(ctx context.Context, seq iter.Seq[I], name ...string) iter.Seq[O]
	ExecuteWithConfig(ctx context.Context, cfg any, seq iter.Seq[I], name ...string) iter.Seq[O]
}

type executor interface {
	Run(ctx context.Context, seq iter.Seq[any]) iter.Seq[any]
}

func New[I any]() Chain[I, I] {
	return convertChain[I, I](&chain{})
}

////////////////////////////////////////////////////////////////////////////////

type chain struct {
	chain *chain
	step  Step
}

func chainImpl[I, O any](c Chain[I, O]) *chain {
	if c == nil {
		return nil
	}
	return c.impl()
}

func (c *chain) String() string {
	return fmt.Sprintf("chain[%s<-%s]", c.step, c.chain)
}

func (c *chain) impl() *chain {
	return c
}

func (c *chain) Execute(ctx context.Context, seq iter.Seq[any], name ...string) iter.Seq[any] {
	return c.sequential(ctx).Run(ctx, seq)
}

func (c *chain) ExecuteWithConfig(ctx context.Context, cfg any, seq iter.Seq[any], name ...string) iter.Seq[any] {
	return c.sequential(ctx).Run(WithConfig(ctx, cfg), seq)
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

func convertChain[I, O any](c *chain) Chain[I, O] {
	return &convertingChain[I, O]{c}
}

type convertingChain[I, O any] struct {
	chain *chain
}

var _ Chain[int, string] = (*convertingChain[int, string])(nil)

func (c *convertingChain[I, O]) impl() *chain {
	return c.chain
}

func (c *convertingChain[I, O]) Execute(ctx context.Context, seq iter.Seq[I], name ...string) iter.Seq[O] {
	return iterutils.Convert[O](c.chain.Execute(ctx, iterutils.Convert[any](seq), name...))
}
func (c *convertingChain[I, O]) ExecuteWithConfig(ctx context.Context, cfg any, seq iter.Seq[I], name ...string) iter.Seq[O] {
	return iterutils.Convert[O](c.chain.ExecuteWithConfig(ctx, cfg, iterutils.Convert[any](seq), name...))
}

////////////////////////////////////////////////////////////////////////////////

func Mapped[I, O any](f Mapper[I, O], name ...string) Chain[I, O] {
	return AddMap(New[I](), f, name...)
}

func Exploded[I, O any](f Exploder[I, O], name ...string) Chain[I, O] {
	return AddExplode(New[I](), f, name...)
}

func Filtered[I any](f Filter[I], name ...string) Chain[I, I] {
	return AddFilter(New[I](), f, name...)
}

func Aggregated[I, O any](f Aggregator[I, O], name ...string) Chain[I, O] {
	return AddAggregation[O](New[I](), f, name...)
}

func Sorted[I any](f CompareFunc[I], name ...string) Chain[I, I] {
	return AddSort[I, I](New[I](), f, name...)
}

func Parallel[I, O any](f Chain[I, O], p processing.Processing, name ...string) Chain[I, O] {
	return AddParallel(New[I](), f, p, name...)
}

func Stable[I, O any](f Chain[I, O], p processing.Processing, name ...string) Chain[I, O] {
	return AddStable(New[I](), f, p, name...)
}

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
