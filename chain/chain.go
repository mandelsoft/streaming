package chain

import (
	"context"
	"fmt"
	"github.com/mandelsoft/goutils/general"
	"iter"
	"sync/atomic"

	"github.com/mandelsoft/goutils/iterutils"
	"github.com/mandelsoft/streaming/elem"
	"github.com/mandelsoft/streaming/processing"
)

type Chain[I, O any] interface {
	impl() *chain

	Execute(ctx context.Context, seq iter.Seq[I], name ...string) iter.Seq[O]
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

func (c *chain) String() string {
	return fmt.Sprintf("chain[%s<-%s]", c.step, c.chain)
}

func (c *chain) impl() *chain {
	return c
}

func (c *chain) Execute(ctx context.Context, seq iter.Seq[any], name ...string) iter.Seq[any] {
	return c.sequential().Run(ctx, iterutils.Convert[any](seq))
}

////////////////////////////////////////////////////////////////////////////////

func (c *chain) sequential() executor {
	if c.chain != nil {
		return &sequentialExecutor{c.chain.sequential(), sequentialStepExecutor{c.step}}
	}
	return &sequentialStepExecutor{c.step}
}

func (c *chain) parallel(f executionFactory) executionFactory {
	if c.step != nil {
		f = c.step.parallel(f)
	}
	if c.chain != nil {
		f = c.chain.parallel(f)
	}
	return f
}

////////////////////////////////////////////////////////////////////////////////

type sequentialStepExecutor struct {
	step Step
}

func newSequentialStepExecutor(s *sequentialStep) executor {
	return &sequentialStepExecutor{s}
}

func (e *sequentialStepExecutor) Run(ctx context.Context, seq iter.Seq[any]) iter.Seq[any] {

	if e.step != nil {
		return e.step.sequential().Run(nil, seq)
	}
	return seq
}

type sequentialExecutor struct {
	parent executor
	step   sequentialStepExecutor
}

func (e *sequentialExecutor) Run(ctx context.Context, seq iter.Seq[any]) iter.Seq[any] {
	return e.step.Run(nil, e.parent.Run(nil, seq))
}

////////////////////////////////////////////////////////////////////////////////

type sequentialStepExecutionFactory struct {
	step *sequentialStep
	f    executionFactory
}

func (s *sequentialStepExecutionFactory) requestExecution(ctx context.Context, v *elem.Element) {
	s.f.getPool().Execute(newSequentialStepRequest(s.step.chain, v, s.f))
}

func (s *sequentialStepExecutionFactory) getPool() processing.Processing {
	return s.f.getPool()
}

func newSequentialStepExecutionFactory(s *sequentialStep, f executionFactory) executionFactory {
	return &sequentialStepExecutionFactory{s, f}
}

////////////////////////////////////////////////////////////////////////////////

type sequentialStepRequest struct {
	chain *chain
	value *elem.Element
	f     executionFactory
}

var _ processing.Request = (*sequentialStepRequest)(nil)

func newSequentialStepRequest(chain *chain, value *elem.Element, f executionFactory) *sequentialStepRequest {
	return &sequentialStepRequest{chain, value, f}
}

func (s *sequentialStepRequest) Execute(ctx context.Context) {
	var result []any
	for v := range s.chain.sequential().Run(ctx, iterutils.SingleValue(s.value.V())) {
		result = append(result, v)
	}
	for i, v := range result {
		s.f.requestExecution(ctx, elem.NewElement(s.value.Id().Add2(i, len(result)), v))
	}
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
