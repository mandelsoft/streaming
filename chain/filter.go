package chain

import (
	"context"
	"fmt"
	"github.com/mandelsoft/streaming/elem"
	"github.com/mandelsoft/streaming/processing"
	"iter"
)

type Filter[I any] func(I) bool

type filterStep struct {
	step
	filter Filter[any]
}

func (s *filterStep) String() string {
	return fmt.Sprintf("filterStep[%s]", s.name)
}

func (s *filterStep) Renamed(name string) Step {
	n := *s
	n.name = name
	return &n
}

func (s *filterStep) sequential() executor {
	return &filterStepExecutor{s}
}

func (s *filterStep) parallel(f executionFactory) executionFactory {
	return &filterFactory{s, f}
}

////////////////////////////////////////////////////////////////////////////////

var filterId = newDefaultName("Filter")

func (c *chain) Filter(m Filter[any], name ...string) Untyped {
	return filterWith(c, m, name...)
}

func AddFilter[I, O any](base Chain[I, O], m Filter[O], name ...string) Chain[I, O] {
	c := filterWith(base.impl(), ConvertFilter[any](m), name...)
	return convertChain[I, O](c)
}

func filterWith(c *chain, m Filter[any], name ...string) *chain {
	return &chain{c, &filterStep{filterId.Step(name...), m}}
}

////////////////////////////////////////////////////////////////////////////////

type filterStepExecutor struct {
	step *filterStep
}

var _ executor = (*filterStepExecutor)(nil)

func (m *filterStepExecutor) Run(ctx context.Context, seq iter.Seq[any]) iter.Seq[any] {
	return func(yield func(any) bool) {
		for v := range seq {
			ok := m.step.filter(v)
			if ok && !yield(v) {
				return
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

type filterFactory struct {
	step    *filterStep
	consume executionFactory
}

var _ executionFactory = (*filterFactory)(nil)

func (f *filterFactory) getPool() processing.Processing {
	return f.consume.getPool()
}

func (f *filterFactory) requestExecution(ctx context.Context, v *elem.Element) {
	f.consume.getPool().Execute(&filterRequest{v, f}, f.step.name)
}

type filterRequest struct {
	value   *elem.Element
	factory *filterFactory
}

var _ processing.Request = (*filterRequest)(nil)

func (r *filterRequest) Execute(ctx context.Context) {
	v := r.value.V()
	if r.value.IsValid() {
		if !r.factory.step.filter(v) {
			r.value.SetInvalid()
		}
	}

	r.factory.consume.requestExecution(ctx, r.value)
}

////////////////////////////////////////////////////////////////////////////////

// ConvertFilter adapts a Filter of type I to a Filter of type N using type
// assertions for compatibility in chains.
func ConvertFilter[N, I any](m Filter[I]) Filter[N] {
	return func(i N) bool {
		var in any = i
		return m(in.(I))
	}
}
