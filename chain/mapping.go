package chain

import (
	"context"
	"fmt"
	"github.com/mandelsoft/streaming/elem"
	"github.com/mandelsoft/streaming/processing"
	"iter"
)

type Mapper[I, O any] func(I) O

type mappingStep struct {
	step
	mapper Mapper[any, any]
}

func (s *mappingStep) String() string {
	return fmt.Sprintf("mappingStep[%s]", s.name)
}

func (s *mappingStep) Renamed(name string) Step {
	n := *s
	n.name = name
	return &n
}

func (s *mappingStep) sequential(context.Context) executor {
	return &mappingStepExecutor{s}
}

func (s *mappingStep) parallel(ctx context.Context, f executionFactory) executionFactory {
	return &mappingFactory{s, f}
}

////////////////////////////////////////////////////////////////////////////////

var mapId = newDefaultName("Mapping")

func (c *chain) Map(m Mapper[any, any], name ...string) Untyped {
	return mapWith(c, m, name...)
}

func MapChain[I, O any](m Mapper[I, O], name ...string) Chain[I, O] {
	return AddMap[O, I, I](nil, m, name...)
}

func AddMap[N, I, O any](base Chain[I, O], m Mapper[O, N], name ...string) Chain[I, N] {
	c := mapWith(chainImpl(base), ConvertMapper[any, any](m), name...)
	return convertChain[I, N](c)
}

func mapWith(c *chain, m Mapper[any, any], name ...string) *chain {
	return &chain{c, &mappingStep{mapId.Step(), m}}
}

////////////////////////////////////////////////////////////////////////////////

type mappingStepExecutor struct {
	step *mappingStep
}

var _ executor = (*mappingStepExecutor)(nil)

func (m *mappingStepExecutor) Run(ctx context.Context, seq iter.Seq[any]) iter.Seq[any] {
	return func(yield func(any) bool) {
		for v := range seq {
			v = m.step.mapper(v)
			//fmt.Printf("after: %T(%v)\n", s, s)
			if !yield(v) {
				return
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

type mappingFactory struct {
	step    *mappingStep
	consume executionFactory
}

var _ executionFactory = (*mappingFactory)(nil)

func (f *mappingFactory) getPool() processing.Processing {
	return f.consume.getPool()
}

func (f *mappingFactory) requestExecution(ctx context.Context, v *elem.Element) {
	f.consume.getPool().Execute(&mappingRequest{v, f}, f.step.name)
}

type mappingRequest struct {
	value   *elem.Element
	factory *mappingFactory
}

var _ processing.Request = (*mappingRequest)(nil)

func (r *mappingRequest) Execute(ctx context.Context) {
	v := r.value.V()
	if r.value.IsValid() {
		v = r.factory.step.mapper(v)
	}
	r.factory.consume.requestExecution(ctx, r.value.New(v))
}

////////////////////////////////////////////////////////////////////////////////

// ConvertMapper is a generic function that adapts a Mapper of one type pair to
// another using type conversions.
func ConvertMapper[NI, NO, I, O any](m Mapper[I, O]) Mapper[NI, NO] {
	return func(i NI) NO {
		var in any = i
		var out any = m(in.(I))
		return out.(NO)
	}
}
