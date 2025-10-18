package chain

import (
	"context"
	"fmt"
	"github.com/mandelsoft/goutils/sliceutils"
	"github.com/mandelsoft/streaming/elem"
	"github.com/mandelsoft/streaming/processing"
	"iter"
)

type Exploder[I, O any] func(I) []O

type explodeStep struct {
	step
	exploder Exploder[any, any]
}

func (s *explodeStep) String() string {
	return fmt.Sprintf("explodeStep[%s]", s.name)
}

func (s *explodeStep) Renamed(name string) Step {
	n := *s
	n.name = name
	return &n
}

func (s *explodeStep) sequential() executor {
	return &explodeStepExecutor{s}
}

func (s *explodeStep) parallel(f executionFactory) executionFactory {
	return &explodeFactory{s, f}
}

////////////////////////////////////////////////////////////////////////////////

var explodeId = newDefaultName("Explode")

func (c *chain) Explode(m Exploder[any, any], name ...string) Untyped {
	return explodeWith(c, m, name...)
}

func AddExplode[N, I, O any](base Chain[I, O], m Exploder[O, N], name ...string) Chain[I, N] {
	c := explodeWith(base.impl(), ConvertExploder[any, any](m), name...)
	return convertChain[I, N](c)
}

func explodeWith(c *chain, m Exploder[any, any], name ...string) *chain {
	return &chain{c, &explodeStep{explodeId.Step(name...), m}}
}

////////////////////////////////////////////////////////////////////////////////

type explodeStepExecutor struct {
	step *explodeStep
}

var _ executor = (*explodeStepExecutor)(nil)

func (m *explodeStepExecutor) Run(ctx context.Context, seq iter.Seq[any]) iter.Seq[any] {
	return func(yield func(any) bool) {
		for v := range seq {
			r := m.step.exploder(v)
			//fmt.Printf("after: %T(%v)\n", s, s)
			for _, e := range r {
				if !yield(e) {
					return
				}
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

type explodeFactory struct {
	step    *explodeStep
	consume executionFactory
}

var _ executionFactory = (*explodeFactory)(nil)

func (f *explodeFactory) getPool() processing.Processing {
	return f.consume.getPool()
}

func (f *explodeFactory) requestExecution(ctx context.Context, v *elem.Element) {
	f.consume.getPool().Execute(&explodeRequest{v, f}, f.step.name)
}

////////////////////////////////////////////////////////////////////////////////

type explodeRequest struct {
	value   *elem.Element
	factory *explodeFactory
}

var _ processing.Request = (*explodeRequest)(nil)

func (r *explodeRequest) Execute(ctx context.Context) {
	if r.value.IsValid() {
		v := r.value.V()
		l := r.factory.step.exploder(v)
		switch len(l) {
		case 0:
			r.factory.consume.requestExecution(ctx, r.value.NewInvalid())
		case 1:
			r.factory.consume.requestExecution(ctx, r.value.New(l[0]))
		default:
			for i, e := range l {
				r.factory.consume.requestExecution(ctx, elem.NewElement(r.value.Id().Add2(i, len(l)), e))
			}
		}
	} else {
		r.factory.consume.requestExecution(ctx, r.value)
	}
}

////////////////////////////////////////////////////////////////////////////////

// ConvertExploder converts an Exploder of type I to O into an Exploder of type
// NI to NO using type assertions and slice conversion.
func ConvertExploder[NI, NO, I, O any](m Exploder[I, O]) Exploder[NI, NO] {
	return func(i NI) []NO {
		var in any = i
		var out = m(in.(I))
		return sliceutils.Convert[NO](out)
	}
}
