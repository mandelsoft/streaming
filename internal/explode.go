package internal

import (
	"context"
	"fmt"
	"github.com/mandelsoft/streaming/elem"
	"github.com/mandelsoft/streaming/processing"
	"iter"
)

type GExploder[I, O any] func(I) []O
type Exploder = GExploder[any, any]

type explodeStep struct {
	step
	exploder Exploder
}

func (s *explodeStep) String() string {
	return fmt.Sprintf("explodeStep[%s]", s.name)
}

func (s *explodeStep) Renamed(name string) Step {
	n := *s
	n.name = name
	return &n
}

func (s *explodeStep) sequential(context.Context) executor {
	return &explodeStepExecutor{s}
}

func (s *explodeStep) parallel(ctx context.Context, f executionFactory) executionFactory {
	return &explodeFactory{s, f}
}

////////////////////////////////////////////////////////////////////////////////

var explodeId = newDefaultName("Explode")

func (c *chain) Explode(m Exploder, name ...string) Chain {
	return &chain{c.clean(), &explodeStep{explodeId.Step(name...), m}}
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
