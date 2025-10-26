package internal

import (
	"context"
	"iter"
	"sync"

	"github.com/mandelsoft/streaming/elem"
	"github.com/mandelsoft/streaming/processing"
)

type gatherStep struct {
	step
	single Mapper
	all    func(context.Context, []any) iter.Seq[any]
}

func (m *gatherStep) sequential(context.Context) executor {
	return &gatherStepExecutor{m}
}

func (m *gatherStep) parallel(ctx context.Context, f executionFactory) executionFactory {
	return &gatherStepFactory{step: m, consume: f, input: elem.NewElements()}
}

////////////////////////////////////////////////////////////////////////////////

type gatherStepExecutor struct {
	step *gatherStep
}

var _ executor = (*gatherStepExecutor)(nil)

func (m *gatherStepExecutor) Run(ctx context.Context, seq iter.Seq[any]) iter.Seq[any] {
	return func(yield func(any) bool) {
		list := []any{}
		for v := range seq {
			if m.step.single != nil {
				v = m.step.single(v)
			}
			list = append(list, v)
		}
		m.step.all(ctx, list)(yield)
	}
}

////////////////////////////////////////////////////////////////////////////////

type gatherStepFactory struct {
	step    *gatherStep
	consume executionFactory

	lock  sync.Mutex
	input elem.Elements
}

var _ executionFactory = (*gatherFactory)(nil)

func (f *gatherStepFactory) getPool() processing.Processing {
	return f.consume.getPool()
}

func (f *gatherStepFactory) requestExecution(ctx context.Context, v *elem.Element) {
	f.lock.Lock()
	defer f.lock.Unlock()

	// fmt.Printf("gather: %s\n", v)
	f.input.Add(v)
	if f.input.IsComplete() {
		// fmt.Printf("complete -> request execution\n")
		f.getPool().Execute(&gatherStepRequest{f})
	} else {
		// fmt.Printf("not complete\n")
	}
}

type gatherStepRequest struct {
	factory *gatherStepFactory
}

var _ processing.Request = (*gatherStepRequest)(nil)

func (r *gatherStepRequest) Execute(ctx context.Context) {
	values := []any{}
	for v := range r.factory.input.Values {
		if r.factory.step.single != nil {
			v = r.factory.step.single(v)
		}
		values = append(values, v)
	}

	i := 0
	for v := range r.factory.step.all(ctx, values) {
		r.factory.consume.requestExecution(ctx, elem.NewElement(elem.NewId(i, 0), v))
		i++
	}
	r.factory.consume.requestExecution(ctx, elem.NewPropagationElement(i))
}

////////////////////////////////////////////////////////////////////////////////

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
