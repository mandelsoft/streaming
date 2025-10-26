package internal

import (
	"context"
	"fmt"
	"iter"
	"sync"
	"sync/atomic"

	"github.com/mandelsoft/logging"
	"github.com/mandelsoft/streaming/elem"
	"github.com/mandelsoft/streaming/log"
	"github.com/mandelsoft/streaming/processing"
)

type GAggregator[I, O any] = func() GAggregation[I, O]
type GAggregation[I, O any] interface {
	// Aggregate consumes one element and optionally provides a set of new
	// elements based on the previously consumed set of elements. If called
	// in a parallel chain, the implementation must potentially be synchronized.
	Aggregate(I) []O
	// Finish must finally provide new elements based on the actual internal
	// state. It is called, after the last element has been propagated.
	Finish() []O
}

// Aggregation represents a generic interface for performing incremental
// aggregation over input elements.
// The Aggregate method processes input elements and optionally finalizes
// aggregation for the current input batch.
// The Finish method finalizes and retrieves still pending aggregated results.
type Aggregation = GAggregation[any, any]

// Aggregator is a factory function creating an aggregation object.
// The generated object typically has its own local state to process the aggregation.
type Aggregator = GAggregator[any, any]

type aggregateStep struct {
	step
	aggregator Aggregator
}

func (s *aggregateStep) String() string {
	return fmt.Sprintf("aggregateStep[%s}", s.name)
}

func (s *aggregateStep) Renamed(name string) Step {
	n := *s
	n.name = name
	return &n
}

func (s *aggregateStep) sequential(context.Context) executor {
	return &aggregateStepExecutor{s, s.aggregator()}
}

func (s *aggregateStep) parallel(ctx context.Context, f executionFactory) executionFactory {
	return &aggregateFactory{name: s.name, key: s.key, aggregator: s.aggregator(), consume: f, input: elem.NewElements()}
}

////////////////////////////////////////////////////////////////////////////////

var aggregateId = newDefaultName("Aggregate")

func (c *chain) Aggregate(m Aggregator, name ...string) Chain {
	return &chain{c.clean(), &aggregateStep{aggregateId.Step(name...), m}}
}

////////////////////////////////////////////////////////////////////////////////

type aggregateStepExecutor struct {
	step       *aggregateStep
	aggregator Aggregation
}

var _ executor = (*aggregateStepExecutor)(nil)

func (m *aggregateStepExecutor) Run(ctx context.Context, seq iter.Seq[any]) iter.Seq[any] {
	return func(yield func(any) bool) {
		kv := logging.KeyValue("step", m.step.key)
		i := 0
		for v := range seq {
			log.Log.Debug("aggregate", kv, "in", v, "index", i)
			r := m.aggregator.Aggregate(v)
			log.Log.Debug("aggregation result", kv, "out", r, "index", i)
			for _, e := range r {
				if !yield(e) {
					return
				}
			}
			i++
		}
		r := m.aggregator.Finish()
		//fmt.Printf("after: %T(%v)\n", s, s)
		for _, e := range r {
			if !yield(e) {
				return
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

type aggregateFactory struct {
	name       string
	key        string
	aggregator Aggregation
	consume    executionFactory

	pending atomic.Int64
	index   atomic.Int64

	lock  sync.Mutex
	input elem.Elements
}

var _ executionFactory = (*aggregateFactory)(nil)

func (f *aggregateFactory) getPool() processing.Processing {
	return f.consume.getPool()
}

func (f *aggregateFactory) requestExecution(ctx context.Context, v *elem.Element) {
	f.pending.Add(1)
	f.lock.Lock()
	f.input.Add(v)
	f.lock.Unlock()
	f.consume.getPool().Execute(&aggregateRequest{v, f}, f.name)
}

func (f *aggregateFactory) isComplete() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.input.IsComplete()
}

func (f *aggregateFactory) nextId() elem.Id {
	return elem.NewId(int(f.index.Add(1)-1), 0)
}

////////////////////////////////////////////////////////////////////////////////

type aggregateRequest struct {
	value   *elem.Element
	factory *aggregateFactory
}

var _ processing.Request = (*aggregateRequest)(nil)

func (r *aggregateRequest) Execute(ctx context.Context) {
	kv := logging.KeyValue("step", r.factory.key)
	// use a new number range
	if r.value.IsValid() {
		v := r.value.V()
		log.Log.Debug("aggregate", kv, "in", v, "index", r.value.Id())
		l := r.factory.aggregator.Aggregate(v)
		log.Log.Debug("aggregation result", kv, "out", l, "index", r.value.Id())
		for _, e := range l {
			r.factory.consume.requestExecution(ctx, elem.NewElement(r.factory.nextId(), e))
		}
	}
	if r.factory.pending.Add(-1) == 0 && r.factory.isComplete() {
		l := r.factory.aggregator.Finish()
		for _, e := range l {
			r.factory.consume.requestExecution(ctx, elem.NewElement(r.factory.nextId(), e))
		}
		r.factory.consume.requestExecution(ctx, elem.NewPropagationElement(int(r.factory.index.Load())))
	}
}
