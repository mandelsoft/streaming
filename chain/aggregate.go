package chain

import (
	"context"
	"fmt"
	"github.com/mandelsoft/logging"
	"github.com/mandelsoft/streaming/log"
	"iter"
	"sync"
	"sync/atomic"

	"github.com/mandelsoft/goutils/sliceutils"
	"github.com/mandelsoft/streaming/elem"
	"github.com/mandelsoft/streaming/processing"
)

// Aggregation represents a generic interface for performing incremental
// aggregation over input elements.
// The input type is represented by I and the output type by O.
// The Aggregate method processes input elements and optionally finalizes
// aggregation for the current input batch.
// The Finish method finalizes and retrieves still pending aggregated results.
type Aggregation[I, O any] interface {
	// Aggregate consumes one element and optionally provides a set of new
	// elements based on the previously consumed set of elements. If called
	// in a parallel chain, the implementation must potentially be synchronized.
	Aggregate(I) []O
	// Finish must finally provide new elements based on the actual internal
	// state. It is called, after the last element has been propagated.
	Finish() []O
}

// Aggregator is a factory function creating an aggregation object.
// The generated object typically has its own local state to process the aggregation.
type Aggregator[I, O any] = func() Aggregation[I, O]

type aggregateStep struct {
	step
	aggregator Aggregator[any, any]
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

func (c *chain) Aggregate(m Aggregator[any, any], name ...string) Untyped {
	return aggregateWith(c, m, name...)
}

func AggregationChain[I, O any](m Aggregator[I, O], name ...string) Chain[I, O] {
	return AddAggregation[O, I, I](nil, m, name...)
}

func AddAggregation[N, I, O any](base Chain[I, O], m Aggregator[O, N], name ...string) Chain[I, N] {
	c := aggregateWith(chainImpl(base), convertAggregator[any, any, O, N](m), name...)
	return convertChain[I, N](c)
}

func aggregateWith(c *chain, m Aggregator[any, any], name ...string) *chain {
	return &chain{c, &aggregateStep{aggregateId.Step(name...), m}}
}

////////////////////////////////////////////////////////////////////////////////

type aggregateStepExecutor struct {
	step       *aggregateStep
	aggregator Aggregation[any, any]
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
	aggregator Aggregation[any, any]
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

////////////////////////////////////////////////////////////////////////////////

func convertAggregator[NI, NO, I, O any](in Aggregator[I, O]) Aggregator[NI, NO] {
	return func() Aggregation[NI, NO] {
		return &convertingAggregation[NI, NO, I, O]{in()}
	}
}

type convertingAggregation[NI, NO, I, O any] struct {
	aggregation Aggregation[I, O]
}

var _ Aggregation[any, any] = (*convertingAggregation[any, any, int, int])(nil)

func (c *convertingAggregation[NI, NO, I, O]) convertIn(in NI) I {
	var i any = in
	if i == nil {
		var _nil I
		i = _nil
	}
	return i.(I)
}

func (c *convertingAggregation[NI, NO, I, O]) convertOut(out []O) []NO {
	return sliceutils.Convert[NO](out)
}

func (c *convertingAggregation[NI, NO, I, O]) Aggregate(i NI) []NO {
	return c.convertOut(c.aggregation.Aggregate(c.convertIn(i)))
}

func (c *convertingAggregation[NI, NO, I, O]) Finish() []NO {
	return c.convertOut(c.aggregation.Finish())
}
