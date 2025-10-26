# Data Pipelining in Go

This library offers an API to process a set of elements by a chain of processing steps. Hereby, every step in the chain works on elements provided by the downstream chain and passes elements to the upstream chain until the final consumer it reached. The inbound and outbound interfaces of such a pipeline are iterators. The step chain of the pipeline may contain an arbitrary number of processing steps. Processing along the step chain may be sequential or parallel.

There are several step types supported:
- *Mapping* step (`chain.Mapper[I,O]`) maps an element of type I to another
  element of type O, for example, incrementing an integer (int->int), converting
  an integer to a string (int->string), or for a CLI, converting an element to a slice of descriptive fields.
- *Explode* step (`chain.Exploder[I,O]`) maps an element of type I to a slice of elements of type O, for example, replacing an element by its transitive
  closure in a graph.
- *Filter* step (`chain.Filter[I]`) decides whether an element of type I should be used or ignored for upstream processing, for example, omitting particular elements not fulfilling a selection criteria for the upstream processing.
- *Sort* step using a `chain.CompareFunc[I]` to sort elements. If used inside a parallel processing chain, `Stable` should be used instead of `Parallel`to preserve the intended order at the end of the parallel step chain, again.
- *Aggregation* step (`chain.Aggregator[I,O]`) is fed by elements and is able to emit a sequence of other elements featuring state to enable to aggregate multiple (earlier consumed) elements to provide a set of new elements. Because of the state an aggregator is able to completely rearrange the flowing elements by caching elements as long as a new set of elements can be emitted.

The step-specific operation is passed as an argument to the methods defining the step.

Additionally, it is possible to control the execution. With
- *Sequential* chain execution executes a sub chain by processing the elements sequentially
- *Parallel* chain execution executes a sub-chain in-parallel for the elements. 
  The degree of parallelism is controlled by a processor pool. 
- *Stable* chain executions does parallel element execution, but finally preserves the order the elements are fed into the chain for upstream processing
- *Conditional* chain execution a chain is optionally executed based on a `Condition` evaluating the execution context.

The default chain is executed sequentially.
All those execution modes can be combined, a parallel step may incorporate sequential steps and vice versa.

Partial chains can be composed to a new chain by including a sequential or parallel step or just by adding a chain definition to the end of another chain.
In the last case the added chain will be copied to keep the original chain as it is for further usage.

> **Attention:** The processing result can be consumed by an iterator. But
> be aware that the content might be available only once (like iterating over
> a channel).

## Conditionals

A chain can be executed based on some config value with `chain.ExecuteWithConfig`.  The configuration value is bound to the execution context
and can be retrieved via `chain.GetConfig`

```go

type CondConfig struct {
    state bool
}

func Condition(ctx context.Context) bool {
    cfg := chain.GetConfig[*CondConfig](ctx)
    if cfg == nil || cfg.state {
        return true
    }
    return false
}

// and now the pipeline code
            ...
            c := chain.New[string]()
			c_explode := chain.AddExplode(c, ExplodeAppendToString(".go", ".c"))

			cond := chain.AddFilter(chain.New[string](), FilterExcludeSuffix(".c"))
			c_cond := chain.AddConditional(c_explode, Condition, cond)
			result := chain.ExecuteWithConfig(ctx, &CondConfig{true}, c_cond, iterutils.For("a", "b", "c"))
			Expect(result).To(HaveExactElements("a.go", "b.go", "c.go"))
			...
```
## Typed and Untyped Processing Chains

The library offers a type save chain definition or an untyped one.
The untyped steps always work on elements of type `any`, the step code must do
appropriate type assertions on their own. A typed chain can be composed in a typesafe way, by using appropriate parameter types.

### Untyped Chains

```go
		s := untypedchain.New().
			Explode(ExplodeAppendToString("a", "b", "c")).
			Filter(FilterExcludeSuffix("b"))

		p := untypedchain.New().
			Map(MapIntToString).
			Sequential(s).
			Map(MapAppendToString("."))

		c := untypedchain.New().
			Map(Inc).
			Stable(p, pool)
		
		r := c.Execute(context.Background(), IntIterator(1, 10))
		Expect(r).To(HaveExactElements(
			"  2a.",
			"  2c.",
			"  3a.",
			"  3c.",
			"  4a.",
			"  4c.",
			"  5a.",
			"  5c.",
			"  6a.",
			"  6c.",
			"  7a.",
			"  7c.",
			"  8a.",
			"  8c.",
			"  9a.",
			"  9c.",
			" 10a.",
			" 10c.",
			" 11a.",
			" 11c.",
		))
```


Two examples can be found in [examples/untyped1](examples/untyped1/main.go) and
[examples/untyped2](examples/untyped2/main.go)

### Typed Chains

Go does not support parameterized methods. Therefore, the typed chaining cannot be done by methods as in the example before. Instead, separated typed functions must be used. This makes the composing of processing chains more complicate, but it is type-safe, because all involved implementations must be appropriately typed.


```go
        c := chain.New[int]()

		c_inc := chain.AddMap(c, Inc)

		p := chain.New[int]()
		p_map := chain.AddMap(p, MapIntToString)

		s := chain.New[string]()
		s_exp := chain.AddExplode(s, ExplodeAppendToString("a", "b", "c"))
		s_exc := chain.AddFilter(s_exp, FilterExcludeSuffix("b"))

		p_seq := chain.AddSequential(p_map, s_exc)
		p_app := chain.AddMap(p_seq, MapAppendToString("."))

		c_par := chain.AddStable(c_inc, p_app, pool)
		r := c_par.Execute(context.Background(), IntIterator(1, 10))
		Expect(r).To(HaveExactElements(
			"  2a.",
			"  2c.",
			"  3a.",
			"  3c.",
			"  4a.",
			"  4c.",
			"  5a.",
			"  5c.",
			"  6a.",
			"  6c.",
			"  7a.",
			"  7c.",
			"  8a.",
			"  8c.",
			"  9a.",
			"  9c.",
			" 10a.",
			" 10c.",
			" 11a.",
			" 11c.",
		))
```

A complete example can be found in [examples/typed](examples/typed/main.go).

## Preconfigured Pipelines

Preconfigured pipelines include code to generate the source element stream
and a processing providing a final result from the element stream provided by
the processing chain.

A `streaming.Sink` describes the final processing, typically an aggregation of the provided elements. It is created with `streaming.NewSink` which takes
a (`ProcessorFactory[R,I]`) creating a `Processor[R,I]` able to consume
the provided elements for inbound elements of type `I` and provides the final result of type `R`.

A `streaming.Source` is an object able to provide an initial sequence of elements to fed into a `chaim.Chain`. Multiple instantiations of the iterator
should provide similar results, which means that executing and iterator should not consume elements not visible by another instance anymore.


A `Pipeline[C, R, I, O]` describes a complete processing including a source factory (for sources producing inbound elements of type I), a chain and a processor factory (for a processor consuming elements of type O and producing a result of type R) configured by type C. 
It can then be used to execute the processing for a given config.

A Pipeline might be incomplete if some element is missing, and it may be used to create derived pipelines by setting particular source or processor factories or chains. Thereby, the used and provided types are fixed by the pipeline object.


```go
c_go := chain.AddFilter(chain.New[string](), FilterIncludeSuffix(".go"))
c_nontest := chain.AddFilter(c_go, FilterExcludeSuffix("_test.go"))
c_sort := chain.AddSort(c_nontest, strings.Compare)

def := streaming.DefinePipeline[string, string](
streaming.SourceFactoryFunc[string, string](NewSource),
c_sort, nil)

Expect(def.IsComplete()).To(BeFalse())
def = def.WithProcessor(streaming.ProcessorFactoryFunc[string, string, string](NewProcessor))
Expect(def.IsComplete()).To(BeTrue())
Expect(def.Execute(ctx, ".")).To(Equal("pipeline.go, sink.go, source.go"))
```

A complete example can be found in [examples/pipeline](examples/pipeline/main.go).


## Conversions

the `chain` package also provides some conversion functions converting input and output parameters 
- to convert mappers `chain.ConvertMapper`
- to convert exploders `chain.ConvertExploder`
- to convert filters `chain.ConvertFilter`
- to convert aggregators `chain.ConvertAggregator`
- to convert compare function a `goutils` function `general.ConvertCompareFunc` can be used

## Processing Pools

Parallel step executions are executed by a processing pool (package `processing`)
. This package provides an interface used to request step executions
and to provide communication channels.
There is a simple implementation provided by package `simplepool`.

A processing pool can potentially be reused by multiple `Parallel` steps.
The pool manages Go routines used to execute the requests by limiting the used degree of parallelism.
The default implementation provided by package `simplepool`uses plain Go channels for interaction. Because those
channel operations may block, the default implementation is not usable for sharing a processing pool among multiple nested parallel processing chains.

Package [`schedulerpool`](schedulerpool/README.md) provides a refined implementation based on the [`jobscheduler`](https://github.com/mandelsoft/jobscheduler) framework, which provides synchronization and communication objects working together with the job scheduler to limit the number of running Go routines without using a fixed number Go routines. 
Instead, it creates and deletes Go routines on-the fly by assuring that only a given number of such routines are not blocked on synchronization objects and are ready to run.

If this implementation is used, the `jobscheduler` package is required, which is quite complex, but it provides more flexibility for implementing the processing steps.

The synchronization objects get access to the processing pools using the `context.Context` object, but they also work if no such pool implementation is used by falling back to the standard Go types. Therefore, if processing steps require more complex synchronization, they should use those object types
to be prepared for the `schedulerpool` to be usable by the end user.