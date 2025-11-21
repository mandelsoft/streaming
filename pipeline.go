package streaming

import (
	"context"
	"github.com/mandelsoft/goutils/generics"
	"github.com/mandelsoft/streaming/chain"
	"iter"
)

type Pipeline[C, R, I, O any] interface {
	IsComplete() bool

	WithSource(src SourceFactory[C, I]) Pipeline[C, R, I, O]
	WithChain(chain chain.Chain[I, O]) Pipeline[C, R, I, O]
	WithProcessor(proc ProcessorFactory[C, R, O]) Pipeline[C, R, I, O]

	GetChain() chain.Chain[I, O]
	GetSourceFactory() SourceFactory[C, I]
	GetProcessorFactory() ProcessorFactory[C, R, O]

	Sink() Sink[C, R, I]
	Source(cfg C) (iter.Seq[I], error)
	Processor(cfg C) (Processor[R, O], error)

	Execute(ctx context.Context, cfg C) (R, error)
}

type pipeline[C, R, I, O any] struct {
	src   SourceFactory[C, I]
	chain chain.Chain[I, O]
	proc  ProcessorFactory[C, R, O]
}

// DefinePipeline defines a pipeline for a given source factor, chain and processor factory.
// Executing a pipeline instantiates source and processor using a given configuration
// and provides the processing result.
// If no processor factory is given, the result type R must be iter.Seq[O], and the
// output iterator of the chain is returned (see DefineSimplePipeline).
func DefinePipeline[C, R, I, O any](src SourceFactory[C, I], chain chain.Chain[I, O], proc ProcessorFactory[C, R, O]) Pipeline[C, R, I, O] {
	return &pipeline[C, R, I, O]{
		src, chain, proc,
	}
}

// DefineSimplePipeline creates a Pipeline using the provided SourceFactory and chain.Chain without an explicit
// processor. The pipeline execution result will be with iter.Seq[O] the output iterator of the chain.
func DefineSimplePipeline[C, I, O any](src SourceFactory[C, I], chain chain.Chain[I, O]) Pipeline[C, iter.Seq[O], I, O] {
	return &pipeline[C, iter.Seq[O], I, O]{
		src, chain, nil,
	}
}

func (p *pipeline[C, R, I, O]) IsComplete() bool {
	return p.chain != nil && (p.proc != nil || generics.CanAssign[iter.Seq[O], R]()) && p.src != nil
}

func (p *pipeline[C, R, I, O]) GetChain() chain.Chain[I, O] {
	return p.chain
}

func (p *pipeline[C, R, I, O]) GetSourceFactory() SourceFactory[C, I] {
	return p.src
}

func (p *pipeline[C, R, I, O]) GetProcessorFactory() ProcessorFactory[C, R, O] {
	return p.proc
}

func (p *pipeline[C, R, I, O]) Sink() Sink[C, R, I] {
	return NewSink(p.chain, p.proc)
}

func (p *pipeline[C, R, I, O]) Source(cfg C) (iter.Seq[I], error) {
	return p.src.Elements(cfg)
}

func (p *pipeline[C, R, I, O]) Processor(cfg C) (Processor[R, O], error) {
	return p.proc.Processor(cfg)
}

///////////////

func (p *pipeline[C, R, I, O]) WithSource(src SourceFactory[C, I]) Pipeline[C, R, I, O] {
	return &pipeline[C, R, I, O]{
		src, p.chain, p.proc,
	}
}
func (p *pipeline[C, R, I, O]) WithChain(chain chain.Chain[I, O]) Pipeline[C, R, I, O] {
	return &pipeline[C, R, I, O]{
		p.src, chain, p.proc,
	}
}
func (p *pipeline[C, R, I, O]) WithProcessor(proc ProcessorFactory[C, R, O]) Pipeline[C, R, I, O] {
	return &pipeline[C, R, I, O]{
		p.src, p.chain, proc,
	}
}

///////////////

func (p *pipeline[C, R, I, O]) Execute(ctx context.Context, cfg C) (R, error) {
	var _nil R

	s, err := p.Source(cfg)
	if err != nil {
		return _nil, err
	}
	sink := p.Sink()
	return sink.Execute(ctx, cfg, s)
}
