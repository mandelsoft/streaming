package streaming

import "context"

// Pipeline represents a processing pipeline that transforms data from a source to a sink.
// I is the type of input elements processed by the Source.
// R is the result type produced by the Sink after processing.
// IN defines the Source providing input data to the pipeline.
// OUT defines the Sink processing data and generating the output.
// Source retrieves the input source used by the pipeline.
// Sink retrieves the output sink used by the pipeline.
// The Pipeline object grants access to the Source and Sink with
// its effective types (IN and OUT) to enable configuration prior to calling Execute.
type Pipeline[I any, R any, IN Source[I], OUT Sink[R, I]] interface {
	// Source retrieves the input source used by the pipeline for processing data.
	Source() IN

	// Sink retrieves the output sink used by the pipeline for processing and
	// generating the resultant output.
	Sink() OUT

	// Execute runs the (preconfigured) pipeline by processing data from the source through the
	// sink and returns the final result of type R.
	Execute(ctx context.Context) (R, error)
}

type pipeline[I, R any, IN Source[I], OUT Sink[R, I]] struct {
	src  IN
	sink OUT
}

func NewPipeline[I, R any, IN Source[I], OUT Sink[R, I]](src IN, sink OUT) Pipeline[I, R, IN, OUT] {
	return &pipeline[I, R, IN, OUT]{src, sink}
}

func (p *pipeline[I, R, IN, OUT]) Source() IN {
	return p.src
}

func (p *pipeline[I, R, IN, OUT]) Sink() OUT {
	return p.sink
}

func (p *pipeline[I, R, IN, OUT]) Execute(ctx context.Context) (R, error) {
	src, err := p.src.Source()
	if err != nil {
		var _nil R
		return _nil, err
	}
	return p.sink.Execute(ctx, src), nil
}
