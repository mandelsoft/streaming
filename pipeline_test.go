package streaming_test

import (
	"context"
	"iter"
	"os"
	"strings"

	"github.com/mandelsoft/streaming"
	"github.com/mandelsoft/streaming/chain"
	"github.com/mandelsoft/streaming/processing"
	"github.com/mandelsoft/streaming/simplepool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Pipeline", func() {
	var (
		ctx  context.Context
		pool processing.Processing
	)

	BeforeEach(func() {
		ctx = context.Background()
		pool = simplepool.New(ctx, 3)
	})

	AfterEach(func() {
		pool.Close()
	})

	It("preconfigured pipeline", func() {
		c_go := chain.AddFilter(chain.New[string](), FilterIncludeSuffix(".go"))
		c_nontest := chain.AddFilter(c_go, FilterExcludeSuffix("_test.go"))
		c_sort := chain.AddSort(c_nontest, strings.Compare)

		s := streaming.NewSink[string](c_sort, Processor)
		p := streaming.NewPipeline(&Source{}, s)

		p.Source().SetDir(".")
		Expect(p.Execute(ctx)).To(Equal("pipeline.go, sink.go, source.go"))
	})

	It("preconfigued type", func() {
		c_go := chain.AddFilter(chain.New[string](), FilterIncludeSuffix(".go"))
		c_nontest := chain.AddFilter(c_go, FilterExcludeSuffix("_test.go"))
		c_sort := chain.AddSort(c_nontest, strings.Compare)

		p := NewPipeline(c_sort)

		p.SetDir(".")
		Expect(p.Execute(ctx)).To(Equal("pipeline.go, sink.go, source.go"))
	})
})

////////////////////////////////////////////////////////////////////////////////

type Source struct {
	dir string
}

var _ streaming.Source[string] = (*Source)(nil)

func (s *Source) Source() (iter.Seq[string], error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	return func(yield func(string) bool) {
		for _, e := range entries {
			if !yield(e.Name()) {
				return
			}
		}
	}, nil
}

func (s *Source) SetDir(dir string) {
	s.dir = dir
}

////////////////////////////////////////////////////////////////////////////////

type Pipeline struct {
	streaming.Pipeline[string, string, *Source, streaming.Sink[string, string]]
}

func NewPipeline(c chain.Chain[string, string]) *Pipeline {
	return &Pipeline{
		streaming.NewPipeline(&Source{}, streaming.NewSink(c, Processor)),
	}
}

func (p *Pipeline) SetDir(dir string) {
	p.Source().SetDir(dir)
}
