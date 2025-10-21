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

const RESULT = "pipeline.go, sink.go, source.go"

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

		sink := streaming.NewSink[string, string](c_sort, streaming.ProcessorFactoryFunc[string, string, string](NewProcessor))
		src, err := NewSource(".")
		Expect(err).To(BeNil())
		in, err := src.Elements()
		Expect(err).To(BeNil())
		Expect(sink.Execute(ctx, ".", in)).To(Equal(RESULT))
	})

	It("definition", func() {
		c_go := chain.AddFilter(chain.New[string](), FilterIncludeSuffix(".go"))
		c_nontest := chain.AddFilter(c_go, FilterExcludeSuffix("_test.go"))
		c_sort := chain.AddSort(c_nontest, strings.Compare)

		def := streaming.DefinePipeline[string, string](
			streaming.SourceFactoryFunc[string, string](NewSource),
			c_sort, nil)

		Expect(def.IsComplete()).To(BeFalse())
		def = def.WithProcessor(streaming.ProcessorFactoryFunc[string, string, string](NewProcessor))
		Expect(def.IsComplete()).To(BeTrue())
		Expect(def.Execute(ctx, ".")).To(Equal(RESULT))
	})
})

////////////////////////////////////////////////////////////////////////////////

type Source struct {
	dir string
}

var _ streaming.Source[string] = (*Source)(nil)

func NewSource(cfg string) (streaming.Source[string], error) {
	return &Source{cfg}, nil
}

func (s *Source) Elements() (iter.Seq[string], error) {
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

////////////////////////////////////////////////////////////////////////////////

func NewProcessor(cfg string) (streaming.Processor[string, string], error) {
	return func(ctx context.Context, i iter.Seq[string]) (string, error) {
		s := ""
		for e := range i {
			if s != "" {
				s += ", "
			}
			s += e
		}
		return s, nil
	}, nil
}
