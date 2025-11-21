package streaming_test

import (
	"context"
	"github.com/mandelsoft/goutils/iterutils"
	. "github.com/mandelsoft/goutils/testutils"
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

var RESULT_SLICE = []string{"pipeline.go", "sink.go", "source.go"}
var RESULT = strings.Join(RESULT_SLICE, ", ")

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
		src := NewSource()
		in, err := src.Elements(".")
		Expect(err).To(BeNil())
		Expect(sink.Execute(ctx, ".", in)).To(Equal(RESULT))
	})

	It("definition", func() {
		c_go := chain.AddFilter(chain.New[string](), FilterIncludeSuffix(".go"))
		c_nontest := chain.AddFilter(c_go, FilterExcludeSuffix("_test.go"))
		c_sort := chain.AddSort(c_nontest, strings.Compare)

		def := streaming.DefinePipeline[string, string](
			NewSource(),
			c_sort, nil)

		Expect(def.IsComplete()).To(BeFalse())
		def = def.WithProcessor(streaming.ProcessorFactoryFunc[string, string, string](NewProcessor))
		Expect(def.IsComplete()).To(BeTrue())
		Expect(def.Execute(ctx, ".")).To(Equal(RESULT))
	})
	It("definition without processor", func() {
		c_go := chain.AddFilter(chain.New[string](), FilterIncludeSuffix(".go"))
		c_nontest := chain.AddFilter(c_go, FilterExcludeSuffix("_test.go"))
		c_sort := chain.AddSort(c_nontest, strings.Compare)

		def := streaming.DefinePipeline[string, iter.Seq[string]](
			NewSource(),
			c_sort, nil)

		Expect(def.IsComplete()).To(BeTrue())
		result := Must(def.Execute(ctx, "."))
		Expect(iterutils.Get(result)).To(Equal(RESULT_SLICE))
	})
})

////////////////////////////////////////////////////////////////////////////////

type Source struct {
}

func NewSource() streaming.SourceFactory[string, string] {
	return &Source{}
}

func (s *Source) Elements(dir string) (iter.Seq[string], error) {
	entries, err := os.ReadDir(dir)
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
