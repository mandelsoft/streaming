package streaming_test

import (
	"context"
	"github.com/mandelsoft/goutils/iterutils"
	"github.com/mandelsoft/streaming"
	"github.com/mandelsoft/streaming/chain"
	"github.com/mandelsoft/streaming/processing"
	"github.com/mandelsoft/streaming/simplepool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"iter"
	"strings"
)

var _ = Describe("Sink", func() {
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

	It("process", func() {
		c := chain.New[string]()
		c_filter := chain.AddFilter(c, FilterExcludeSuffix(".c"))
		c_sort := chain.AddSort(c_filter, strings.Compare)
		s := streaming.NewSink[streaming.Void, string](
			c_sort,
			streaming.ProcessorFactoryFunc[streaming.Void, string, string](ProcessorFactory),
		)

		result, err := s.Execute(ctx, nil, iterutils.For("c.go", "b.c", "a.go"))
		Expect(err).To(BeNil())
		Expect(result).To(Equal("a.go, c.go"))
	})
})

////////////////////////////////////////////////////////////////////////////////

func ProcessorFactory(streaming.Void) (streaming.Processor[string, string], error) {
	return func(ctx context.Context, in iter.Seq[string]) (string, error) {
		s := ""
		for v := range in {
			if s != "" {
				s += ", "
			}
			s += v
		}
		return s, nil
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func FilterExcludeSuffix(suffix string) chain.Filter[string] {
	return func(in string) bool {
		r := !strings.HasSuffix(in, suffix)
		return r
	}
}

func FilterIncludeSuffix(suffix string) chain.Filter[string] {
	return func(in string) bool {
		r := strings.HasSuffix(in, suffix)
		return r
	}
}
