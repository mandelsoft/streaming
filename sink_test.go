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
		p := streaming.NewSink[string](c_sort, Processor)

		result := p.Execute(ctx, iterutils.For("c.go", "b.c", "a.go"))
		Expect(result).To(Equal("a.go, c.go"))
	})
})

////////////////////////////////////////////////////////////////////////////////

func Processor() streaming.Processor[string, string] {
	return func(ctx context.Context, in iter.Seq[string]) string {
		s := ""
		for v := range in {
			if s != "" {
				s += ", "
			}
			s += v
		}
		return s
	}
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
