package untypedchain_test

import (
	"context"
	"github.com/mandelsoft/goutils/general"
	"github.com/mandelsoft/goutils/iterutils"
	"github.com/mandelsoft/streaming/processing"
	"github.com/mandelsoft/streaming/simplepool"
	"github.com/mandelsoft/streaming/untypedchain"
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
		c := untypedchain.Filtered(FilterExcludeSuffix(".c")).Sort(general.ConvertCompareFunc[any](strings.Compare))
		p := untypedchain.NewSink[string](c, Processor)

		result := p.Execute(ctx, iterutils.For[any]("c.go", "b.c", "a.go"))
		Expect(result).To(Equal("a.go, c.go"))
	})
})

////////////////////////////////////////////////////////////////////////////////

func Processor() untypedchain.Processor[string] {
	return func(ctx context.Context, in iter.Seq[any]) string {
		s := ""
		for v := range in {
			if s != "" {
				s += ", "
			}
			s += v.(string)
		}
		return s
	}
}

func FilterExcludeSuffix(suffix string) untypedchain.Filter {
	return func(in any) bool {
		r := !strings.HasSuffix(in.(string), suffix)
		if !r {
			// fmt.Printf("exclude %s\n", in)
		}
		return r
	}
}
