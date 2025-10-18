package chain_test

import (
	"context"
	"github.com/mandelsoft/goutils/iterutils"
	"github.com/mandelsoft/streaming/chain"
	"github.com/mandelsoft/streaming/processing"
	"github.com/mandelsoft/streaming/simplepool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"strings"
)

var _ = Describe("Chain", func() {
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

	It("filter", func() {
		c := chain.New[string]()
		c_filter := chain.AddFilter(c, FilterExcludeSuffix(".c"))

		result := c_filter.Execute(ctx, iterutils.For("a.go", "b.c", "c.go"))
		Expect(result).To(HaveExactElements("a.go", "c.go"))
	})

	It("filter parallel", func() {
		c := chain.New[string]()
		c_filter := chain.AddFilter(c, FilterExcludeSuffix(".c"))

		p := chain.New[string]()
		pc := chain.AddParallel(p, c_filter, pool)

		result := pc.Execute(ctx, iterutils.For("a.go", "b.c", "c.go"))
		Expect(result).To(ConsistOf("a.go", "c.go"))
	})
})

////////////////////////////////////////////////////////////////////////////////

func FilterExcludeSuffix(suffix string) chain.Filter[string] {
	return func(in string) bool {
		r := !strings.HasSuffix(in, suffix)
		if !r {
			// fmt.Printf("exclude %s\n", in)
		}
		return r
	}
}
