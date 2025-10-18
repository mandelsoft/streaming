package chain_test

import (
	"context"
	"github.com/mandelsoft/goutils/iterutils"
	"github.com/mandelsoft/streaming/chain"
	"github.com/mandelsoft/streaming/processing"
	"github.com/mandelsoft/streaming/simplepool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

	It("explode", func() {
		c := chain.New[string]()
		c_explode := chain.AddExplode(c, ExplodeAppendToString(".go", ".c"))

		result := c_explode.Execute(ctx, iterutils.For("a", "b", "c"))
		Expect(result).To(HaveExactElements("a.go", "a.c", "b.go", "b.c", "c.go", "c.c"))
	})

	It("explode parallel", func() {
		c := chain.New[string]()
		c_explode := chain.AddExplode(c, ExplodeAppendToString(".go", ".c"))

		p := chain.New[string]()
		pp := chain.AddParallel(p, c_explode, pool)
		result := pp.Execute(ctx, iterutils.For("a", "b", "c"))
		Expect(result).To(ConsistOf("a.go", "a.c", "b.go", "b.c", "c.go", "c.c"))
	})
})

////////////////////////////////////////////////////////////////////////////////

func ExplodeAppendToString(suffix ...string) chain.Exploder[string, string] {
	return func(in string) []string {
		out := make([]string, len(suffix))
		for i, a := range suffix {
			out[i] = in + string(a)
		}
		return out
	}
}
