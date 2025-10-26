package chain_test

import (
	"context"
	"github.com/mandelsoft/goutils/iterutils"
	"github.com/mandelsoft/streaming/chain"
	"github.com/mandelsoft/streaming/internal"
	"github.com/mandelsoft/streaming/processing"
	"github.com/mandelsoft/streaming/simplepool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Conditional", func() {
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

	Context("sequential", func() {
		It("conditional without config", func() {
			c := chain.New[string]()
			c_explode := chain.AddExplode(c, ExplodeAppendToString(".go", ".c"))

			cond := chain.AddFilter(chain.New[string](), FilterExcludeSuffix(".c"))
			c_cond := chain.AddConditional(c_explode, Condition, cond)
			result := c_cond.Execute(ctx, iterutils.For("a", "b", "c"))
			Expect(result).To(HaveExactElements("a.go", "b.go", "c.go"))
		})

		It("conditional without config true", func() {
			c := chain.New[string]()
			c_explode := chain.AddExplode(c, ExplodeAppendToString(".go", ".c"))

			cond := chain.AddFilter(chain.New[string](), FilterExcludeSuffix(".c"))
			c_cond := chain.AddConditional(c_explode, Condition, cond)
			result := c_cond.ExecuteWithConfig(ctx, &CondConfig{true}, iterutils.For("a", "b", "c"))
			Expect(result).To(HaveExactElements("a.go", "b.go", "c.go"))
		})

		It("conditional without config false", func() {
			c := chain.New[string]()
			c_explode := chain.AddExplode(c, ExplodeAppendToString(".go", ".c"))

			cond := chain.AddFilter(chain.New[string](), FilterExcludeSuffix(".c"))
			c_cond := chain.AddConditional(c_explode, Condition, cond)
			result := c_cond.ExecuteWithConfig(ctx, &CondConfig{false}, iterutils.For("a", "b", "c"))
			Expect(result).To(HaveExactElements("a.go", "a.c", "b.go", "b.c", "c.go", "c.c"))
		})
	})

	Context("parallel", func() {
		It("conditional without config", func() {
			c := chain.New[string]()
			c_explode := chain.AddExplode(c, ExplodeAppendToString(".go", ".c"))

			cond := chain.AddFilter(chain.New[string](), FilterExcludeSuffix(".c"))
			c_cond := chain.AddConditional(c_explode, Condition, cond)

			c_par := chain.AddParallel(chain.New[string](), c_cond, pool)

			result := c_par.Execute(ctx, iterutils.For("a", "b", "c"))
			Expect(result).To(ConsistOf("a.go", "b.go", "c.go"))
		})
	})
})

////////////////////////////////////////////////////////////////////////////////

type CondConfig struct {
	state bool
}

func Condition(ctx context.Context) bool {
	cfg := internal.GetConfig[*CondConfig](ctx)
	if cfg == nil || cfg.state {
		return true
	}
	return false
}
