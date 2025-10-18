package chain_test

import (
	"context"
	"github.com/mandelsoft/goutils/general"
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

	It("parallel", func() {
		s := chain.NewUntyped().
			Map(chain.ConvertMapper[any, any](Inc)).
			Map(chain.ConvertMapper[any, any](MapIntToString))
		p := chain.NewUntyped().Parallel(s, pool)

		result := p.Execute(ctx, iterutils.Convert[any](IntIterator(1, 4)))
		Expect(result).To(ConsistOf("  2", "  3", "  4", "  5"))
	})

	It("stable", func() {
		s := chain.NewUntyped().
			Map(chain.ConvertMapper[any, any](Inc)).
			Map(chain.ConvertMapper[any, any](MapIntToString)).
			Sort(general.ConvertCompareFunc[any](strings.Compare))

		p := chain.NewUntyped().Stable(s, pool)

		result := p.Execute(ctx, iterutils.Convert[any](IntIterator(1, 4)))
		Expect(result).To(HaveExactElements("  2", "  3", "  4", "  5"))
	})

	It("combines everything", func() {

		s := chain.NewUntyped().
			Explode(chain.ConvertExploder[any, any](ExplodeAppendToString("a", "b", "c"))).
			Filter(chain.ConvertFilter[any](FilterExcludeSuffix("b")))

		p := chain.NewUntyped().
			Map(chain.ConvertMapper[any, any](MapIntToString)).
			Sequential(s).
			Map(chain.ConvertMapper[any, any](MapAppendToString(".")))

		c := chain.NewUntyped().
			Map(chain.ConvertMapper[any, any](Inc)).
			Stable(p, pool)
		
		r := c.Execute(context.Background(), iterutils.Convert[any](IntIterator(1, 10)))
		Expect(r).To(HaveExactElements(
			"  2a.",
			"  2c.",
			"  3a.",
			"  3c.",
			"  4a.",
			"  4c.",
			"  5a.",
			"  5c.",
			"  6a.",
			"  6c.",
			"  7a.",
			"  7c.",
			"  8a.",
			"  8c.",
			"  9a.",
			"  9c.",
			" 10a.",
			" 10c.",
			" 11a.",
			" 11c.",
		))
	})
})
