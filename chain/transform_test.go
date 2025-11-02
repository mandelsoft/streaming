package chain_test

import (
	"context"
	"github.com/mandelsoft/goutils/iterutils"
	"github.com/mandelsoft/streaming/chain"
	"github.com/mandelsoft/streaming/processing"
	"github.com/mandelsoft/streaming/simplepool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"slices"
	"sort"
)

var _ = Describe("Transform", func() {
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

	It("sort", func() {
		c := chain.New[string]()
		c_sort := chain.AddTransform(c, transform)

		result := c_sort.Execute(ctx, iterutils.For("c", "a", "b"))
		Expect(result).To(HaveExactElements("a", "b", "c"))
	})

	It("sort parallel", func() {
		c := chain.New[string]()
		c_sort := chain.AddTransform(c, transform)
		c_app := chain.AddMap(c_sort, MapAppendToString("."))

		p := chain.New[string]()
		ps := chain.AddStable(p, c_app, pool)
		result := ps.Execute(ctx, iterutils.For("c", "a", "b"))
		Expect(result).To(HaveExactElements("a.", "b.", "c."))
	})
})

func transform(in []string) []string {
	out := slices.Clone(in)
	sort.Strings(out)
	return out
}
