package internal_test

import (
	"context"

	mine "github.com/mandelsoft/streaming/internal"

	"github.com/mandelsoft/goutils/iterutils"
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

	It("filter", func() {

		c := mine.New()
		c_filter := c.Filter(FilterExcludeSuffix(".c"))

		result := c_filter.Execute(ctx, iterutils.For[any]("a.go", "b.c", "c.go"))
		Expect(result).To(HaveExactElements("a.go", "c.go"))
	})

	It("filter parallel", func() {
		c := mine.New()
		c_filter := c.Filter(FilterExcludeSuffix(".c"))

		p := mine.New()
		pc := p.Parallel(c_filter, pool)

		result := pc.Execute(ctx, iterutils.For[any]("a.go", "b.c", "c.go"))
		Expect(result).To(ConsistOf("a.go", "c.go"))
	})
})
