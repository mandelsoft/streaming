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

	It("explode", func() {
		c := mine.New()
		c_explode := c.Explode(ExplodeAppendToString(".go", ".c"))

		result := c_explode.Execute(ctx, iterutils.For[any]("a", "b", "c"))
		Expect(result).To(HaveExactElements("a.go", "a.c", "b.go", "b.c", "c.go", "c.c"))
	})

	It("explode parallel", func() {
		c := mine.New()
		c_explode := c.Explode(ExplodeAppendToString(".go", ".c"))

		p := mine.New()
		pp := p.Parallel(c_explode, pool)
		result := pp.Execute(ctx, iterutils.For[any]("a", "b", "c"))
		Expect(result).To(ConsistOf("a.go", "a.c", "b.go", "b.c", "c.go", "c.c"))
	})
})
