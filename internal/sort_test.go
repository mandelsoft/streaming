package internal_test

import (
	"context"
	"github.com/mandelsoft/goutils/general"
	mine "github.com/mandelsoft/streaming/internal"
	"strings"

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

	It("sort", func() {
		c := mine.New()
		c_sort := c.Sort(general.ConvertCompareFunc[any](strings.Compare))

		result := c_sort.Execute(ctx, iterutils.For[any]("c", "a", "b"))
		Expect(result).To(HaveExactElements("a", "b", "c"))
	})

	It("sort parallel", func() {
		c := mine.New()
		c_sort := c.Sort(general.ConvertCompareFunc[any](strings.Compare))
		c_app := c_sort.Map(MapAppendToString("."))

		p := mine.New()
		ps := p.Stable(c_app, pool)
		result := ps.Execute(ctx, iterutils.For[any]("c", "a", "b"))
		Expect(result).To(HaveExactElements("a.", "b.", "c."))
	})
})
