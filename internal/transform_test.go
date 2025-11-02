package internal_test

import (
	"context"
	"github.com/mandelsoft/goutils/general"
	mine "github.com/mandelsoft/streaming/internal"
	"slices"
	"strings"

	"github.com/mandelsoft/goutils/iterutils"
	"github.com/mandelsoft/streaming/processing"
	"github.com/mandelsoft/streaming/simplepool"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

	It("transform", func() {
		c := mine.New()
		c_sort := c.Transform(transform)

		result := c_sort.Execute(ctx, iterutils.For[any]("c", "a", "b"))
		Expect(result).To(HaveExactElements("a", "b", "c"))
	})

	It("transform parallel", func() {
		c := mine.New()
		c_sort := c.Transform(transform)
		c_app := c_sort.Map(MapAppendToString("."))

		p := mine.New()
		ps := p.Stable(c_app, pool)
		result := ps.Execute(ctx, iterutils.For[any]("c", "a", "b"))
		Expect(result).To(HaveExactElements("a.", "b.", "c."))
	})
})

func transform(in []any) []any {
	out := slices.Clone(in)
	slices.SortFunc(out, general.ConvertCompareFunc[any](strings.Compare))
	return out
}
