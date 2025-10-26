package internal_test

import (
	"context"
	"github.com/mandelsoft/goutils/general"
	"strings"

	mine "github.com/mandelsoft/streaming/internal"

	"github.com/mandelsoft/logging"
	"github.com/mandelsoft/logging/logrusl"
	"github.com/mandelsoft/streaming/processing"
	"github.com/mandelsoft/streaming/simplepool"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Stable", func() {
	logging.DefaultContext().SetBaseLogger(logrusl.Human(true).NewLogr())
	logging.DefaultContext().SetDefaultLevel(logging.DebugLevel)

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

	It("stable", func() {
		c := mine.New()

		s := mine.New()
		si := s.Map(Inc)
		sm := si.Map(MapIntToString)
		ss := sm.Sort(general.ConvertCompareFunc[any](strings.Compare))
		c_parallel := c.Stable(ss, pool)

		result := c_parallel.Execute(ctx, IntIterator(1, 4))
		Expect(result).To(HaveExactElements("  2", "  3", "  4", "  5"))
	})

	It("combines everything", func() {
		c := mine.New()

		c_inc := c.Map(Inc)

		p := mine.New()
		p_map := p.Map(MapIntToString)

		s := mine.New()
		s_exp := s.Explode(ExplodeAppendToString("a", "b", "c"))
		s_exc := s_exp.Filter(FilterExcludeSuffix("b"))
		//as := chain.AddSort(am, strings.Compare)

		p_seq := p_map.Sequential(s_exc)
		p_app := p_seq.Map(MapAppendToString("."))

		c_par := c_inc.Stable(p_app, pool)
		r := c_par.Execute(context.Background(), IntIterator(1, 10))
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
