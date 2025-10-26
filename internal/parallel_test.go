package internal_test

import (
	"context"
	mine "github.com/mandelsoft/streaming/internal"

	"github.com/mandelsoft/logging"
	"github.com/mandelsoft/logging/logrusl"
	"github.com/mandelsoft/streaming/processing"
	"github.com/mandelsoft/streaming/simplepool"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Parallel", func() {
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

	It("parallel", func() {
		c := mine.New()

		s := mine.New()
		si := s.Map(Inc)
		sm := si.Map(MapIntToString)
		c_parallel := c.Parallel(sm, pool)

		result := c_parallel.Execute(ctx, IntIterator(1, 4))
		Expect(result).To(ConsistOf("  2", "  3", "  4", "  5"))
	})
})
