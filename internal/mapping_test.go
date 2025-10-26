package internal_test

import (
	"context"
	mine "github.com/mandelsoft/streaming/internal"
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

	It("mapping", func() {
		c := mine.New()
		c_inc := c.Map(Inc)

		result := c_inc.Execute(ctx, IntIterator(1, 4))
		Expect(result).To(HaveExactElements(2, 3, 4, 5))
	})
})
