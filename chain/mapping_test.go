package chain_test

import (
	"context"
	"fmt"
	"github.com/mandelsoft/streaming/chain"
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
		c := chain.New[int]()
		c_inc := chain.AddMap(c, Inc)

		result := c_inc.Execute(ctx, IntIterator(1, 4))
		Expect(result).To(HaveExactElements(2, 3, 4, 5))
	})
})

////////////////////////////////////////////////////////////////////////////////

func MapIntToString(i int) string {
	r := fmt.Sprintf("%3d", i)
	return r
}
