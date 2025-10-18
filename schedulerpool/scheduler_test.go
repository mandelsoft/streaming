package schedulerpool_test

import (
	"context"
	"fmt"
	"github.com/mandelsoft/goutils/iterutils"
	"github.com/mandelsoft/streaming/chain"
	"github.com/mandelsoft/streaming/processing"
	"github.com/mandelsoft/streaming/schedulerpool"
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
		pool = schedulerpool.New(ctx, 3)
	})

	AfterEach(func() {
		pool.Close()
	})

	It("mapping", func() {
		c := chain.New[int]()
		c_inc := chain.AddMap(c, Inc)

		p := chain.New[int]()
		p_par := chain.AddParallel(p, c_inc, pool)
		p_map := chain.AddMap(p_par, MapIntToString)
		result := p_map.Execute(ctx, iterutils.For(1, 2, 3, 4))

		Expect(result).To(ConsistOf("  2", "  3", "  4", "  5"))
	})
})

////////////////////////////////////////////////////////////////////////////////

func MapIntToString(i int) string {
	r := fmt.Sprintf("%3d", i)
	return r
}

func Inc(i int) int {
	return i + 1
}
