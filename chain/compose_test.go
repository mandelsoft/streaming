package chain_test

import (
	"context"
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

	It("composing", func() {
		c := chain.New[int]()
		c_inc := chain.AddMap(c, Inc)

		r := chain.AddChain(c_inc, c_inc)
		r = chain.AddChain(c_inc, r)
		result := r.Execute(ctx, IntIterator(1, 4))
		Expect(result).To(HaveExactElements(4, 5, 6, 7))
	})
})
