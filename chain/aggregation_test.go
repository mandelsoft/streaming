package chain_test

import (
	"context"
	"github.com/mandelsoft/streaming/chain"
	"github.com/mandelsoft/streaming/processing"
	"github.com/mandelsoft/streaming/simplepool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sort"
	"sync"
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

	It("aggregates", func() {
		c := chain.New[int]()
		a := chain.AddAggregation[[]int](c, IntAggregator(2))
		result := a.Execute(ctx, IntIterator(1, 5))
		Expect(result).To(HaveExactElements([]int{1, 2}, []int{3, 4}, []int{5}))
	})

	It("aggregates parallel", func() {
		c := chain.New[int]()

		p := chain.New[int]()
		a := chain.AddAggregation[[]int](p, IntAggregator(2))
		pa := chain.AddParallel(c, a, pool)
		result := pa.Execute(ctx, IntIterator(1, 5))

		var values []int
		var lengthes []int
		for a := range result {
			for _, v := range a {
				values = append(values, v)
			}
			lengthes = append(lengthes, len(a))
		}
		Expect(values).To(ConsistOf(1, 2, 3, 4, 5))
		Expect(lengthes).To(ConsistOf(2, 2, 1))
	})
})

////////////////////////////////////////////////////////////////////////////////

// IntAggregator aggregates int values to pairs of int values.
func IntAggregator(n int) chain.Aggregator[int, []int] {
	return func() chain.Aggregation[int, []int] {
		return (&intAggregation{max: n})
	}
}

var _ chain.Aggregator[int, []int] = IntAggregator(2)

type intAggregation struct {
	lock   sync.Mutex
	max    int
	values []int
}

func (a *intAggregation) result() [][]int {
	values := a.values
	a.values = nil
	sort.Ints(values)
	return [][]int{values}
}

func (a *intAggregation) Aggregate(i int) [][]int {
	a.lock.Lock()
	defer a.lock.Unlock()
	//fmt.Printf("aggregate %d %t\n", i, final)

	a.values = append(a.values, i)
	if len(a.values) == a.max {
		return a.result()
	}
	return nil
}

func (a *intAggregation) Finish() [][]int {
	a.lock.Lock()
	defer a.lock.Unlock()
	//fmt.Printf("aggregate %d %t\n", i, final)

	return a.result()
}
