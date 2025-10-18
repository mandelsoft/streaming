package chain_test

import (
	"context"
	"github.com/mandelsoft/logging"
	"github.com/mandelsoft/logging/logrusl"
	"github.com/mandelsoft/streaming/chain"
	"github.com/mandelsoft/streaming/processing"
	"github.com/mandelsoft/streaming/simplepool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"iter"
	"strings"
)

var _ = Describe("Chain", func() {
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
		c := chain.New[int]()

		s := chain.New[int]()
		si := chain.AddMap(s, Inc)
		sm := chain.AddMap(si, MapIntToString)
		c_parallel := chain.AddParallel(c, sm, pool)

		result := c_parallel.Execute(ctx, IntIterator(1, 4))
		Expect(result).To(ConsistOf("  2", "  3", "  4", "  5"))
	})

	It("stable", func() {
		c := chain.New[int]()

		s := chain.New[int]()
		si := chain.AddMap(s, Inc)
		sm := chain.AddMap(si, MapIntToString)
		ss := chain.AddSort(sm, strings.Compare)
		c_parallel := chain.AddStable(c, ss, pool)

		result := c_parallel.Execute(ctx, IntIterator(1, 4))
		Expect(result).To(HaveExactElements("  2", "  3", "  4", "  5"))
	})

	It("combines ecerything", func() {
		c := chain.New[int]()

		c_inc := chain.AddMap(c, Inc)

		p := chain.New[int]()
		p_map := chain.AddMap(p, MapIntToString)

		s := chain.New[string]()
		s_exp := chain.AddExplode(s, ExplodeAppendToString("a", "b", "c"))
		s_exc := chain.AddFilter(s_exp, FilterExcludeSuffix("b"))
		//as := chain.AddSort(am, strings.Compare)

		p_seq := chain.AddSequential(p_map, s_exc)
		p_app := chain.AddMap(p_seq, MapAppendToString("."))

		c_par := chain.AddStable(c_inc, p_app, pool)
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

////////////////////////////////////////////////////////////////////////////////

func MapAppendToString(suffix string) chain.Mapper[string, string] {
	return func(in string) string {
		r := in + suffix
		// fmt.Printf("append %s->%s\n", in, r)
		return r
	}
}

func Inc(i int) int {
	return i + 1
}

func IntIterator(s, e int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for i := s; i <= e; i++ {
			if !yield(i) {
				return
			}
		}
	}
}
