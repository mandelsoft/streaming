package main

import (
	"context"
	"fmt"
	"github.com/mandelsoft/streaming/chain"
	"github.com/mandelsoft/streaming/simplepool"
	"iter"
	"strings"
)

func MapIntToString(i int) string {
	r := fmt.Sprintf("%3d", i)
	return r
}

func MapAppendToString(suffix string) chain.Mapper[string, string] {
	return func(in string) string {
		r := in + suffix
		// fmt.Printf("append %s->%s\n", in, r)
		return r
	}
}

func ExplodeAppendIndexToString(index ...rune) chain.Exploder[string, string] {
	return func(in string) []string {
		out := make([]string, len(index))
		for i, a := range index {
			out[i] = in + string(a)
		}
		return out
	}
}

func ExcludeSuffix(suffix string) chain.Filter[string] {
	return func(in string) bool {
		r := !strings.HasSuffix(in, suffix)
		if !r {
			// fmt.Printf("exclude %s\n", in)
		}
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

func main() {
	pool := simplepool.New(nil, 3)

	c := chain.New[int]()

	i := chain.AddMap(c, Inc)

	pc := chain.New[int]()
	pm := chain.AddMap(pc, MapIntToString)

	ac := chain.New[string]()
	ae := chain.AddExplode(ac, ExplodeAppendIndexToString('a', 'b', 'c'))
	af := chain.AddFilter(ae, ExcludeSuffix("b"))
	am := chain.AddMap(af, MapAppendToString("."))
	//as := chain.AddSort(am, strings.Compare)

	pn := chain.AddSequential(pm, am)
	p := chain.AddParallel(i, pn, pool)
	for v := range p.Execute(context.Background(), IntIterator(1, 10)) {
		fmt.Println(v)
	}

	pool.Close()

}
