package main

import (
	"context"
	"fmt"
	"github.com/mandelsoft/streaming/simplepool"
	"github.com/mandelsoft/streaming/untypedchain"
	"iter"
	"strings"
)

func MapIntToString(i any) any {
	r := fmt.Sprintf("%3d", i.(int))
	return r
}

func MapAppendToString(suffix string) untypedchain.Mapper {
	return func(in any) any {
		r := in.(string) + suffix
		// fmt.Printf("append %s->%s\n", in, r)
		return r
	}
}

func ExplodeAppendIndexToString(index ...rune) untypedchain.Exploder {
	return func(in any) []any {
		out := make([]any, len(index))
		for i, a := range index {
			out[i] = in.(string) + string(a)
		}
		return out
	}
}

func ExcludeSuffix(suffix string) untypedchain.Filter {
	return func(in any) bool {
		r := !strings.HasSuffix(in.(string), suffix)
		return r
	}
}

func Inc(i any) any {
	return i.(int) + 1
}

func IntIterator(s, e int) iter.Seq[any] {
	return func(yield func(any) bool) {
		for i := s; i <= e; i++ {
			if !yield(i) {
				return
			}
		}
	}
}

func main() {
	pool := simplepool.New(nil, 3)

	s := untypedchain.New().
		Explode(ExplodeAppendIndexToString('a', 'b', 'c')).
		Filter(ExcludeSuffix("b")).Map(MapAppendToString("."))

	p := untypedchain.New().Map(MapIntToString).Sequential(s)

	c := untypedchain.New().Map(Inc).Parallel(p, pool)
	for v := range c.Execute(context.Background(), IntIterator(1, 10)) {
		fmt.Println(v)
	}

	pool.Close()

}
