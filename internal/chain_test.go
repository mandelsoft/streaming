package internal_test

import (
	"fmt"
	"iter"
	"strings"

	mine "github.com/mandelsoft/streaming/internal"
)

////////////////////////////////////////////////////////////////////////////////

func MapAppendToString(suffix any) mine.Mapper {
	return func(in any) any {
		r := in.(string) + suffix.(string)
		// fmt.Printf("append %s->%s\n", in, r)
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

func ExplodeAppendToString(suffix ...string) mine.Exploder {
	return func(in any) []any {
		out := make([]any, len(suffix))
		for i, a := range suffix {
			out[i] = in.(string) + string(a)
		}
		return out
	}
}

func FilterExcludeSuffix(suffix string) mine.Filter {
	return func(in any) bool {
		r := !strings.HasSuffix(in.(string), suffix)
		if !r {
			// fmt.Printf("exclude %s\n", in)
		}
		return r
	}
}

func MapIntToString(i any) any {
	r := fmt.Sprintf("%3d", i)
	return r
}
