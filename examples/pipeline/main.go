package main

import (
	"context"
	"fmt"
	"iter"
	"os"
	"strings"

	"github.com/mandelsoft/streaming"
	"github.com/mandelsoft/streaming/chain"
)

func main() {
	c_go := chain.AddFilter(chain.New[string](), FilterIncludeSuffix(".go"))
	c_nontest := chain.AddFilter(c_go, FilterExcludeSuffix("_test.go"))
	c_sort := chain.AddSort(c_nontest, strings.Compare)

	def := streaming.DefinePipeline[string, string](streaming.SourceFactoryFunc[string, string](NewSource), c_sort, nil)

	def = def.WithProcessor(streaming.ProcessorFactoryFunc[string, string, string](NewProcessor))

	result, err := def.Execute(context.Background(), ".")
	if err != nil {
		panic(err)
	}

	fmt.Printf("result list: %s\n", result)
}

////////////////////////////////////////////////////////////////////////////////
// Step implementations

func FilterExcludeSuffix(suffix string) chain.Filter[string] {
	return func(in string) bool {
		r := !strings.HasSuffix(in, suffix)
		return r
	}
}

func FilterIncludeSuffix(suffix string) chain.Filter[string] {
	return func(in string) bool {
		r := strings.HasSuffix(in, suffix)
		return r
	}
}

////////////////////////////////////////////////////////////////////////////////
// Source

func NewSource(cfg string) (streaming.Source[string], error) {
	return &Source{cfg}, nil
}

type Source struct {
	dir string
}

var _ streaming.Source[string] = (*Source)(nil)

func (s *Source) Elements() (iter.Seq[string], error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	return func(yield func(string) bool) {
		for _, e := range entries {
			if !yield(e.Name()) {
				return
			}
		}
	}, nil
}

////////////////////////////////////////////////////////////////////////////////
// Final Processor

func NewProcessor(cfg string) (streaming.Processor[string, string], error) {
	return func(ctx context.Context, i iter.Seq[string]) (string, error) {
		s := ""
		for e := range i {
			if s != "" {
				s += ", "
			}
			s += e
		}
		return s, nil
	}, nil
}
