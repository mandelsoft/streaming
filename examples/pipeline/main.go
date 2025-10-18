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

	p := NewPipeline(c_sort)

	p.SetDir(".")

	result, err := p.Execute(context.Background())
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

type Source struct {
	dir string
}

var _ streaming.Source[string] = (*Source)(nil)

func (s *Source) Source() (iter.Seq[string], error) {
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

func (s *Source) SetDir(dir string) {
	s.dir = dir
}

////////////////////////////////////////////////////////////////////////////////
// Final Processor

func Processor() streaming.Processor[string, string] {
	return func(ctx context.Context, in iter.Seq[string]) string {
		s := ""
		for v := range in {
			if s != "" {
				s += ", "
			}
			s += v
		}
		return s
	}
}

////////////////////////////////////////////////////////////////////////////////

type Pipeline struct {
	streaming.Pipeline[string, string, *Source, streaming.Sink[string, string]]
}

func NewPipeline(c chain.Chain[string, string]) *Pipeline {
	p := streaming.NewPipeline(&Source{}, streaming.NewSink(c, Processor))
	return &Pipeline{
		p,
	}
}

func (p *Pipeline) SetDir(dir string) {
	p.Source().SetDir(dir)
}
