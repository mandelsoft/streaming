package internal

import (
	"context"
	"fmt"
	"iter"

	"github.com/mandelsoft/goutils/iterutils"
)

type Transformer = func(a []any) []any

type transformStep struct {
	gatherStep
	transformer Transformer
}

func (s *transformStep) String() string {
	return fmt.Sprintf("transformStep[%s]", s.name)
}

func (s *transformStep) Renamed(name string) Step {
	n := *s
	n.name = name
	n.all = n.transform
	return &n
}

func (s *transformStep) transform(ctx context.Context, in []any) iter.Seq[any] {
	out := s.transformer(in)
	return iterutils.For(out...)
}

////////////////////////////////////////////////////////////////////////////////

var transformId = newDefaultName("Transform")

func TransformStep(m Transformer, name ...string) Step {
	s := &transformStep{
		gatherStep:  gatherStep{step: transformId.Step(name...)},
		transformer: m,
	}
	s.all = s.transform
	return s
}

func (c *chain) Transform(m Transformer, name ...string) Chain {
	return c.Step(TransformStep(m, name...))
}
