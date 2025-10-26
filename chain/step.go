package chain

import "github.com/mandelsoft/streaming/internal"

type Step[I, O any] interface {
	impl() internal.Step
}

type step[I, O any] struct {
	step internal.Step
}

func (s *step[I, O]) impl() internal.Step {
	return s.step
}

func (s *step[I, O]) String() string {
	return s.step.String()
}

////////////////////////////////////////////////////////////////////////////////

func AddStep[N, I, O any](base Chain[I, O], s Step[O, N]) Chain[I, N] {
	c := chainImpl(base).Step(s.impl())
	return convertChain[I, N](c)
}

func ForStep[I, O any](s Step[I, O]) Chain[I, O] {
	return AddStep[O, I, O](nil, s)
}
