package chain

import "context"

type Step interface {
	String() string
	Renamed(name string) Step
	sequential(ctx context.Context) executor
	parallel(ctx context.Context, factory executionFactory) executionFactory
}

type step struct {
	key  string
	name string
}

func (s *step) GetName() string {
	return s.name
}

func (s *step) GetKey() string {
	return s.key
}
