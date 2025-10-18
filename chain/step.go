package chain

type Step interface {
	String() string
	Renamed(name string) Step
	sequential() executor
	parallel(factory executionFactory) executionFactory
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
