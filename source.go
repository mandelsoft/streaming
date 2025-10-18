package streaming

import "iter"

type Source[I any] interface {
	Source() (iter.Seq[I], error)
}

type SourceFunc[I any] func() (iter.Seq[I], error)

func (s SourceFunc[I]) Source() (iter.Seq[I], error) {
	return s()
}
