package streaming

import "iter"

type SourceFactory[C, I any] interface {
	Elements(C) (iter.Seq[I], error)
}

type SourceFactoryFunc[C, I any] func(C) (iter.Seq[I], error)

func (s SourceFactoryFunc[C, I]) Elements(cfg C) (iter.Seq[I], error) {
	return s(cfg)
}
