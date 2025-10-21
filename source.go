package streaming

import "iter"

type Source[I any] interface {
	Elements() (iter.Seq[I], error)
}

type SourceFunc[I any] func() (iter.Seq[I], error)

func (s SourceFunc[I]) Elements() (iter.Seq[I], error) {
	return s()
}

type SourceFactory[C, I any] interface {
	Source(cfg C) (Source[I], error)
}

type SourceFactoryFunc[C, I any] func(cfg C) (Source[I], error)

func (s SourceFactoryFunc[C, I]) Source(cfg C) (Source[I], error) {
	return s(cfg)
}
