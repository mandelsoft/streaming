package chain

import (
	"context"
	"iter"

	"github.com/mandelsoft/goutils/iterutils"
	"github.com/mandelsoft/streaming/internal"
)

type Chain[I, O any] interface {
	impl() internal.Chain

	Execute(ctx context.Context, seq iter.Seq[I], name ...string) iter.Seq[O]
	ExecuteWithConfig(ctx context.Context, cfg any, seq iter.Seq[I], name ...string) iter.Seq[O]
}

func New[I any]() Chain[I, I] {
	return convertChain[I, I](internal.New())
}

////////////////////////////////////////////////////////////////////////////////

func chainImpl[I, O any](c Chain[I, O]) internal.Chain {
	if c == nil {
		return internal.New()
	}
	return c.impl()
}

func convertChain[I, O any](c internal.Chain) Chain[I, O] {
	return &convertingChain[I, O]{c}
}

type convertingChain[I, O any] struct {
	chain internal.Chain
}

var _ Chain[int, string] = (*convertingChain[int, string])(nil)

func (c *convertingChain[I, O]) impl() internal.Chain {
	return c.chain
}

func (c *convertingChain[I, O]) Execute(ctx context.Context, seq iter.Seq[I], name ...string) iter.Seq[O] {
	return iterutils.Convert[O](c.chain.Execute(ctx, iterutils.Convert[any](seq), name...))
}

func (c *convertingChain[I, O]) ExecuteWithConfig(ctx context.Context, cfg any, seq iter.Seq[I], name ...string) iter.Seq[O] {
	return iterutils.Convert[O](c.chain.ExecuteWithConfig(ctx, cfg, iterutils.Convert[any](seq), name...))
}
