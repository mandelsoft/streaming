package chain

import (
	"context"
	"github.com/mandelsoft/goutils/generics"
)

var keyName = "chain config"
var key = &keyName

////////////////////////////////////////////////////////////////////////////////

func WithConfig(ctx context.Context, cfg any) context.Context {
	return context.WithValue(ctx, key, cfg)
}

func GetConfig[C any](ctx context.Context) C {
	return generics.Cast[C](ctx.Value(key))
}
