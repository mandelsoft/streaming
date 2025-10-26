package internal

import (
	"github.com/mandelsoft/streaming/processing"
)

type nestedChain struct {
	step
	chain *chain
	pool  processing.Processing
}

func (c *nestedChain) Renamed(name string) *nestedChain {
	n := *c
	n.name = name
	return &n
}
