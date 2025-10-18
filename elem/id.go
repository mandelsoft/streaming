package elem

import (
	"fmt"
	"slices"
)

type Level struct {
	no  int
	max int
}

func NewLevel(no int, max int) Level {
	return Level{no, max}
}

func (l Level) String() string {
	return fmt.Sprintf("%d[%d]", l.no, l.max)
}

func (l Level) Max() int {
	return l.max
}

func (l Level) No() int {
	return l.no
}

////////////////////////////////////////////////////////////////////////////////

type Id []Level

func NewId(i int, max int) Id {
	return Id{NewLevel(i, max)}
}

func (i Id) String() string {
	return fmt.Sprintf("%v", []Level(i))
}

func (i Id) Len() int {
	return len(i)
}

func (i Id) Add(l Level) Id {
	n := slices.Clone(i)
	return append(n, l)
}

func (i Id) Add2(no, max int) Id {
	n := slices.Clone(i)
	return append(n, NewLevel(no, max))
}

func (i Id) WithMax(max int) Id {
	n := slices.Clone(i)
	n[0].max = max
	return n
}
