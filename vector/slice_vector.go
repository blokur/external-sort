package vector

import (
	"sort"
	"sync"

	"github.com/askiada/external-sort/vector/key"
)

var _ Vector = &SliceVec{}

var elementPool = &sync.Pool{
	New: func() interface{} {
		return &Element{}
	},
}

func AllocateSlice(size int, allocateKey func(line string) (key.Key, error)) Vector {
	return &SliceVec{
		allocateKey: allocateKey,
		s:           make([]*Element, 0, size),
	}
}

type SliceVec struct {
	allocateKey func(line string) (key.Key, error)
	s           []*Element
}

func (v *SliceVec) Reset() {
	for i := range v.s {
		elementPool.Put(v.s[i])
	}
	v.s = v.s[:0]
}

func (v *SliceVec) Get(i int) *Element {
	return v.s[i]
}

func (v *SliceVec) Len() int {
	return len(v.s)
}

func (v *SliceVec) PushBack(line string) error {
	k, err := v.allocateKey(line)
	if err != nil {
		return err
	}
	// nolint:forcetypeassert // we know for the fact what the type is.
	e := elementPool.Get().(*Element)
	e.Line = line
	e.Key = k
	v.s = append(v.s, e)
	return nil
}

func (v *SliceVec) Sort() {
	sort.Slice(v.s, func(i, j int) bool {
		return Less(v.Get(i), v.Get(j))
	})
}

func (v *SliceVec) FrontShift() {
	elementPool.Put(v.s[0])
	v.s = v.s[1:]
}
