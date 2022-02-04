package vector

import (
	"bufio"
	"os"
	"strconv"

	"github.com/pkg/errors"
)

var _ Vector = &IntVec{}

func AllocateIntVector(size int) Vector {
	return &IntVec{
		s: make([]Element, 0, size),
	}
}

type IntVec struct {
	s []Element
}

func (*IntVec) newElement(value string) *element {
	i, err := strconv.Atoi(value)
	if err != nil {
		panic(errors.Wrap(err, "converting value from string"))
	}

	return &element{
		line: value,
		i:    i,
	}
}

func (v *IntVec) Get(i int) Element {
	return v.s[i]
}

func (v *IntVec) End() int {
	return len(v.s)
}

func (v *IntVec) insert(i int, value string) error {
	v.s = append(v.s[:i], append([]Element{v.newElement(value)}, v.s[i:]...)...)
	return nil
}

func (v *IntVec) PushBack(value string) error {
	v.s = append(v.s, v.newElement(value))
	return nil
}

func (v *IntVec) Less(v1, v2 Element) bool {
	return v1.Less(v2)
}

func (v *IntVec) convertFromString(value string) (Element, error) {
	return v.newElement(value), nil
}

func (v *IntVec) Dump(filename string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return errors.Errorf("failed creating file: %s", err)
	}
	datawriter := bufio.NewWriter(file)

	for _, data := range v.s {
		_, err = datawriter.WriteString(data.Value() + "\n")
		if err != nil {
			return errors.Errorf("failed writing file: %s", err)
		}
	}
	datawriter.Flush()
	file.Close()
	return nil
}

func (v *IntVec) FrontShift() {
	v.s = v.s[1:]
}
