package dbx

import (
	"fmt"
	"strconv"
)

type Money float64

func (m Money) ToFloat64 () float64 {
	res, _ := strconv.ParseFloat(fmt.Sprintf("%2.f", m), 64)
	return res
}

func (m Money) ToString () string {
	return fmt.Sprintf("%2.f", m)
}

type Enum string

type EnumDefinition struct {
	Key 	  string
	Reference string
	Value     map[string]string
}