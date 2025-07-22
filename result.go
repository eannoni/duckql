package duckql

import (
	"fmt"
	"reflect"
	"strings"
)

type ResultRows []ResultRow

func (r *ResultRows) String() string {
	var s strings.Builder
	for i, row := range *r {
		if i > 0 {
			s.WriteString("\n")
		}
		s.WriteString(row.String())
	}
	return s.String()
}

type ResultRow []reflect.Value

func (r *ResultRow) String() string {
	var s strings.Builder
	for i, v := range *r {
		if i > 0 {
			s.WriteString("|")
		}
		switch v.Kind() {
		case reflect.Bool:
			s.WriteString(fmt.Sprintf("%t", v.Bool()))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			s.WriteString(fmt.Sprintf("%d", v.Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			s.WriteString(fmt.Sprintf("%d", v.Uint()))
		case reflect.Float32, reflect.Float64:
			s.WriteString(fmt.Sprintf("%f", v.Float()))
		case reflect.String:
			s.WriteString(fmt.Sprintf("%s", v.String()))
		default:
			s.WriteString(fmt.Sprintf("%v", v.Interface()))
		}
	}
	return s.String()
}
