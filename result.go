package duckql

import (
	"fmt"
	"reflect"
	"strings"
	"time"
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

type ResultRow []ResultValue

func (r *ResultRow) String() string {
	var s strings.Builder
	for i, v := range *r {
		if i > 0 {
			s.WriteString("|")
		}
		switch v.Value.Kind() {
		case reflect.Bool:
			s.WriteString(fmt.Sprintf("%t", v.Value.Bool()))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			s.WriteString(fmt.Sprintf("%d", v.Value.Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			s.WriteString(fmt.Sprintf("%d", v.Value.Uint()))
		case reflect.Float32, reflect.Float64:
			s.WriteString(fmt.Sprintf("%f", v.Value.Float()))
		case reflect.String:
			s.WriteString(fmt.Sprintf("%s", v.Value.String()))
		case reflect.Struct:
			if v.Value.Type().Name() == "Time" {
				s.WriteString(fmt.Sprintf("%d", v.Value.Interface().(time.Time).Unix()))
			}
		default:
			s.WriteString(fmt.Sprintf("%v", v.Value.Interface()))
		}
	}
	return s.String()
}

type ResultValue struct {
	Name  string
	Value reflect.Value
}
