package duckql

import (
	"reflect"
	"strings"
)

type AggregateFunctionColumn struct {
	UnderlyingColumn string
	Function         AggregateFunction
}

type AggregateFunction func(ResultRows) ResultRows

var functionMap = map[string]AggregateFunction{
	"count": countRows,
}

func countRows(rows ResultRows) ResultRows {
	return ResultRows{
		ResultRow{
			{
				Column: "count",
				Value:  reflect.ValueOf(len(rows)),
			},
		},
	}
}

func ParseAggregateFunction(text string) *AggregateFunctionColumn {
	var column AggregateFunctionColumn

	var current strings.Builder
	for _, r := range text {
		switch r {
		case '(':
			functionName := current.String()
			if f, ok := functionMap[functionName]; ok {
				column.Function = f
			} else {
				return nil
			}

			current.Reset()
		case ')':
			column.UnderlyingColumn = current.String()
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}

	if current.String() == "" {
		return &column
	}

	return nil
}
