package duckql

import (
	"reflect"
	"strconv"
	"strings"
)

type AggregateFunctionColumn struct {
	UnderlyingColumn string
	Function         AggregateFunction
}

func (a *AggregateFunctionColumn) Call(rows ResultRows) ResultRows {
	return a.Function(a, rows)
}

type AggregateFunction func(*AggregateFunctionColumn, ResultRows) ResultRows

var functionMap = map[string]AggregateFunction{
	"count": countRows,
	"avg":   averageOfColumn,
}

func countRows(_ *AggregateFunctionColumn, rows ResultRows) ResultRows {
	return ResultRows{
		ResultRow{
			{
				Name:  "count",
				Value: reflect.ValueOf(len(rows)),
			},
		},
	}
}

func averageOfColumn(c *AggregateFunctionColumn, rows ResultRows) ResultRows {
	var sum float64

	for _, row := range rows {
		for _, column := range row {
			if column.Name == c.UnderlyingColumn {
				switch column.Value.Kind() {
				case reflect.Float32, reflect.Float64:
					sum += column.Value.Float()
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					sum += float64(column.Value.Int())
				}
			}
		}
	}

	return ResultRows{
		ResultRow{
			{
				Name:  "average",
				Value: reflect.ValueOf(sum / float64(len(rows))),
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
			var err error
			column.UnderlyingColumn, err = strconv.Unquote(current.String())
			if err != nil {
				column.UnderlyingColumn = current.String()
			}
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
