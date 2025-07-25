package duckql

import (
	"reflect"
)

type AggregateFunctionColumn struct {
	UnderlyingColumn string
	ResultPosition   int
	Function         AggregateFunction
}

func (a *AggregateFunctionColumn) Call(rows ResultRows) ResultRows {
	return a.Function(a, rows)
}

type AggregateFunction func(*AggregateFunctionColumn, ResultRows) ResultRows

var functionMap = map[string]AggregateFunction{
	"avg":   averageOfColumn,
	"count": countRows,
	"max":   maxOfColumn,
	"min":   minOfColumn,
	"sum":   sumOfColumn,
	"total": sumOfColumn,
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
				goto nextRow
			}
		}

	nextRow:
	}

	resultRow := rows[len(rows)-1]
	resultRow[c.ResultPosition] = ResultValue{
		Name:  "average",
		Value: reflect.ValueOf(sum / float64(len(rows))),
	}

	return ResultRows{
		resultRow,
	}
}

func countRows(c *AggregateFunctionColumn, rows ResultRows) ResultRows {
	resultRow := rows[len(rows)-1]
	resultRow[c.ResultPosition] = ResultValue{
		Name:  "count",
		Value: reflect.ValueOf(len(rows)),
	}

	return ResultRows{
		resultRow,
	}
}

func maxOfColumn(c *AggregateFunctionColumn, rows ResultRows) ResultRows {
	var max reflect.Value
	var maxRow ResultRow

	for _, row := range rows {
		for _, column := range row {
			if column.Name == c.UnderlyingColumn {
				if !max.IsValid() {
					max = column.Value
					maxRow = row
					goto nextRow
				} else {
					switch column.Value.Kind() {
					case reflect.Float32, reflect.Float64:
						if column.Value.Float() > max.Float() {
							max = column.Value
							maxRow = row
						}
					case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						if column.Value.Int() > max.Int() {
							max = column.Value
							maxRow = row
						}
					case reflect.String:
						if column.Value.String() > max.String() {
							max = column.Value
							maxRow = row
						}
					}
					goto nextRow
				}
			}
		}
	nextRow:
	}

	return ResultRows{
		maxRow,
	}
}

func minOfColumn(c *AggregateFunctionColumn, rows ResultRows) ResultRows {
	var min reflect.Value
	var minRow ResultRow

	for _, row := range rows {
		for _, column := range row {
			if column.Name == c.UnderlyingColumn {
				if !min.IsValid() {
					min = column.Value
					minRow = row
					goto nextRow
				} else {
					switch column.Value.Kind() {
					case reflect.Float32, reflect.Float64:
						if column.Value.Float() < min.Float() {
							min = column.Value
							minRow = row
						}
					case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						if column.Value.Int() < min.Int() {
							min = column.Value
							minRow = row
						}
					case reflect.String:
						if column.Value.String() < min.String() {
							min = column.Value
							minRow = row
						}
					}
					goto nextRow
				}
			}
		}
	nextRow:
	}

	return ResultRows{
		minRow,
	}
}

func sumOfColumn(c *AggregateFunctionColumn, rows ResultRows) ResultRows {
	var sum reflect.Value
	var sumRow ResultRow

	for _, row := range rows {
		for _, column := range row {
			if column.Name == c.UnderlyingColumn {
				if !sum.IsValid() {
					sum = column.Value
					goto nextRow
				} else {
					switch column.Value.Kind() {
					case reflect.Float32, reflect.Float64:
						sum = reflect.ValueOf(sum.Float() + column.Value.Float())
					case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						sum = reflect.ValueOf(sum.Int() + column.Value.Int())
					}
					goto nextRow
				}
			}
		}
	nextRow:
	}

	sumRow = rows[len(rows)-1]
	sumRow[c.ResultPosition] = ResultValue{
		Name:  "sum",
		Value: sum,
	}

	return ResultRows{
		sumRow,
	}
}
