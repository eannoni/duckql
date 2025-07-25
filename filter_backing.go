package duckql

import (
	"github.com/rqlite/sql"
	"reflect"
)

type SliceFilter struct {
	exec *QueryExecutor
	s    *SQLizer
	data []any
}

func (f *SliceFilter) Rows() ResultRows {
	return f.exec.Rows()
}

func (f *SliceFilter) FillIntermediate(table *IntermediateTable) {
	if table.Source == nil {
		panic("cannot fill intermediate without a table")
	}

	for _, column := range table.Source.Columns {
		table.Columns = append(table.Columns, column)
	}

	for _, d := range f.data {
		if reflect.TypeOf(d).Kind() == reflect.Slice {
			v := reflect.ValueOf(d)

			if v.Len() == 0 {
				continue
			}

			if f.s.TableForData(v.Index(0).Interface()) != table.Source {
				continue
			}

			for i := 0; i < v.Len(); i++ {
				var result ResultRow
				for _, column := range table.Source.Columns {
					result = append(result, ResultValue{Name: column, Value: v.Index(i).Elem().FieldByName(table.Source.ColumnMappings[column].GoField)})
				}
				table.Rows = append(table.Rows, result)
			}
		} else {
			v := reflect.ValueOf(d)

			var result ResultRow
			for _, column := range table.Source.Columns {
				result = append(result, ResultValue{Name: column, Value: v.Elem().FieldByName(table.Source.ColumnMappings[column].GoField)})
			}

			table.Rows = append(table.Rows, result)
		}
	}
}

func (f *SliceFilter) Visit(n sql.Node) (sql.Visitor, sql.Node, error) {
	if f.exec == nil {
		f.exec = NewQueryExecutor(f.s, f.FillIntermediate)
	}

	return f.exec.Visit(n)
}

func (f *SliceFilter) VisitEnd(n sql.Node) (sql.Node, error) {

	return n, nil
}

func NewSliceFilter(s *SQLizer, data []any) *SliceFilter {
	return &SliceFilter{
		s:    s,
		data: data,
	}
}
