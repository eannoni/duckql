package duckql

import (
	"reflect"
	"strconv"

	"github.com/rqlite/sql"
)

type SliceFilter struct {
	s    *SQLizer
	data []any

	tables        map[string]*Table
	resultColumns []string
	filter        sql.Node
}

func (f *SliceFilter) Rows() ResultRows {
	var r ResultRows
	for _, d := range f.data {
		if !f.s.Matches(f.filter, d) {
			continue
		}

		t := f.s.TypeForData(d)
		x := f.s.TableForData(d)
		v := reflect.ValueOf(d)

		result := ResultRow{}

		for _, column := range f.resultColumns {
			if column == "*" {
				for _, tableColumn := range x.Columns {
					result = append(result, ResultValue{Column: column, Value: v.Elem().FieldByName(x.ColumnMappings[tableColumn].GoField)})
				}
				continue
			}

			if field, ok := t.FieldByName(x.ColumnMappings[column].GoField); ok {
				result = append(result, ResultValue{Column: column, Value: v.Elem().FieldByName(field.Name)})
			}
		}

		r = append(r, result)
	}

	f.tables = make(map[string]*Table)
	f.resultColumns = []string{}

	return r
}

func (f *SliceFilter) Visit(n sql.Node) (sql.Visitor, sql.Node, error) {
	if f.tables == nil {
		f.tables = make(map[string]*Table)
	}

	switch t := n.(type) {
	case *sql.QualifiedTableName:
		f.tables[f.s.Tables[t.TableName()].StructName] = f.s.Tables[t.TableName()]
	case *sql.ResultColumn:
		s := t.String()
		if x, err := strconv.Unquote(s); err == nil {
			s = x
		}

		f.resultColumns = append(f.resultColumns, s)
	}

	return f, n, nil
}

func (f *SliceFilter) VisitEnd(n sql.Node) (sql.Node, error) {
	switch t := n.(type) {
	case *sql.UnaryExpr, *sql.BinaryExpr:
		f.filter = t
	}
	return n, nil
}

func NewSliceFilter(s *SQLizer, data []any) *SliceFilter {
	return &SliceFilter{
		s:    s,
		data: data,
	}
}
