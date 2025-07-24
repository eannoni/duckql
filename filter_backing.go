package duckql

import (
	"fmt"
	"github.com/rqlite/sql"
	"reflect"
)

type SliceFilter struct {
	s    *SQLizer
	data []any

	tables        map[string]*Table
	resultColumns []*sql.ResultColumn
	filter        sql.Node
}

func (f *SliceFilter) rowInternal(d any) ResultRow {
	if !f.s.Matches(f.filter, d) {
		return nil
	}

	t := f.s.TypeForData(d)
	x := f.s.TableForData(d)
	v := reflect.ValueOf(d)

	result := ResultRow{}

	for _, column := range f.resultColumns {
		if column.Star.Line > 0 {
			for _, tableColumn := range x.Columns {
				result = append(result, ResultValue{Name: "*", Value: v.Elem().FieldByName(x.ColumnMappings[tableColumn].GoField)})
			}
			continue
		}

		switch e := column.Expr.(type) {
		case *sql.Ident:
			if field, ok := t.FieldByName(x.ColumnMappings[e.Name].GoField); ok {
				result = append(result, ResultValue{Name: e.Name, Value: v.Elem().FieldByName(field.Name)})
			}
		default:
			panic(fmt.Sprintf("unknown column type %T", column))
		}
	}

	return result
}

func (f *SliceFilter) Rows() ResultRows {
	var r ResultRows

	for _, d := range f.data {
		if reflect.TypeOf(d).Kind() == reflect.Slice {
			v := reflect.ValueOf(d)
			for i := 0; i < v.Len(); i++ {
				row := f.rowInternal(v.Index(i).Interface())
				if row != nil {
					r = append(r, row)
				}
			}
		} else {
			row := f.rowInternal(d)
			if row != nil {
				r = append(r, row)
			}
		}
	}

	f.tables = make(map[string]*Table)
	f.resultColumns = []*sql.ResultColumn{}

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
		f.resultColumns = append(f.resultColumns, t)
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
