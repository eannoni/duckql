package ddllm

import (
	"github.com/rqlite/sql"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type SliceFilter struct {
	s    *SQLizer
	data []any

	tables        map[string]*Table
	resultColumns []string
	filter        []sql.Node
}

func (f *SliceFilter) valueOf(n sql.Node, v reflect.Value, table *Table) reflect.Value {
	switch t := n.(type) {
	case *sql.NumberLit:
		i, err := strconv.Atoi(t.Value)
		if err != nil {
			panic(err)
		}
		return reflect.ValueOf(i)
	case *sql.StringLit:
		return reflect.ValueOf(t.Value)
	case *sql.Ident:
		fieldName := table.ColumnStructFields[t.Name]
		return v.Elem().FieldByName(fieldName)
	default:
		panic("unhandled type: " + reflect.TypeOf(n).String())
	}
}

func (f *SliceFilter) matches(v reflect.Value, table *Table) bool {
	for _, filter := range f.filter {
		switch t := filter.(type) {
		case *sql.BinaryExpr:
			x := f.valueOf(t.X, v, table)
			y := f.valueOf(t.Y, v, table)

			switch t.Op {
			case sql.EQ:
				return reflect.DeepEqual(x.Interface(), y.Interface())
			case sql.NE:
				return !reflect.DeepEqual(x.Interface(), y.Interface())
			case sql.GT:
				return x.Int() > y.Int()
			case sql.LIKE:
				match := strings.ReplaceAll(y.String(), "%", ".*")
				matched, err := regexp.MatchString(match, x.String())
				if err != nil {
					return false
				}
				return matched
			case sql.NOTLIKE:
				match := strings.ReplaceAll(y.String(), "%", ".*")
				matched, err := regexp.MatchString(match, x.String())
				if err != nil {
					return false
				}
				return !matched
			}
		}
	}

	return false
}

func (f *SliceFilter) Result() []any {
	var r []any
	for _, d := range f.data {
		t := reflect.TypeOf(d)

		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}

		v := reflect.ValueOf(d)

		if x, ok := f.tables[t.Name()]; ok {
			if !f.matches(v, x) {
				continue
			}

			n := reflect.New(t)

			for _, column := range f.resultColumns {
				if column == "*" {
					for _, tableColumn := range x.Columns {
						n.Elem().FieldByName(x.ColumnStructFields[tableColumn]).Set(v.Elem().FieldByName(x.ColumnStructFields[tableColumn]))
					}
					continue
				}

				if field, ok := t.FieldByName(x.ColumnStructFields[column]); ok {
					n.Elem().FieldByName(field.Name).Set(v.Elem().FieldByName(field.Name))
				}
			}

			r = append(r, n.Interface())
		} else {
			continue
		}
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
	case *sql.BinaryExpr:
		f.filter = append(f.filter, t)
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
	return n, nil
}

func NewSliceFilter(s *SQLizer, data []any) *SliceFilter {
	return &SliceFilter{
		s:    s,
		data: data,
	}
}
