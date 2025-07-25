package duckql

import (
	"github.com/rqlite/sql"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type Table struct {
	Name           string
	StructName     string
	Columns        []string
	ColumnMappings map[string]ColumnMapping
	ForeignKeys    map[string]*Table
}

type IntermediateTable struct {
	Source  *Table
	Aliases map[string]string
	Columns []string
	Rows    ResultRows
}

func (i *IntermediateTable) evaluate(n sql.Node, row ResultRow) reflect.Value {
	switch t := n.(type) {
	case *sql.BinaryExpr:
		x := i.evaluate(t.X, row)
		y := i.evaluate(t.Y, row)

		switch t.Op {
		case sql.AND:
			return reflect.ValueOf(x.Bool() && y.Bool())
		case sql.OR:
			return reflect.ValueOf(x.Bool() || y.Bool())
		case sql.EQ:
			return reflect.ValueOf(reflect.DeepEqual(x.Interface(), y.Interface()))
		case sql.NE:
			return reflect.ValueOf(!reflect.DeepEqual(x.Interface(), y.Interface()))
		case sql.GT:
			return reflect.ValueOf(x.Int() > y.Int())
		case sql.GE:
			return reflect.ValueOf(x.Int() >= y.Int())
		case sql.LT:
			return reflect.ValueOf(x.Int() < y.Int())
		case sql.LE:
			return reflect.ValueOf(x.Int() <= y.Int())
		case sql.LIKE:
			match := strings.ReplaceAll(y.String(), "%", ".*")
			matched, err := regexp.MatchString(match, x.String())
			if err != nil {
				return reflect.ValueOf(false)
			}
			return reflect.ValueOf(matched)
		case sql.NOTLIKE:
			match := strings.ReplaceAll(y.String(), "%", ".*")
			matched, err := regexp.MatchString(match, x.String())
			if err != nil {
				return reflect.ValueOf(false)
			}
			return reflect.ValueOf(!matched)
		}
	case *sql.NumberLit:
		i, err := strconv.Atoi(t.Value)
		if err != nil {
			panic(err)
		}
		return reflect.ValueOf(i)
	case *sql.StringLit:
		return reflect.ValueOf(t.Value)
	case *sql.QualifiedRef:
		lh := t.Table.Name
		rh := "*"
		if t.Column != nil {
			rh = t.Column.Name
		}
		ref := lh + "." + rh

		for idx, column := range i.Columns {
			if ref == column {
				return row[idx].Value
			}

			if i.Aliases[lh]+"."+rh == column {
				return row[idx].Value
			}
		}

		return reflect.ValueOf(nil)

	case *sql.Ident:
		for idx, column := range i.Columns {
			if t.Name == column {
				return row[idx].Value
			}

			if i.Source != nil && i.Source.Name+"."+column == t.Name {
				return row[idx].Value
			}
		}

		return reflect.ValueOf(nil)
	default:
		panic("unhandled type: " + reflect.TypeOf(n).String())
	}
	return reflect.ValueOf(false)
}

func (i *IntermediateTable) Filter(n sql.Node) *IntermediateTable {
	var result IntermediateTable

	if n == nil {
		return i
	}

	result.Source = i.Source
	result.Aliases = i.Aliases
	result.Columns = i.Columns

	for _, row := range i.Rows {
		v := i.evaluate(n, row)

		if v.Kind() == reflect.Bool && v.Bool() {
			result.Rows = append(result.Rows, row)
		}
	}

	return &result
}

func NewIntermediateTable() *IntermediateTable {
	return &IntermediateTable{
		Aliases: make(map[string]string),
	}
}

func (s *SQLizer) valueOf(n sql.Node, v reflect.Value, mappings map[string]ColumnMapping) reflect.Value {
	switch t := n.(type) {
	case *sql.BinaryExpr:
		x := s.valueOf(t.X, v, mappings)
		y := s.valueOf(t.Y, v, mappings)

		switch t.Op {
		case sql.AND:
			return reflect.ValueOf(x.Bool() && y.Bool())
		case sql.OR:
			return reflect.ValueOf(x.Bool() || y.Bool())
		case sql.EQ:
			return reflect.ValueOf(reflect.DeepEqual(x.Interface(), y.Interface()))
		case sql.NE:
			return reflect.ValueOf(!reflect.DeepEqual(x.Interface(), y.Interface()))
		case sql.GT:
			return reflect.ValueOf(x.Int() > y.Int())
		case sql.GE:
			return reflect.ValueOf(x.Int() >= y.Int())
		case sql.LT:
			return reflect.ValueOf(x.Int() < y.Int())
		case sql.LE:
			return reflect.ValueOf(x.Int() <= y.Int())
		case sql.LIKE:
			match := strings.ReplaceAll(y.String(), "%", ".*")
			matched, err := regexp.MatchString(match, x.String())
			if err != nil {
				return reflect.ValueOf(false)
			}
			return reflect.ValueOf(matched)
		case sql.NOTLIKE:
			match := strings.ReplaceAll(y.String(), "%", ".*")
			matched, err := regexp.MatchString(match, x.String())
			if err != nil {
				return reflect.ValueOf(false)
			}
			return reflect.ValueOf(!matched)
		}
	case *sql.NumberLit:
		i, err := strconv.Atoi(t.Value)
		if err != nil {
			panic(err)
		}
		return reflect.ValueOf(i)
	case *sql.StringLit:
		return reflect.ValueOf(t.Value)
	case *sql.Ident:
		return v.Elem().FieldByName(mappings[t.Name].GoField)
	default:
		panic("unhandled type: " + reflect.TypeOf(n).String())
	}
	return reflect.ValueOf(false)
}

func (s *SQLizer) Matches(filter sql.Node, data any) bool {
	table := s.TableForData(data)
	if table == nil {
		return false
	}

	if filter == nil {
		return true
	}

	value := s.valueOf(filter, reflect.ValueOf(data), table.ColumnMappings)
	if value.Kind() == reflect.Bool {
		return value.Bool()
	}

	return false
}
