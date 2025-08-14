package duckql

import (
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/rqlite/sql"
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

func coerceToInt(x reflect.Value) *int64 {
	var i int64
	switch x.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i = x.Int()

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		i = x.Int()

	case reflect.Bool:
		if x.Bool() {
			i = 1
		}

	case reflect.Struct:
		if x.Type().Name() == "Time" {
			i = x.Interface().(time.Time).Unix()
		} else {
			return nil
		}
	default:
		return nil
	}

	return &i
}

func (i *IntermediateTable) evaluate(n sql.Node, row ResultRow) reflect.Value {
	switch t := n.(type) {
	case *sql.BinaryExpr:
		x := i.evaluate(t.X, row)
		y := i.evaluate(t.Y, row)

		xI, yI := coerceToInt(x), coerceToInt(y)

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
			// FIXME: This should be an error
			if xI == nil || yI == nil {
				return reflect.ValueOf(false)
			}
			return reflect.ValueOf(*xI > *yI)
		case sql.GE:
			// FIXME: This should be an error
			if xI == nil || yI == nil {
				return reflect.ValueOf(false)
			}
			return reflect.ValueOf(*xI >= *yI)
		case sql.LT:
			// FIXME: This should be an error
			if xI == nil || yI == nil {
				return reflect.ValueOf(false)
			}
			return reflect.ValueOf(*xI < *yI)
		case sql.LE:
			// FIXME: This should be an error
			if xI == nil || yI == nil {
				return reflect.ValueOf(false)
			}
			return reflect.ValueOf(*xI <= *yI)
		case sql.LIKE:
			match := strings.ReplaceAll(y.String(), "%", ".*")
			matched, err := regexp.MatchString(match, x.String())
			// FIXME: This should be an error
			if err != nil {
				return reflect.ValueOf(false)
			}
			return reflect.ValueOf(matched)
		case sql.NOTLIKE:
			match := strings.ReplaceAll(y.String(), "%", ".*")
			matched, err := regexp.MatchString(match, x.String())
			// FIXME: This should be an error
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
		return reflect.ValueOf(int64(i))
	case *sql.BoolLit:
		return reflect.ValueOf(t.Value)
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
			if ref == column || i.Aliases[lh]+"."+rh == column {
				if row[idx].Value.Kind() == reflect.Bool {
					if i := coerceToInt(row[idx].Value); i != nil {
						return reflect.ValueOf(*i)
					}
				}

				return row[idx].Value
			}
		}

		return reflect.ValueOf(nil)

	case *sql.Ident:
		for idx, column := range i.Columns {
			if t.Name == column || (i.Source != nil && i.Source.Name+"."+column == t.Name) {
				if row[idx].Value.Kind() == reflect.Bool {
					if i := coerceToInt(row[idx].Value); i != nil {
						return reflect.ValueOf(*i)
					}
				}

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

		xI, yI := coerceToInt(x), coerceToInt(y)

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
			// FIXME: This should be an error
			if xI == nil || yI == nil {
				return reflect.ValueOf(false)
			}
			return reflect.ValueOf(*xI > *yI)
		case sql.GE:
			// FIXME: This should be an error
			if xI == nil || yI == nil {
				return reflect.ValueOf(false)
			}
			return reflect.ValueOf(*xI >= *yI)
		case sql.LT:
			// FIXME: This should be an error
			if xI == nil || yI == nil {
				return reflect.ValueOf(false)
			}
			return reflect.ValueOf(*xI < *yI)
		case sql.LE:
			// FIXME: This should be an error
			if xI == nil || yI == nil {
				return reflect.ValueOf(false)
			}
			return reflect.ValueOf(*xI <= *yI)
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
