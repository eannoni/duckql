package duckql

import (
	"errors"
	"strconv"

	"github.com/rqlite/sql"
)

type Validator struct {
	s       *SQLizer
	columns []string
}

func (v *Validator) Visit(n sql.Node) (sql.Visitor, sql.Node, error) {
	switch t := n.(type) {
	case *sql.SelectStatement:
		if v.s.Permissions&AllowSelectStatements == 0 {
			return nil, nil, errors.New("duckql: SelectStatements are not allowed")
		}
	case *sql.InsertStatement:
		if v.s.Permissions&AllowInsertStatements == 0 {
			return nil, nil, errors.New("duckql: InsertStatements are not allowed")
		}
	case *sql.UpdateStatement:
		if v.s.Permissions&AllowUpdateStatements == 0 {
			return nil, nil, errors.New("duckql: UpdateStatements are not allowed")
		}
	case *sql.DeleteStatement:
		if v.s.Permissions&AllowDeleteStatements == 0 {
			return nil, nil, errors.New("duckql: DeleteStatements are not allowed")
		}
	case *sql.QualifiedTableName:
		var table *Table
		var ok bool

		if table, ok = v.s.Tables[t.TableName()]; !ok || table == nil {
			return nil, nil, errors.New("duckql: Unknown table '" + t.TableName() + "'")
		}

		// Validate all columns
		for _, column := range v.columns {
			if column == "*" {
				continue
			}

			if _, ok = table.ColumnMappings[column]; !ok {
				return nil, nil, errors.New("duckql: Unknown column '" + column + "' for table '" + t.TableName() + "'")
			}
		}
	case *sql.ResultColumn:
		s := t.String()
		if x, err := strconv.Unquote(s); err == nil {
			s = x
		}

		f := ParseAggregateFunction(s)
		if f != nil {
			v.s.AggregateFunctions = append(v.s.AggregateFunctions, f)
			s = f.UnderlyingColumn
			n = &sql.ResultColumn{
				Expr: &sql.StringLit{
					Value: s,
				},
			}
		}

		v.columns = append(v.columns, s)
	}

	return v, n, nil
}

func (v *Validator) VisitEnd(n sql.Node) (sql.Node, error) {
	return n, nil
}
