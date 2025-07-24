package duckql

import (
	"errors"
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
		if t.Alias != nil {
			if table, ok := v.s.Tables[t.Name.Name]; ok {
				v.s.Tables[t.Alias.Name] = table
			} else {
				return nil, nil, errors.New("duckql: table not found: " + t.Name.Name)
			}
		}

		if table, ok := v.s.Tables[t.TableName()]; !ok || table == nil {
			return nil, nil, errors.New("duckql: Unknown table '" + t.TableName() + "'")
		}

	case *sql.ResultColumn:

		switch e := t.Expr.(type) {
		case *sql.Ident:
			v.columns = append(v.columns, e.Name)
		case *sql.Call:
			var aggregate AggregateFunctionColumn
			var star sql.Pos

			// FIXME
			if len(e.Args) > 0 {
				aggregate.UnderlyingColumn = e.Args[0].(*sql.Ident).Name
			} else {
				aggregate.UnderlyingColumn = "*"
				star.Line = 1
			}

			aggregate.ResultPosition = len(v.columns)
			aggregate.Function = functionMap[e.Name.Name]

			v.s.AggregateFunctions = append(v.s.AggregateFunctions, &aggregate)
			// FIXME
			if v.s.HandleAggregateFunctions {
				n = &sql.ResultColumn{
					Star: star,
					Expr: &sql.Ident{
						Name:   aggregate.UnderlyingColumn,
						Quoted: false,
					},
				}
			}
			v.columns = append(v.columns, aggregate.UnderlyingColumn)
		}
	}

	return v, n, nil
}

func (v *Validator) VisitEnd(n sql.Node) (sql.Node, error) {
	switch t := n.(type) {
	case *sql.SelectStatement:
		source := t.Source

		sourceTable, ok := source.(*sql.QualifiedTableName)
		//sourceJoin, ok := t.Source.(*sql.JoinClause)

		for _, column := range t.Columns {
			switch e := column.Expr.(type) {
			case *sql.Ident:
				if column.Star.Line > 0 {
					continue
				}

				if sourceTable != nil {
					table := v.s.Tables[sourceTable.TableName()]

					if _, ok = table.ColumnMappings[e.Name]; !ok {
						return nil, errors.New("duckql: Unknown column '" + e.Name + "' for table '" + sourceTable.TableName() + "'")
					}
				}

			case *sql.QualifiedRef:

			}
		}

	}

	return n, nil
}
