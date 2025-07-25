package duckql

import (
	"github.com/rqlite/sql"
	"strings"
)

type QueryExecutor struct {
	FillIntermediate func(table *IntermediateTable)

	s             *SQLizer
	intermediate  IntermediateVisitor
	tables        map[string]*Table
	filter        sql.Node
	resultColumns []*sql.ResultColumn
}

func (q *QueryExecutor) Visit(n sql.Node) (sql.Visitor, sql.Node, error) {
	if q.tables == nil {
		q.tables = make(map[string]*Table)
	}

	switch t := n.(type) {
	case *sql.QualifiedTableName:
		var qt QualifiedTableVisitor
		qt.F = q

		q.intermediate = &qt

		return qt.Visit(n)

	case *sql.JoinClause:
		var jv JoinVisitor
		jv.F = q

		q.intermediate = &jv

		return jv.Visit(n)

	case *sql.ResultColumn:
		q.resultColumns = append(q.resultColumns, t)

	}

	return q, n, nil
}

func (q *QueryExecutor) VisitEnd(n sql.Node) (sql.Node, error) {
	switch t := n.(type) {
	case *sql.UnaryExpr, *sql.BinaryExpr:
		q.filter = t
	}
	return n, nil
}

func (q *QueryExecutor) Rows() ResultRows {
	var r ResultRows

	source := q.intermediate.Result()
	source = source.Filter(q.filter)

	if len(source.Rows) == 0 {
		return r
	}

	// Transform our intermediate columns into a lookup table
	lookup := make(map[string]int)
	for idx, column := range source.Columns {
		lookup[column] = idx
	}

	// Find column positions to narrow
	var narrowColumns []int
	for _, column := range q.resultColumns {
		if column.Star.Line > 0 {
			for idx, _ := range source.Rows[0] {
				narrowColumns = append(narrowColumns, idx)
			}
			break
		}

		switch t := column.Expr.(type) {
		case *sql.Ident:
			index, ok := lookup[t.Name]
			if !ok {
				// FIXME: There should be a better way
				parts := strings.Split(source.Columns[0], ".")
				if len(parts) > 1 {
					index = lookup[parts[0]+"."+t.Name]
				}
			}

			narrowColumns = append(narrowColumns, index)
		case *sql.QualifiedRef:
			if t.Star.Line != 0 {
				// FIXME: Implement
				continue
			}

			lh := t.Table.Name
			rh := t.Column.Name

			var index int
			var ok bool
			if source.Source != nil {
				index, ok = lookup[rh]
			} else {
				index, ok = lookup[lh+"."+rh]
				if !ok {
					index = lookup[source.Aliases[lh]+"."+rh]
				}
			}

			narrowColumns = append(narrowColumns, index)
		default:
			narrowColumns = append(narrowColumns, 0)
		}
	}

	// Find aggregations
	var aggregations []AggregateFunctionColumn
	for idx, column := range q.resultColumns {
		switch t := column.Expr.(type) {
		case *sql.Call:
			var underlying string
			if t.Star.Line == 0 {
				if len(t.Args) != 1 {
					panic("unexpected number of args to function")
				}

				switch arg := t.Args[0].(type) {
				case *sql.Ident:
					// Validate?
					index, ok := lookup[arg.Name]
					if ok {
						underlying = arg.Name
						break
					}

					// FIXME: There should be a better way
					parts := strings.Split(source.Columns[0], ".")
					if len(parts) > 1 {
						index, ok = lookup[parts[0]+"."+arg.Name]
						if ok {
							underlying = parts[0] + "." + arg.Name
						}
					}

					narrowColumns[idx] = index
				}
			} else {
				narrowColumns[idx] = 0
			}

			aggregations = append(aggregations, AggregateFunctionColumn{
				UnderlyingColumn: underlying,
				ResultPosition:   idx,
				Function:         functionMap[t.Name.Name],
			})
		}
	}

	for _, row := range source.Rows {
		if len(row) == len(narrowColumns) {
			r = append(r, row)
			continue
		}

		var newRow ResultRow
		for _, column := range narrowColumns {
			newRow = append(newRow, row[column])
		}
		r = append(r, newRow)
	}

	for _, aggregation := range aggregations {
		r = aggregation.Call(r)
	}

	q.resultColumns = []*sql.ResultColumn{}

	return r
}

func NewQueryExecutor(s *SQLizer, f func(table *IntermediateTable)) *QueryExecutor {
	return &QueryExecutor{
		FillIntermediate: f,
		s:                s,
	}
}

type IntermediateVisitor interface {
	sql.Visitor
	Result() *IntermediateTable
}

type QualifiedTableVisitor struct {
	F     *QueryExecutor
	Table *IntermediateTable
}

func (i *QualifiedTableVisitor) Visit(n sql.Node) (sql.Visitor, sql.Node, error) {
	switch t := n.(type) {
	case *sql.QualifiedTableName:
		i.Table = NewIntermediateTable()
		i.Table.Source = i.F.s.Tables[t.TableName()]
		i.F.FillIntermediate(i.Table)

		if t.Alias != nil {
			i.Table.Aliases[t.Alias.Name] = t.Name.Name
		}
	}
	return i, n, nil
}

func (i *QualifiedTableVisitor) VisitEnd(n sql.Node) (sql.Node, error) {
	return n, nil
}

func (i *QualifiedTableVisitor) Result() *IntermediateTable {
	return i.Table
}

type JoinVisitor struct {
	F          *QueryExecutor
	JoinResult *IntermediateTable
	Sources    []*IntermediateTable
	Previous   IntermediateVisitor
}

func (j *JoinVisitor) Result() *IntermediateTable {
	return j.JoinResult
}

func (j *JoinVisitor) joinRowOn(row ResultRow, expr sql.Expr) ResultRows {
	from := j.Sources[len(j.Sources)-2]
	to := j.Sources[len(j.Sources)-1]

	var results ResultRows

	switch t := expr.(type) {
	case *sql.BinaryExpr:
		var a, b string
		var swapped bool

		op := t.Op

		for idx, x := range []sql.Expr{t.X, t.Y} {
			switch n := x.(type) {
			case *sql.Ident:
				if from.Source == nil {
					return nil
				}

				if _, ok := from.Source.ColumnMappings[n.Name]; ok {
					if a == "" {
						a = n.Name
					} else {
						b = n.Name
					}
				}
			case *sql.QualifiedRef:
				var table *IntermediateTable
				var tableName string
				var columnName string

				if n.Star.Line > 0 {
					columnName = "*"
				}

				if columnName == "" {
					columnName = n.Column.Name
				}

				if from.Source != nil && n.Table != nil {
					if from.Source.Name == n.Table.Name {
						tableName = n.Table.Name
						table = from
					} else if _, ok := from.Aliases[n.Table.Name]; ok && from.Aliases[n.Table.Name] == from.Source.Name {
						tableName = from.Aliases[n.Table.Name]
						table = from
					}

					if table != nil {
						if a != "" {
							b = a
						}
						a = tableName + "." + columnName

						if idx == 1 {
							swapped = true
						}
					}
				}

				if table == nil && to.Source != nil && n.Table != nil {
					if to.Source.Name == n.Table.Name {
						tableName = n.Table.Name
						table = to
					} else if _, ok := to.Aliases[n.Table.Name]; ok && to.Aliases[n.Table.Name] == to.Source.Name {
						tableName = to.Aliases[n.Table.Name]
						table = to
					}

					if table != nil {
						if b != "" {
							a = b
						}
						b = tableName + "." + columnName
						if idx == 0 {
							swapped = true
						}
					}
				}

				if table == nil {
					return nil
				}
			}
		}

		if swapped {
			switch op {
			case sql.LT:
				op = sql.GE
			case sql.GT:
				op = sql.LE
			case sql.GE:
				op = sql.LT
			case sql.LE:
				op = sql.GT
			case sql.EQ:
				// DO nothing
			default:
				panic("unhandled default case")
			}
		}

		var fromValue ResultValue
		for _, value := range row {
			if value.Name == a || from.Source.Name+"."+value.Name == a {
				fromValue = value
				break
			}
		}

		if fromValue.Name == "" {
			return nil
		}

		for _, targetRow := range to.Rows {
			index := -1

			for idx, value := range targetRow {
				if value.Name == b || to.Source.Name+"."+value.Name == b {
					// FIXME: Handle other operators
					if fromValue.Value.Equal(value.Value) {
						index = idx
					}
					break
				}
			}

			if index > -1 {
				var r ResultRow

				for _, c := range row {
					r = append(r, ResultValue{
						Name:  from.Source.Name + "." + c.Name,
						Value: c.Value,
					})
				}

				for _, c := range targetRow {
					r = append(r, ResultValue{
						Name:  to.Source.Name + "." + c.Name,
						Value: c.Value,
					})
				}

				results = append(results, r)
			}
		}
	}

	return results
}

func (j *JoinVisitor) Visit(n sql.Node) (sql.Visitor, sql.Node, error) {
	switch t := n.(type) {
	case *sql.QualifiedTableName:
		if j.Previous != nil {
			j.Sources = append(j.Sources, j.Previous.Result())
		}

		j.Previous = &QualifiedTableVisitor{F: j.F}
		return j.Previous.Visit(n)
	case *sql.OnConstraint:
		if j.Previous != nil && j.Sources[len(j.Sources)-1] != j.Previous.Result() {
			j.Sources = append(j.Sources, j.Previous.Result())
		}

		j.JoinResult = &IntermediateTable{}
		j.JoinResult.Aliases = j.Sources[len(j.Sources)-2].Aliases

		for k, v := range j.Sources[len(j.Sources)-1].Aliases {
			j.JoinResult.Aliases[k] = v
		}

		for _, row := range j.Sources[len(j.Sources)-2].Rows {
			rows := j.joinRowOn(row, t.X)
			if len(j.JoinResult.Columns) == 0 && len(rows) > 0 {
				for _, column := range rows[0] {
					j.JoinResult.Columns = append(j.JoinResult.Columns, column.Name)
				}
			}
			j.JoinResult.Rows = append(j.JoinResult.Rows, rows...)
		}
	}

	return j, n, nil
}

func (j *JoinVisitor) VisitEnd(n sql.Node) (sql.Node, error) {
	switch n.(type) {
	case *sql.JoinClause:
	}
	return n, nil
}
