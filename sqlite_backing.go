package duckql

import (
	gosql "database/sql"
	"reflect"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rqlite/sql"
)

type SQLiteBacking struct {
	sqlizer      *SQLizer
	db           *gosql.DB
	lastError    error
	rawStatement string
}

// New creates a new SQLiteBacking with the given SQLite database connection
func NewSQLiteBacking(db *gosql.DB, s *SQLizer) *SQLiteBacking {
	s.HandleAggregateFunctions = false
	return &SQLiteBacking{
		sqlizer: s,
		db:      db,
	}
}

// Visit implements sql.Visitor
func (s *SQLiteBacking) Visit(n sql.Node) (sql.Visitor, sql.Node, error) {
	switch t := n.(type) {
	case *sql.InsertStatement, *sql.DeleteStatement, *sql.UpdateStatement:
		s.rawStatement = t.String()
	case *sql.SelectStatement:
		// Rewrite the AST to expand '*'
		// This allows intentionally hidden fields to stay hidden

		var rewritten []*sql.ResultColumn
		for _, column := range t.Columns {
			if column.Star.Line > 0 {
				src, err := strconv.Unquote(t.Source.String())
				if err != nil {
					continue
				}

				source, ok := s.sqlizer.Tables[src]
				if !ok {
					continue
				}

				for _, sourceColumn := range source.Columns {
					c := column.Clone()
					c.Star = sql.Pos{}
					c.Expr = &sql.Ident{
						Name:   sourceColumn,
						Quoted: false,
					}
					rewritten = append(rewritten, c)
				}

			} else {
				rewritten = append(rewritten, column)
			}
		}

		t.Columns = rewritten

		s.rawStatement = t.String()

		return s, t, nil
	}

	return s, n, nil
}

// VisitEnd implements sql.Visitor
func (s *SQLiteBacking) VisitEnd(n sql.Node) (sql.Node, error) {
	return n, nil
}

// Rows implements duckql.BackingStore
func (s *SQLiteBacking) Rows() ResultRows {
	var results ResultRows
	if s.rawStatement != "" {
		rows, err := s.db.Query(s.rawStatement)
		if err != nil {
			s.lastError = err
			return nil
		}
		defer rows.Close()

		// Get column names
		columns, err := rows.Columns()
		if err != nil {
			s.lastError = err
			return nil
		}

		// Prepare values holder
		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		// Scan rows
		for rows.Next() {
			err = rows.Scan(scanArgs...)
			if err != nil {
				s.lastError = err
				return nil
			}

			resultRow := make(ResultRow, len(columns))
			for i, col := range columns {
				var val reflect.Value
				if values[i] == nil {
					val = reflect.Zero(reflect.TypeOf(&gosql.NullString{}))
				} else {
					val = reflect.ValueOf(values[i])
				}
				resultRow[i] = ResultValue{
					Name:  col,
					Value: val,
				}
			}
			results = append(results, resultRow)
		}

		if err = rows.Err(); err != nil {
			s.lastError = err
			return nil
		}
	}
	return results
}

// Error returns the last error encountered during query execution
func (s *SQLiteBacking) Error() error {
	return s.lastError
}
