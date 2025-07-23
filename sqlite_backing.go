package duckql

import (
	gosql "database/sql"
	"reflect"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rqlite/sql"
)

type SQLiteBacking struct {
	sqlizer   *SQLizer
	db        *gosql.DB
	lastError error
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
	return s, n, nil
}

// VisitEnd implements sql.Visitor
func (s *SQLiteBacking) VisitEnd(n sql.Node) (sql.Node, error) {
	return n, nil
}

// Rows implements duckql.BackingStore
func (s *SQLiteBacking) Rows() ResultRows {
	var results ResultRows
	if s.sqlizer.RawStatement != "" {
		rows, err := s.db.Query(s.sqlizer.RawStatement)
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
