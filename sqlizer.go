package ddllm

import (
	"github.com/rqlite/sql"
	"reflect"
	"strings"
	"unicode"
)

type BackingStore interface {
	sql.Visitor
	Result() []any
}

type Table struct {
	Name               string
	StructName         string
	Columns            []string
	ColumnStructFields map[string]string
	ColumnTypes        map[string]string
	ColumnComments     map[string]string
}

const (
	AllowSelectStatements = 1 << iota
	AllowInsertStatements
	AllowUpdateStatements
	AllowDeleteStatements
)

type SQLizer struct {
	Tables      map[string]*Table
	Permissions uint
	Backing     BackingStore
}

func (s *SQLizer) SetPermissions(permissions uint) {
	s.Permissions = permissions
}

func (s *SQLizer) SetBacking(backing BackingStore) {
	s.Backing = backing
}

func (s *SQLizer) Execute(statement string) ([]any, error) {
	parser := sql.NewParser(strings.NewReader(statement))
	stmt, err := parser.ParseStatement()
	if err != nil {
		return nil, err
	}

	v := &Validator{s: s}
	n, err := sql.Walk(v, stmt)
	if err != nil {
		return nil, err
	}

	if s.Backing != nil {
		_, err = sql.Walk(s.Backing, n)
		if err != nil {
			return nil, err
		}

		return s.Backing.Result(), nil
	}

	return nil, nil
}

func Initialize(structs ...any) *SQLizer {
	var sql SQLizer
	sql.Tables = make(map[string]*Table)

	for _, s := range structs {
		var table Table

		table.ColumnStructFields = make(map[string]string)
		table.ColumnTypes = make(map[string]string)
		table.ColumnComments = make(map[string]string)

		t := reflect.TypeOf(s)

		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}

		table.Name = toSnakeCase(pluralize(t.Name()))
		table.StructName = t.Name()

		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)

			columnName := toSnakeCase(field.Name)
			columnType := SQLiteTypeForType(field.Type)
			columnComment := ""

			if tag := field.Tag.Get("ddl"); tag != "" {
				parsed := parseTagValue(tag)

				if _, ok := parsed["omit"]; ok {
					continue
				}

				if _, ok := parsed["primary"]; ok {
					columnType += " primary key autoincrement"
				}

				if c, ok := parsed["comment"]; ok {
					columnComment = c
				}
			}

			table.Columns = append(table.Columns, columnName)
			table.ColumnStructFields[columnName] = field.Name
			table.ColumnTypes[columnName] = columnType
			table.ColumnComments[columnName] = columnComment
		}

		sql.Tables[table.Name] = &table
	}

	return &sql
}

func toSnakeCase(s string) string {
	var snakeCase string

	if len(s) <= 2 {
		return strings.ToLower(s)
	}

	for idx, r := range s {
		if unicode.IsUpper(r) {
			if idx > 0 {
				snakeCase += "_"
			}
		}

		snakeCase += string(unicode.ToLower(r))
	}

	return snakeCase
}

func pluralize(s string) string {
	if strings.HasSuffix(s, "s") {
		return s
	}

	return s + "s"
}

func parseTagValue(s string) map[string]string {
	parsed := make(map[string]string)
	settings := strings.Split(s, ",")
	for _, setting := range settings {
		if setting == "-" {
			parsed["omit"] = ""
			return parsed
		}

		parts := strings.Split(setting, "=")

		if len(parts) != 2 {
			parsed[setting] = ""
			continue
		}

		k, v := parts[0], parts[1]

		if v[0] == '\'' && v[len(v)-1] == '\'' {
			v = v[1 : len(v)-1]
		}

		parsed[k] = v
	}

	return parsed
}

func SQLiteTypeForType(t reflect.Type) string {
	switch t.Kind() {
	case reflect.String:
		return "TEXT"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Bool:
		return "INTEGER"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "INTEGER"
	case reflect.Float32, reflect.Float64:
		return "REAL"
	}

	return t.String()
}
