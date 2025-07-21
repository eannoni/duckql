package main

import (
	"errors"
	"fmt"
	"github.com/rqlite/sql"
	"reflect"
	"strings"
	"unicode"
)

type Person struct {
	ID        int `ddl:"primary"`
	FirstName string
	LastName  string
	Email     string `ddl:"comment='Not validated'"`
	Age       int
	Internal  bool `ddl:"-"`
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
}

func (s *SQLizer) SetPermissions(permissions uint) {
	s.Permissions = permissions
}

func (s *SQLizer) Visit(n sql.Node) (sql.Visitor, sql.Node, error) {
	switch t := n.(type) {
	case *sql.SelectStatement:
		if s.Permissions&AllowSelectStatements == 0 {
			return nil, nil, errors.New("ddllm: SelectStatements are not allowed")
		}
	case *sql.InsertStatement:
		if s.Permissions&AllowInsertStatements == 0 {
			return nil, nil, errors.New("ddllm: InsertStatements are not allowed")
		}
	case *sql.UpdateStatement:
		if s.Permissions&AllowUpdateStatements == 0 {
			return nil, nil, errors.New("ddllm: UpdateStatements are not allowed")
		}
	case *sql.DeleteStatement:
		if s.Permissions&AllowDeleteStatements == 0 {
			return nil, nil, errors.New("ddllm: DeleteStatements are not allowed")
		}
	case *sql.QualifiedTableName:
		if table, ok := s.Tables[t.TableName()]; !ok || table == nil {
			return nil, nil, errors.New("ddllm: Unknown table '" + t.TableName() + "'")
		}
	}

	return s, n, nil
}

func (s *SQLizer) VisitEnd(n sql.Node) (sql.Node, error) {
	return n, nil
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

func main() {
	s := Initialize(&Person{})
	s.SetPermissions(AllowSelectStatements)

	fmt.Println(s)

	parser := sql.NewParser(strings.NewReader("SELECT * FROM persons WHERE email like '%@aol.com'"))
	stmt, err := parser.ParseStatement()
	if err != nil {
		panic(err)
	}

	_, err = sql.Walk(s, stmt)
	if err != nil {
		panic(err)
	}
}

//func main() {
//	data := Person{
//		FirstName: "John",
//		LastName:  "Smith",
//		Email:     "john@aol.com",
//		Age:       21,
//	}
//
//	t := reflect.TypeOf(data)
//
//	ddl := "create table " + toSnakeCase(pluralize(t.Name())) + "\n(\n"
//
//	for i := 0; i < t.NumField(); i++ {
//		field := t.Field(i)
//
//		var row string
//
//		row += toSnakeCase(field.Name) + " " + SQLiteTypeForType(field.Type)
//
//		var additional []string
//		needsComma := true
//
//		if s := field.Tag.Get("ddl"); s != "" {
//			parsed := parseTagValue(s)
//
//			if _, ok := parsed["omit"]; ok {
//				continue
//			}
//
//			if _, ok := parsed["primary"]; ok {
//				needsComma = false
//				additional = append([]string{"primary key autoincrement,"}, additional...)
//			}
//
//			if c, ok := parsed["comment"]; ok {
//				additional = append(additional, "-- "+c)
//			}
//		}
//
//		if needsComma {
//			row += ", "
//		} else {
//			row += " "
//		}
//
//		row += strings.Join(additional, " ")
//
//		row += "\n"
//		ddl += row
//	}
//
//	ddl += ");"
//
//	fmt.Println(ddl)
//}
