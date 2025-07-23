package duckql

import (
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/rqlite/sql"
)

type BackingStore interface {
	sql.Visitor
	Rows() ResultRows
}

type ColumnMapping struct {
	Name       string
	GoField    string
	SQLType    string
	SQLComment string
}

type Table struct {
	Name           string
	StructName     string
	Columns        []string
	ColumnMappings map[string]ColumnMapping
	ForeignKeys    map[string]*Table
}

const (
	AllowSelectStatements = 1 << iota
	AllowInsertStatements
	AllowUpdateStatements
	AllowDeleteStatements
)

type SQLizer struct {
	Tables                   map[string]*Table
	Permissions              uint
	Backing                  BackingStore
	AggregateFunctions       []*AggregateFunctionColumn
	HandleAggregateFunctions bool
}

func (s *SQLizer) SetPermissions(permissions uint) {
	s.Permissions = permissions
}

func (s *SQLizer) SetBacking(backing BackingStore) {
	s.Backing = backing
}

func (s *SQLizer) Execute(statement string) (ResultRows, error) {
	// Support a small subset of dot commands
	switch statement {
	case ".schema":
		return ResultRows{
			ResultRow{
				ResultValue{
					Name:  ".schema",
					Value: reflect.ValueOf(s.DDL()),
				},
			},
		}, nil
	}

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

		rows := s.Backing.Rows()

		if len(rows) > 0 && len(s.AggregateFunctions) > 0 && s.HandleAggregateFunctions {
			var result ResultRows
			for idx, aggregate := range s.AggregateFunctions {
				r := aggregate.Call(rows)
				if idx == 0 {
					result = append(result, r[0])
				} else {
					result[0][aggregate.ResultPosition] = r[0][aggregate.ResultPosition]
				}
			}
			rows = result
		}

		return rows, nil
	}

	return nil, nil
}

func (s *SQLizer) TypeForData(data any) reflect.Type {
	t := reflect.TypeOf(data)

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return t
}

func (s *SQLizer) TableForData(data any) *Table {
	t := s.TypeForData(data)

	if x, ok := s.Tables[toSnakeCase(pluralize(t.Name()))]; ok {
		return x
	}

	return nil
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

func (s *SQLizer) addStructTable(str any) {
	var table Table

	table.ColumnMappings = make(map[string]ColumnMapping)
	table.ForeignKeys = make(map[string]*Table)

	var t reflect.Type
	if _, ok := str.(reflect.Type); !ok {
		t = reflect.TypeOf(str)

		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
	} else {
		t = str.(reflect.Type)
	}

	table.Name = toSnakeCase(pluralize(t.Name()))
	table.StructName = t.Name()

	if x, ok := s.Tables[table.Name]; ok {
		// Check for any foreign keys
		for name, mapping := range x.ColumnMappings {
			if !strings.HasSuffix(name, "_id") {
				continue
			}

			possibleTableName := pluralize(name[:len(name)-3])
			if foreign, ok := s.Tables[possibleTableName]; ok {
				// Ensure that the type matches up
				if strings.HasPrefix(foreign.ColumnMappings["id"].SQLType, mapping.SQLType) {
					x.ForeignKeys[name] = foreign
				}
			}
		}
		return
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		columnName := toSnakeCase(field.Name)
		columnType := sqliteTypeForGoType(field.Type)
		columnComment := ""

		if columnType == "unknown" {
			if field.Type.Kind() == reflect.Slice && field.Type.Elem().Kind() == reflect.Struct {
				s.addStructTable(field.Type.Elem())
			}
			continue
		}

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
		table.ColumnMappings[columnName] = ColumnMapping{
			Name:       columnName,
			GoField:    field.Name,
			SQLType:    columnType,
			SQLComment: columnComment,
		}
	}

	s.Tables[table.Name] = &table
}

func Initialize(structs ...any) *SQLizer {
	var sql SQLizer
	sql.Tables = make(map[string]*Table)

	for _, s := range structs {
		sql.addStructTable(s)
	}

	sql.HandleAggregateFunctions = true

	return &sql
}

func (s *SQLizer) DDL() string {
	var sql strings.Builder

	for _, v := range s.Tables {
		sql.WriteString("CREATE TABLE ")
		sql.WriteString(v.Name)
		sql.WriteString("\n(\n")

		for idx, column := range v.Columns {
			mapping := v.ColumnMappings[column]

			sql.WriteString("  " + column + " " + mapping.SQLType)

			if idx < len(v.Columns)-1 {
				sql.WriteString(",")
			}

			if mapping.SQLComment != "" {
				sql.WriteString("  -- " + mapping.SQLComment)
			}

			sql.WriteString("\n")
		}

		for key, table := range v.ForeignKeys {
			sql.WriteString("  FOREIGN KEY (" + key + ") REFERENCES " + table.Name + "(id)\n")
		}

		sql.WriteString(")\n\n")
	}

	return sql.String()
}

func toSnakeCase(s string) string {
	var snakeCase string

	if len(s) <= 2 {
		return strings.ToLower(s)
	}

	for idx, r := range s {
		if unicode.IsUpper(r) {
			if idx > 0 && (idx-2 < 0 || snakeCase[len(snakeCase)-2] != '_') {
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

func sqliteTypeForGoType(t reflect.Type) string {
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

	return "unknown"
}
