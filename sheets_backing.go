package duckql

import (
	"fmt"
	"github.com/rqlite/sql"
	"google.golang.org/api/sheets/v4"
	"log"
	"reflect"
	"regexp"
	"strconv"
	"time"
)

type SheetsOptions struct {
	Service      *sheets.Service
	SheetId      string
	SheetName    *string
	IDColumn     string
	DataRowStart int
}

type SheetsBacking struct {
	s       *SQLizer
	exec    *QueryExecutor
	options *SheetsOptions
}

func (s *SheetsBacking) Visit(n sql.Node) (sql.Visitor, sql.Node, error) {
	if s.exec == nil {
		s.exec = NewQueryExecutor(s.s, s.FillIntermediate)
	}

	return s.exec.Visit(n)
}

func (s *SheetsBacking) VisitEnd(n sql.Node) (sql.Node, error) {
	return n, nil
}

func (s *SheetsBacking) Rows() ResultRows {
	return s.exec.Rows()
}

func (s *SheetsBacking) getNonEmptyRowCount() (int, error) {
	readRange := fmt.Sprintf("%s%d:%s", s.options.IDColumn, s.options.DataRowStart, s.options.IDColumn)
	resp, err := s.options.Service.Spreadsheets.Values.Get(s.options.SheetId, readRange).Do()
	if err != nil {
		return 0, err
	}

	return len(resp.Values), nil
}

func (s *SheetsBacking) ComputeRangeString(colStart string, rowStart int, colEnd string, endRow int) string {
	rangeStr := ""
	if s.options.SheetName != nil {
		rangeStr += *s.options.SheetName + "!"
	}

	rowStartStr := strconv.FormatInt(int64(rowStart), 10)
	endRowStr := strconv.FormatInt(int64(endRow), 10)

	rangeStr += colStart + rowStartStr + ":" + colEnd + endRowStr

	return rangeStr
}

func coerceSpreadsheetValue(s string, t reflect.Type) reflect.Value {
	switch t.Kind() {
	case reflect.String:
		return reflect.ValueOf(s)
	case reflect.Bool:
		return reflect.ValueOf(s == "TRUE")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return reflect.ValueOf(s)
		}
		return reflect.ValueOf(i)
	case reflect.Float32, reflect.Float64:
		clean := regexp.MustCompile(`[^0-9.\-]`).ReplaceAllString(s, "")
		f, err := strconv.ParseFloat(clean, 64)
		if err != nil {
			return reflect.ValueOf(s)
		}
		return reflect.ValueOf(f)
	case reflect.Struct:
		if t.Name() == "Time" {
			t, err := time.Parse("1/2/2006 15:04:05", s)
			if err != nil {
				t, err = time.Parse("1/2/2006", s)
				if err != nil {
					t, err = time.Parse(time.RFC3339, s)
				}
			}
			return reflect.ValueOf(t)
		}
		return reflect.ValueOf(s)
	default:
		return reflect.ValueOf(s)
	}
}

// SheetColumnToIndex converts a Google Sheets column name into its index ('A' -> 0, 'AA' -> 26)
func SheetColumnToIndex(col string) int {
	index := 0
	for i := 0; i < len(col); i++ {
		c := col[i]
		index = index*26 + int(c-'A'+1)
	}
	return index - 1
}

func (s *SheetsBacking) FillIntermediate(intermediate *IntermediateTable) {
	if intermediate == nil {
		panic("no intermediate table")
	}
	if intermediate.Source == nil {
		panic("cannot fill intermediate without a table")
	}

	colStart := ""
	colEnd := ""
	for _, column := range intermediate.Source.ColumnMappings {
		col := column.Tag.Get("sheets")

		if colStart == "" || col < colStart {
			colStart = col
		}
		if colEnd == "" || col > colEnd {
			colEnd = col
		}
	}

	numRows, err := s.getNonEmptyRowCount()
	if err != nil {
		panic(err)
	}

	if numRows == 0 {
		return
	}

	rowStart := s.options.DataRowStart
	rowEnd := rowStart + numRows - 1

	readRange := s.ComputeRangeString(colStart, rowStart, colEnd, rowEnd)
	colStartIndex := SheetColumnToIndex(colStart)

	resp, err := s.options.Service.Spreadsheets.Values.Get(s.options.SheetId, readRange).Do()
	if err != nil {
		log.Fatalf("Unable to retrieve data from sheet: %v", err)
	}

	for _, column := range intermediate.Source.Columns {
		intermediate.Columns = append(intermediate.Columns, column)
	}

	for _, row := range resp.Values {
		var result ResultRow

		for _, column := range intermediate.Columns {
			mapping := intermediate.Source.ColumnMappings[column]
			columnName := mapping.Tag.Get("sheets")
			absoluteIndex := SheetColumnToIndex(columnName)

			index := absoluteIndex - colStartIndex

			var cellValue reflect.Value
			if index < len(row) {
				cellValue = coerceSpreadsheetValue(row[index].(string), mapping.Type)
			} else {
				cellValue = reflect.ValueOf("")
			}

			result = append(result, ResultValue{
				Name:  column,
				Value: cellValue,
			})
		}

		intermediate.Rows = append(intermediate.Rows, result)
	}
}

func NewSheetsBacking(s *SQLizer, options *SheetsOptions) *SheetsBacking {
	return &SheetsBacking{
		s:       s,
		options: options,
	}
}
