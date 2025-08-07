package duckql

import (
	"fmt"
	"github.com/rqlite/sql"
	"google.golang.org/api/sheets/v4"
	"log"
	"reflect"
	"strconv"
	"unicode/utf8"
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
			columnName := intermediate.Source.ColumnMappings[column].Tag.Get("sheets")

			// Compute index
			colRune, _ := utf8.DecodeRuneInString(columnName)
			startRune, _ := utf8.DecodeRuneInString(colStart)
			index := colRune - startRune

			var cellValue reflect.Value
			if int(index) < len(row) {
				cellValue = reflect.ValueOf(row[index])
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
