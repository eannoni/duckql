package test

import (
	"fmt"
	"github.com/dburkart/duckql"
	"github.com/dburkart/duckql/test/types"
	"os"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	KEY int = iota
	EQUALS
	IDENT
	DIVIDER
	TEXT
)

type token struct {
	Type  int
	Start int
	End   int
	Text  *string
}

func (t *token) Lexeme() string {
	return (*t.Text)[t.Start : t.Start+t.End]
}

type scanner struct {
	input      string
	pos        int
	prev       int
	inText     bool
	prevInText bool
}

func (s *scanner) matchDivider(start int) bool {
	if len(s.input) >= start+3 {
		if s.input[start:start+3] == "---" {
			return true
		}
	}
	return false
}

func (s *scanner) nextBoundary(start int) int {
	var skip int
	for {
		r, width := utf8.DecodeRuneInString(s.input[start+skip:])
		if r == utf8.RuneError {
			break
		}

		switch r {
		case ' ', '\t', '\n', '=', '-':
			return skip
		}

		skip += width
	}
	return skip
}

func (s *scanner) nextDivider(start int) int {
	if i := strings.Index(s.input[start:], "---"); i != -1 {
		return i
	}
	return -1
}

func (s *scanner) rewind() {
	s.pos = s.prev
	s.inText = s.prevInText
}

func (s *scanner) emit() *token {
	var tok *token
	for {
		r, width := utf8.DecodeRuneInString(s.input[s.pos:])
		if r == utf8.RuneError {
			break
		}

		start := s.pos
		skip := 0

		switch {
		case s.matchDivider(s.pos):
			s.prevInText = s.inText
			s.inText = !s.inText
			tok = &token{
				Type:  DIVIDER,
				Start: start,
				End:   3,
			}
			skip = tok.End
			goto finish
		}

		switch {
		case r == '.':
			tok = &token{
				Type:  KEY,
				Start: start,
				End:   s.nextBoundary(start),
			}
			skip = tok.End
		case r == '=':
			tok = &token{
				Type:  EQUALS,
				Start: start,
				End:   width,
			}
			skip = tok.End
		case unicode.IsSpace(r):
			skip = width
		case unicode.IsLetter(r):
			if !s.inText {
				tok = &token{
					Type:  IDENT,
					Start: start,
					End:   s.nextBoundary(start),
				}
				skip = tok.End
			}
		}

		if s.inText && skip == 0 {
			i := s.nextDivider(s.pos)
			if i == -1 {
				i = len(s.input) - start
			}
			tok = &token{
				Type:  TEXT,
				Start: start,
				End:   i,
			}
			skip = i
			goto finish
		}

	finish:
		prev := s.pos
		s.pos += skip
		if tok != nil {
			tok.Text = &s.input
			s.prev = prev
			break
		}
	}

	return tok
}

type Section struct {
	Type string
	Of   string
	Text string
}

func parseSection(s *scanner) *Section {
	var section Section

	for {
		name, value := parseRule(s)
		if name == nil || value == nil {
			break
		}

		switch *name {
		case ".section":
			section.Type = *value
		case ".of":
			section.Of = *value
		}
	}

	if section.Type == "" {
		return nil
	}

	if tok := s.emit(); tok == nil || tok.Type != DIVIDER {
		return nil
	}

	tok := s.emit()
	if tok.Type != TEXT {
		return nil
	}

	section.Text = strings.Trim(tok.Lexeme(), " \t\n")

	s.emit()

	return &section
}

func parseRule(s *scanner) (*string, *string) {
	var name, value string

	tok := s.emit()
	if tok == nil {
		return nil, nil
	}

	if tok.Type != KEY {
		s.rewind()
		return nil, nil
	}

	name = tok.Lexeme()

	if tok = s.emit(); tok.Type != EQUALS {
		return nil, nil
	}

	tok = s.emit()
	if tok.Type != IDENT {
		return nil, nil
	}

	value = tok.Lexeme()

	return &name, &value
}

func ParseFile(path string) ([]Section, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	input := string(b)

	s := scanner{
		input: input,
		pos:   0,
	}

	var sections []Section
	for {
		section := parseSection(&s)
		if section == nil {
			break
		}

		sections = append(sections, *section)
	}

	return sections, nil
}

type StructDescriptionField struct {
	Name string
	Type string
	Tag  string
}

func (f *StructDescriptionField) ReflectType() reflect.Type {
	switch f.Type {
	case "string":
		return reflect.TypeOf("")
	case "int":
		return reflect.TypeOf(0)
	case "bool":
		return reflect.TypeOf(false)
	case "float":
		return reflect.TypeOf(float64(0))
	case "bytes":
		return reflect.TypeOf([]byte{})
	}
	return nil
}

type StructDescription struct {
	Name   string
	Fields []StructDescriptionField
}

func SQLizerForInputFile(path string) (*duckql.SQLizer, string, error) {
	sections, err := ParseFile(path)
	if err != nil {
		return nil, "", err
	}

	// First, we turn all definitions into structs
	var typeList []any
	var fullData []any
	var query string
	for _, section := range sections {
		if section.Type == "data" {
			t := types.TypeByName(section.Of)
			if t == nil {
				return nil, "", fmt.Errorf("no such data type %q", section.Of)
			}

			typeList = append(typeList, t)

			i, err := types.UnmarshallData(section.Of, []byte(section.Text))
			if err != nil {
				return nil, "", err
			}

			fullData = append(fullData, i)
		}

		if section.Type == "query" {
			query = section.Text
		}
	}

	sql := duckql.Initialize(typeList...)

	sql.SetPermissions(duckql.AllowSelectStatements)
	sql.SetBacking(duckql.NewSliceFilter(sql, fullData))

	return sql, query, nil
}
