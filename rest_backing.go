package duckql

import (
	"errors"
	"github.com/rqlite/sql"
	"net/http"
	"reflect"
)

type RESTOptions struct {
	Url    string
	Header http.Header
}

type route struct {
	method  string
	options RESTOptions
	handler func(*http.Response) (any, error)
}

type RESTBacking struct {
	s      *SQLizer
	exec   *QueryExecutor
	routes map[string]route
}

func (r *RESTBacking) Visit(n sql.Node) (sql.Visitor, sql.Node, error) {
	if r.exec == nil {
		r.exec = NewQueryExecutor(r.s, r.FillIntermediate)
	}

	return r.exec.Visit(n)
}

func (r *RESTBacking) VisitEnd(n sql.Node) (sql.Node, error) {
	return n, nil
}

func (r *RESTBacking) Rows() ResultRows {
	return r.exec.Rows()
}

func (r *RESTBacking) FillIntermediate(intermediate *IntermediateTable) {
	if intermediate == nil {
		return
	}

	if intermediate.Source == nil {
		return
	}

	routeToCall := r.routes[intermediate.Source.Name]
	if routeToCall.method != "GET" {
		return
	}

	req, err := http.NewRequest(routeToCall.method, routeToCall.options.Url, nil)

	req.Header = routeToCall.options.Header

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}

	data, err := routeToCall.handler(resp)
	if err != nil {
		return
	}

	for _, column := range intermediate.Source.Columns {
		intermediate.Columns = append(intermediate.Columns, column)
	}

	t := reflect.TypeOf(data)
	if t.Kind() == reflect.Slice {
		v := reflect.ValueOf(data)

		if v.Len() == 0 {
			return
		}

		if t.Elem().Kind() != reflect.Struct {
			return
		}

		for i := 0; i < v.Len(); i++ {
			var result ResultRow

			for _, column := range intermediate.Columns {
				var val reflect.Value
				switch v.Index(i).Kind() {
				case reflect.Ptr:
					val = v.Index(i).Elem().FieldByName(intermediate.Source.ColumnMappings[column].GoField)
				case reflect.Struct:
					val = v.Index(i).FieldByName(intermediate.Source.ColumnMappings[column].GoField)
				}
				result = append(result, ResultValue{Name: column, Value: val})
			}

			intermediate.Rows = append(intermediate.Rows, result)
		}
	}
}

func (r *RESTBacking) Get(s any, options RESTOptions, handler func(*http.Response) (any, error)) error {
	table := r.s.TableForData(s)
	if table == nil {
		return errors.New("duckql: table not found")
	}

	if options.Url == "" {
		return errors.New("duckql: options url is required")
	}

	r.routes[table.Name] = route{
		method:  http.MethodGet,
		options: options,
		handler: handler,
	}

	return nil
}

func NewRESTBacking(s *SQLizer) *RESTBacking {
	return &RESTBacking{
		s:      s,
		routes: make(map[string]route),
	}
}
