package main

import (
	"fmt"

	"github.com/dburkart/duckql"
)

type Organization struct {
	ID       int `ddl:"primary"`
	Accounts []Account
}

type Account struct {
	ID             int `ddl:"primary"`
	FirstName      string
	LastName       string
	Email          string `ddl:"comment='Not validated'"`
	Age            int
	OrganizationID int
	Internal       bool `ddl:"-"`
}

func main() {
	people := []any{
		&Account{
			ID:        1,
			FirstName: "John",
			LastName:  "Smith",
			Email:     "john@aol.com",
			Age:       18,
		},
		&Account{
			ID:        2,
			FirstName: "Jane",
			LastName:  "Smith",
			Email:     "jane@gmail.com",
			Age:       21,
		},
		&Account{
			ID:        3,
			FirstName: "George",
			LastName:  "Orwell",
			Email:     "george@aol.com",
			Age:       42,
		},
	}

	s := duckql.Initialize(&Organization{}, &Account{})
	s.SetPermissions(duckql.AllowSelectStatements)
	s.SetBacking(duckql.NewSliceFilter(
		s, people,
	))

	query := "select avg(age), email from accounts"

	result, err := s.Execute(query)
	if err != nil {
		panic(err)
	}

	fmt.Println(s.DDL())
	fmt.Println()

	fmt.Println()
	fmt.Println("Query:", query)
	fmt.Println()

	fmt.Println("Results")
	fmt.Println(result.String())
}
