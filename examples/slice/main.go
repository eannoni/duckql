package main

import (
	"bufio"
	"fmt"
	"github.com/dburkart/duckql"
	"os"
)

type User struct {
	ID             int
	Name           string
	OrganizationID int
}

type Organization struct {
	ID   int
	Name string
}

func main() {
	sql := duckql.Initialize(&Organization{}, &User{})

	sql.SetPermissions(duckql.AllowSelectStatements)
	sql.SetBacking(duckql.NewSliceFilter(sql, []any{
		[]*User{
			{ID: 1, Name: "John", OrganizationID: 1},
			{ID: 2, Name: "Jane", OrganizationID: 2},
			{ID: 3, Name: "Alice", OrganizationID: 2},
		},
		[]*Organization{
			{ID: 1, Name: "Acme Inc."},
			{ID: 2, Name: "Initech"},
		},
	}))

	fmt.Println("Enter SQLite queries (Ctrl+C to exit):")
	fmt.Println()
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		query := scanner.Text()

		result, err := sql.Execute(query)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		fmt.Println(result.String())
		fmt.Println()
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %v\n", err)
	}
}
