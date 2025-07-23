package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"

	"github.com/dburkart/duckql"
	_ "github.com/mattn/go-sqlite3"
)

var users = [][]string{
	{"John Doe", "john@example.com", "secret1"},
	{"Jane Doe", "jane@example.com", "secret2"},
	{"Bob Smith", "bob@example.com", "secret3"},
}

func initDB() (*sql.DB, error) {
	if _, err := os.Stat("/tmp/users.db"); err == nil {
		return sql.Open("sqlite3", "/tmp/users.db")
	}

	db, err := sql.Open("sqlite3", "/tmp/users.db")
	if err != nil {
		return nil, err
	}
	defer db.Close()

	schema := `
	CREATE TABLE users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		email TEXT NOT NULL UNIQUE,
		secret TEXT NOT NULL
	);
	`

	if _, err := db.Exec(schema); err != nil {
		panic(err)
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT INTO users (name, email, secret) VALUES (?, ?, ?)")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	for _, user := range users {
		if _, err := stmt.Exec(user[0], user[1], user[2]); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return db, nil
}

type User struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

func main() {
	db, err := initDB()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	s := duckql.Initialize(&User{})
	s.SetPermissions(duckql.AllowSelectStatements)
	s.SetBacking(duckql.NewSQLiteBacking(db, s))

	fmt.Println("Enter SQLite queries (Ctrl+C to exit):")
	fmt.Println()
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		query := scanner.Text()

		result, err := s.Execute(query)
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
