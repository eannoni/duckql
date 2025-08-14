package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/dburkart/duckql"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
	"log"
	"os"
	"time"
)

type Item struct {
	ID              int       `sheets:"A"`
	Name            string    `sheets:"B"`
	Description     string    `sheets:"C"`
	CurrentLocation string    `sheets:"D"`
	GeneralNotes    string    `sheets:"E"`
	Plan            string    `sheets:"F"`
	Recipient       string    `sheets:"G"`
	Timing          string    `sheets:"H"`
	EstimatedValue  float32   `sheets:"I"`
	ActualValue     float32   `sheets:"J"`
	ResearchedValue float32   `sheets:"K"`
	CreatedOn       time.Time `sheets:"L"`
	CompletedOn     time.Time `sheets:"M"`
	Boolean         bool      `sheets:"N"`
}

func main() {
	var sheetId string
	if sheetId = os.Getenv("SHEET_ID"); sheetId == "" {
		panic("SHEET_ID environment variable not set")
	}

	sql := duckql.Initialize(&Item{})

	credentialsFile := "service-account.credentials.json"
	service, err := sheets.NewService(context.Background(), option.WithCredentialsFile(credentialsFile))
	if err != nil {
		log.Fatalf("Unable to create Sheets service: %v", err)
	}

	sql.SetPermissions(duckql.AllowSelectStatements)
	sql.SetBacking(duckql.NewSheetsBacking(sql, &duckql.SheetsOptions{
		Service:      service,
		SheetId:      sheetId,
		DataRowStart: 2,
		IDColumn:     "A",
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
