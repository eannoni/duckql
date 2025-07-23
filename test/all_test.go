package test

import (
	"fmt"
	"github.com/hexops/gotextdiff/span"
	"io"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/hexops/gotextdiff"
	"github.com/hexops/gotextdiff/myers"
)

func testPaths() (string, string, error) {
	basePath, err := os.Getwd()
	if err != nil {
		return "", "", err
	}

	inputPath := path.Join(basePath, "input")
	expectationsPath := path.Join(basePath, "expectations")

	info, err := os.Stat(inputPath)
	if err != nil {
		return "", "", err
	}

	if !info.IsDir() {
		return "", "", err
	}

	info, err = os.Stat(expectationsPath)
	if err != nil {
		return "", "", err
	}

	if !info.IsDir() {
		return "", "", err
	}

	return inputPath, expectationsPath, nil
}

func TestE2E(t *testing.T) {
	inputPath, expectationsPath, err := testPaths()
	if err != nil {
		t.Fatal(err)
	}

	err = filepath.Walk(inputPath, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			testName := info.Name()
			expectation := filepath.Join(expectationsPath, testName)
			t.Run(testName, func(t *testing.T) {
				sql, query, err := SQLizerForInputFile(path)
				if err != nil {
					t.Fatal(err)
				}

				r, err := sql.Execute(query)
				if err != nil {
					t.Fatal(err)
				}

				output := ".section = DDL\n---\n"
				output += sql.DDL()
				output += "---\n"
				output += ".section = Result\n---\n"
				output += r.String()

				if _, ok := os.LookupEnv("REBASE"); ok {
					f, err := os.OpenFile(expectation, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := f.Write([]byte(output)); err != nil {
						t.Fatal(err)
					}
					if err := f.Close(); err != nil {
						t.Fatal(err)
					}
				}

				if info, err := os.Stat(expectation); err != nil || info.IsDir() {
					edits := myers.ComputeEdits(span.URIFromPath("expectations/"+testName), "", output)
					diff := gotextdiff.ToUnified("expectations/"+testName, "input/"+testName, "", edits)
					t.Fatal(diff)
				}

				f, err := os.OpenFile(expectation, os.O_RDONLY, 0666)
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()

				b, err := io.ReadAll(f)
				if err != nil {
					t.Fatal(err)
				}

				edits := myers.ComputeEdits(span.URIFromPath("expectations/"+testName), string(b), output)
				diff := gotextdiff.ToUnified("expectations/"+testName, "input/"+testName, string(b), edits)

				fmt.Println(diff)
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
