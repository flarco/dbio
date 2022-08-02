package database

import (
	"os"
	"testing"

	"github.com/flarco/g"
)

func TestDataAnalyzer(t *testing.T) {
	dbURL := os.Getenv("POSTGRES_URL")
	conn, err := NewConn(dbURL)
	if !g.AssertNoError(t, err) {
		return
	}

	schemas := []string{"public"}
	da, err := NewDataAnalyzer(conn, DataAnalyzerOptions{SchemaNames: schemas})
	if !g.AssertNoError(t, err) {
		return
	}

	err = da.AnalyzeColumns(10000)
	if !g.AssertNoError(t, err) {
		return
	}

	err = da.ProcessRelations()
	if !g.AssertNoError(t, err) {
		return
	}
}
