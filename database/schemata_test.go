package database

import (
	"testing"

	"github.com/flarco/dbio"
	"github.com/stretchr/testify/assert"
)

func TestParseTableName(t *testing.T) {
	/*
		schema.table
		"ScheMa".table
		"ScheMa Name".table
		schema."Table Name"
		"ScheMa Name"."Table Name"
	*/
	type testCase struct {
		input   string
		dialect dbio.Type
		output  Table
	}
	cases := []testCase{
		{
			input:   `schema.table`,
			dialect: dbio.TypeDbSnowflake,
			output:  Table{Schema: "SCHEMA", Name: "TABLE"},
		},
		{
			input:   `"ScheMa".table`,
			dialect: dbio.TypeDbSnowflake,
			output:  Table{Schema: "ScheMa", Name: "TABLE"},
		},
		{
			input:   `"ScheMa Name".table`,
			dialect: dbio.TypeDbSnowflake,
			output:  Table{Schema: "ScheMa Name", Name: "TABLE"},
		},
		{
			input:   "schema.`Table Name`",
			dialect: dbio.TypeDbMySQL,
			output:  Table{Schema: "schema", Name: "Table Name"},
		},
		{
			input:   `"ScheMa Name"."Table Name"`,
			dialect: dbio.TypeDbSnowflake,
			output:  Table{Schema: "ScheMa Name", Name: "Table Name"},
		},
		{
			input:   `select 1 from table `,
			dialect: dbio.TypeDbSnowflake,
			output:  Table{SQL: `select 1 from table`},
		},
	}

	for _, c := range cases {
		table, err := ParseTableName(c.input, c.dialect)
		if !assert.NoError(t, err, c) {
			return
		}
		assert.Equal(t, c.output.Name, table.Name, c)
		assert.Equal(t, c.output.Schema, table.Schema, c)
		assert.Equal(t, c.output.Database, table.Database, c)
		assert.Equal(t, c.output.SQL, table.SQL, c)
	}
}
