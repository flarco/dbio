package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/flarco/dbio"
	"github.com/flarco/dbio/iop"

	"github.com/flarco/g"
)

// ClickhouseConn is a Clikchouse connection
type ClickhouseConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *ClickhouseConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbClickhouse

	var instance Connection
	instance = conn
	conn.BaseConn.instance = &instance
	return conn.BaseConn.Init()
}

// NewTransaction creates a new transaction
func (conn *ClickhouseConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (Transaction, error) {

	context := g.NewContext(ctx)

	if len(options) == 0 {
		options = []*sql.TxOptions{&sql.TxOptions{}}
	}

	tx, err := conn.Db().BeginTxx(context.Ctx, options[0])
	if err != nil {
		return nil, g.Error(err, "could not begin Tx")
	}

	Tx := &BaseTransaction{Tx: tx, Conn: conn.Self(), context: &context}
	conn.tx = Tx

	// CH does not support transactions at the moment
	// Tx := &BlankTransaction{Conn: conn.Self(), context: &context}

	return Tx, nil
}

// BulkImportStream inserts a stream into a table
func (conn *ClickhouseConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {

	table, err := ParseTableName(tableFName, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not get  table name for imoprt")
		return
	}

	columns, err := conn.GetColumns(tableFName, ds.GetFields(true, true)...)
	if err != nil {
		err = g.Error(err, "could not get list of columns from table")
		return
	}

	insFields, err := conn.ValidateColumnNames(columns.Names(), ds.GetFields(true, true), true)
	if err != nil {
		err = g.Error(err, "columns mismatch")
		return
	}

	ds, err = ds.Shape(columns)
	if err != nil {
		err = g.Error(err, "could not shape stream")
		return
	}

	// COPY needs a transaction
	if conn.Tx() == nil {
		err = conn.Begin(&sql.TxOptions{Isolation: sql.LevelDefault})
		if err != nil {
			err = g.Error(err, "could not begin")
			return
		}
		defer conn.Commit()
	}

	insertStatement := conn.GenerateInsertStatement(
		table.FullName(),
		insFields,
		1,
	)

	stmt, err := conn.Prepare(insertStatement)
	if err != nil {
		g.Trace("%s: %#v", table, columns.Names())
		return count, g.Error(err, "could not prepare statement")
	}

	// set OnSchemaChange
	if df := ds.Df(); df != nil && conn.GetProp("adjust_column_type") != "false" {

		df.OnSchemaChange = func(i int, newType iop.ColumnType) error {

			ds.Context.Lock()
			defer ds.Context.Unlock()

			err = conn.Commit()
			if err != nil {
				return g.Error(err, "could not pre-commit for schema change")
			}

			table.Columns, err = conn.GetColumns(tableFName)
			if err != nil {
				return g.Error(err, "could not get table columns for schema change")
			}

			df.Columns[i].Type = newType
			ok, err := conn.OptimizeTable(&table, df.Columns)
			if err != nil {
				return g.Error(err, "could not change table schema")
			} else if ok {
				for i := range df.Columns {
					df.Columns[i].Type = table.Columns[i].Type
				}
			}

			err = conn.Begin(&sql.TxOptions{Isolation: sql.LevelDefault})
			if err != nil {
				return g.Error(err, "could not post-begin for schema change")
			}

			stmt, err = conn.Prepare(insertStatement)
			if err != nil {
				return g.Error(err, "could not post-prepare statement")
			}

			return nil
		}
	}

	for row := range ds.Rows {
		count++

		ds.Context.Lock()
		_, err := stmt.Exec(row...)
		ds.Context.Unlock()

		if err != nil {
			ds.Context.CaptureErr(g.Error(err, "could not COPY into table %s", tableFName))
			ds.Context.Cancel()
			g.Trace("error for row: %#v", row)
			return count, g.Error(err, "could not execute statement row")
		}
	}

	ds.SetEmpty()

	err = conn.Commit()
	if err != nil {
		return count, g.Error(err, "could not commit statement")
	}

	g.Debug("COPY %d ROWS", count)
	return count, nil
}

// GenerateInsertStatement returns the proper INSERT statement
func (conn *ClickhouseConn) GenerateInsertStatement(tableName string, fields []string, numRows int) string {

	values := make([]string, len(fields))
	qFields := make([]string, len(fields)) // quoted fields

	valuesStr := ""
	c := 0
	for n := 0; n < numRows; n++ {
		for i, field := range fields {
			c++
			values[i] = conn.bindVar(i+1, field, n, c)
			qFields[i] = conn.Self().Quote(field)
		}
		valuesStr += fmt.Sprintf("(%s),", strings.Join(values, ", "))
	}

	statement := g.R(
		"INSERT INTO {table} ({fields}) VALUES {values}",
		"table", tableName,
		"fields", strings.Join(qFields, ", "),
		"values", strings.TrimSuffix(valuesStr, ","),
	)
	g.Trace("insert statement: "+strings.Split(statement, ") VALUES ")[0]+")"+" x %d", numRows)
	return statement
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *ClickhouseConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {
	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	sqlTempl := `
	ALTER TABLE {tgt_table}
	DELETE WHERE ({pk_fields}) in (
			SELECT {pk_fields}
			FROM {src_table} src
	)
	;

	INSERT INTO {tgt_table}
		({insert_fields})
	SELECT {src_fields}
	FROM {src_table} src
	`
	sql = g.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", upsertMap["src_fields"],
		"pk_fields", upsertMap["pk_fields"],
	)

	return
}
