package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/flarco/dbio"
	"github.com/flarco/dbio/iop"
	"github.com/spf13/cast"

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
	var columns iop.Columns

	table, err := ParseTableName(tableFName, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not get  table name for imoprt")
		return
	}

	// set OnSchemaChange
	if df := ds.Df(); df != nil && cast.ToBool(conn.GetProp("adjust_column_type")) {

		df.OnColumnChanged = func(col iop.Column) error {

			// sleep to allow transaction to close
			time.Sleep(100 * time.Millisecond)

			ds.Context.Lock()
			defer ds.Context.Unlock()

			table.Columns, err = conn.GetColumns(tableFName)
			if err != nil {
				return g.Error(err, "could not get table columns for schema change")
			}

			df.Columns[col.Position-1].Type = col.Type
			ok, err := conn.OptimizeTable(&table, df.Columns)
			if err != nil {
				return g.Error(err, "could not change table schema")
			} else if ok {
				for i := range df.Columns {
					df.Columns[i].Type = table.Columns[i].Type
				}
			}

			return nil
		}
	}

	for batch := range ds.BatchChan {
		if batch.ColumnsChanged() || batch.IsFirst() {
			columns, err = conn.GetColumns(tableFName, batch.Columns.Names()...)
			if err != nil {
				return count, g.Error(err, "could not get matching list of columns from table")
			}

			// err = batch.Shape(columns)
			// if err != nil {
			// 	return count, g.Error(err, "could not shape batch stream")
			// }

		}

		err = func() error {
			// COPY needs a transaction
			if conn.Tx() == nil {
				err = conn.Begin(&sql.TxOptions{Isolation: sql.LevelDefault})
				if err != nil {
					return g.Error(err, "could not begin")
				}
				defer conn.Rollback()
			}

			insFields, err := conn.ValidateColumnNames(columns.Names(), batch.Columns.Names(), true)
			if err != nil {
				return g.Error(err, "columns mismatch")
			}

			insertStatement := conn.GenerateInsertStatement(
				table.FullName(),
				insFields,
				1,
			)

			stmt, err := conn.Prepare(insertStatement)
			if err != nil {
				g.Trace("%s: %#v", table, columns.Names())
				return g.Error(err, "could not prepare statement")
			}

			for row := range batch.Rows {
				count++
				// Do insert
				ds.Context.Lock()
				_, err := stmt.Exec(row...)
				ds.Context.Unlock()
				if err != nil {
					ds.Context.CaptureErr(g.Error(err, "could not COPY into table %s", tableFName))
					ds.Context.Cancel()
					g.Trace("error for row: %#v", row)
					return g.Error(err, "could not execute statement")
				}
			}

			err = stmt.Close()
			if err != nil {
				return g.Error(err, "could not close statement")
			}

			err = conn.Commit()
			if err != nil {
				return g.Error(err, "could not commit transaction")
			}

			return nil
		}()

		if err != nil {
			return count, g.Error(err, "could not copy data")
		}
	}

	ds.SetEmpty()

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
