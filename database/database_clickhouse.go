package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/flarco/dbio"
	"github.com/flarco/dbio/iop"
	"github.com/lib/pq"

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

// func (conn *ClickhouseConn) GetURL(newURL ...string) string {
// 	connURL := conn.BaseConn.URL
// 	if len(newURL) > 0 {
// 		connURL = newURL[0]
// 	}

// 	u, err := net.NewURL(connURL)
// 	if err != nil {
// 		g.LogError(err, "could not parse MySQL URL")
// 		return connURL
// 	}

// 	// Add tcp explicitly...
// 	URL := g.F(
// 		"tcp://%s:%d?debug=false",
// 		u.Hostname(), u.Port(),
// 	)

// 	return URL
// }

// Connect connects to the database
// func (conn *ClickhouseConn) Connect(timeOut ...int) (err error) {
// 	u, err := net.NewURL("clickhouse://admin:dElta123!@mpc:9000/default")
// 	if err != nil {
// 		return g.Error(err, "could not connect")
// 	}
// 	return conn.BaseConn.Connect()
// }

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
	err = conn.Begin()
	if err != nil {
		err = g.Error(err, "could not begin")
		return
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
	if df := ds.Df(); df != nil {

		df.OnSchemaChange = func(i int, newType iop.ColumnType) error {

			ds.Context.Lock()
			defer ds.Context.Unlock()

			_, err = stmt.Exec()
			if err != nil {
				return g.Error(err, "could not pre-execute statement")
			}

			err = stmt.Close()
			if err != nil {
				return g.Error(err, "could not pre-close statement")
			}

			err = conn.Commit()
			g.LogError(err)
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

			err = conn.Begin()
			if err != nil {
				return g.Error(err, "could not post-begin for schema change")
			}

			stmt, err = conn.Prepare(
				pq.CopyInSchema(table.Schema, table.Name, columns.Names()...),
			)
			if err != nil {
				return g.Error(err, "could not post-prepare statement")
			}

			return nil
		}
	}

	for row := range ds.Rows {
		count++
		// Do insert
		ds.Context.Lock()
		_, err := stmt.Exec(row...)
		ds.Context.Unlock()

		if err != nil {
			ds.Context.CaptureErr(g.Error(err, "could not COPY into table %s", tableFName))
			ds.Context.Cancel()
			conn.Context().Cancel()
			g.Trace("error for row: %#v", row)
			return count, g.Error(err, "could not execute statement")
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
	INSERT INTO {tgt_table} as tgt
		({insert_fields}) 
	SELECT {src_fields}
	FROM {src_table} as src
	WHERE true
	ON CONFLICT ({pk_fields})
	DO UPDATE 
	SET {set_fields}
	`

	sql = g.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"src_upd_pk_equal", strings.ReplaceAll(upsertMap["src_tgt_pk_equal"], "tgt.", "upd."),
		"src_fields", upsertMap["src_fields"],
		"pk_fields", upsertMap["pk_fields"],
		"set_fields", strings.ReplaceAll(upsertMap["set_fields"], "src.", "excluded."),
		"insert_fields", upsertMap["insert_fields"],
	)

	return
}
