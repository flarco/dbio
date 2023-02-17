//go:build !windows
// +build !windows

package database

import (
	"database/sql/driver"
	"strings"

	"github.com/flarco/dbio"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	duckdb "github.com/marcboeker/go-duckdb"
)

// DuckDbConn is a Duck DB connection
type DuckDbConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *DuckDbConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbDuckDb

	var instance Connection
	instance = conn
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// GetURL returns the processed URL
func (conn *DuckDbConn) GetURL(newURL ...string) string {
	connURL := conn.BaseConn.URL
	if len(newURL) > 0 {
		connURL = newURL[0]
	}
	URL := strings.ReplaceAll(
		connURL,
		"duckdb://",
		"",
	)
	return URL
}

// BulkImportStream inserts a stream into a table
func (conn *DuckDbConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	var columns iop.Columns

	// FIXME: batching works better when transactions are closed
	// seems, when the appender is closed, the transaction is closed as well
	conn.Commit()

	table, err := ParseTableName(tableFName, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not get  table name for imoprt")
		return
	}

	for batch := range ds.BatchChan {
		if batch.ColumnsChanged() || batch.IsFirst() {
			columns, err = conn.GetColumns(tableFName, batch.Columns.Names(true, true)...)
			if err != nil {
				return count, g.Error(err, "could not get list of columns from table")
			}

			err = batch.Shape(columns)
			if err != nil {
				return count, g.Error(err, "could not shape batch stream")
			}
		}

		err = func() error {
			// COPY needs a transaction
			dbConn, err := conn.Db().Conn(ds.Context.Ctx)
			if err != nil {
				return g.Error(err, "could not open transaction conn")
			}

			err = dbConn.Raw(
				func(driverConn any) (err error) {
					drvConn, ok := driverConn.(driver.Conn)
					if !ok {
						return g.Error("could not cast duckdb conn as driver.Conn")
					}

					appender, err := duckdb.NewAppenderFromConn(drvConn, table.Schema, table.Name)
					if err != nil {
						return g.Error(err, "could not open transaction appender")
					}

					for row0 := range batch.Rows {
						// g.PP(batch.Columns.MakeRec(row))

						row := make([]driver.Value, len(row0))
						for i := range row0 {
							row[i] = row0[i]
						}

						count++

						// Do insert
						ds.Context.Lock()
						err := appender.AppendRow(row...)
						ds.Context.Unlock()
						if err != nil {
							ds.Context.CaptureErr(g.Error(err, "could not Append Row into table %s", tableFName))
							ds.Context.Cancel()
							g.Trace("error for row: %#v", row)
							return g.Error(err, "could not execute statement")
						}
					}

					err = appender.Close()
					if err != nil {
						return g.Error(err, "could not close transaction appender")
					}

					return nil
				})
			if err != nil {
				return g.Error(err, "could not open insert appender rows")
			}

			return nil
		}()

		if err != nil {
			return count, g.Error(err, "could not copy data")
		}
	}

	ds.SetEmpty()

	g.Trace("COPY %d ROWS", count)
	return count, nil
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *DuckDbConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	// _, indexTable := SplitTableFullName(tgtTable)

	// indexSQL := g.R(
	// 	conn.GetTemplateValue("core.create_unique_index"),
	// 	"index", strings.Join(pkFields, "_")+"_idx",
	// 	"table", indexTable,
	// 	"cols", strings.Join(pkFields, ", "),
	// )

	// _, err = conn.Exec(indexSQL)
	// if err != nil {
	// 	err = g.Error(err, "could not create unique index")
	// 	return
	// }

	// V0.7
	// sqlTempl := `
	// INSERT INTO {tgt_table} as tgt
	// 	({insert_fields})
	// SELECT {src_fields}
	// FROM {src_table} as src
	// WHERE true
	// ON CONFLICT ({pk_fields})
	// DO UPDATE
	// SET {set_fields}
	// `

	sqlTempl := `
	DELETE FROM {tgt_table} tgt
	USING {src_table} src
	WHERE {src_tgt_pk_equal}
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
		"src_upd_pk_equal", strings.ReplaceAll(upsertMap["src_tgt_pk_equal"], "tgt.", "upd."),
		"src_fields", upsertMap["src_fields"],
		"pk_fields", upsertMap["pk_fields"],
		"set_fields", strings.ReplaceAll(upsertMap["set_fields"], "src.", "excluded."),
		"insert_fields", upsertMap["insert_fields"],
	)

	return
}
