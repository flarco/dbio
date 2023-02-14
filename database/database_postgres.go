package database

import (
	"bytes"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/flarco/dbio"
	"github.com/spf13/cast"

	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/lib/pq"
)

// PostgresConn is a Postgres connection
type PostgresConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *PostgresConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbPostgres
	conn.BaseConn.defaultPort = 5432

	// Turn off Bulk export for now
	// the CopyToStdout function frequently produces error `read |0: file already closed`
	// also is slower than just select?
	conn.BaseConn.SetProp("allow_bulk_export", "false")

	var instance Connection
	instance = conn
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// CopyToStdout Copy TO STDOUT
func (conn *PostgresConn) CopyToStdout(sql string) (stdOutReader io.Reader, err error) {
	var stderr bytes.Buffer
	copyQuery := fmt.Sprintf(`\copy ( %s ) TO STDOUT WITH CSV HEADER`, sql)
	copyQuery = strings.ReplaceAll(copyQuery, "\n", " ")

	proc := exec.Command("psql", conn.URL, "-X", "-c", copyQuery)
	proc.Stderr = &stderr
	stdOutReader, err = proc.StdoutPipe()

	go func() {
		if err := proc.Run(); err != nil {
			// bytes, _ := proc.CombinedOutput()
			cmdStr := strings.ReplaceAll(strings.Join(proc.Args, " "), conn.URL, "$DBURL")
			err = g.Error(
				err,
				fmt.Sprintf(
					"COPY FROM Command -> %s\nCOPY FROM Error   -> %s",
					cmdStr, stderr.String(),
				),
			)
			conn.Context().CaptureErr(err)
			g.LogError(err, "could not PG copy")
		}
	}()

	return stdOutReader, err
}

// BulkExportStream uses the bulk dumping (COPY)
func (conn *PostgresConn) BulkExportStream(sql string) (ds *iop.Datastream, err error) {
	_, err = exec.LookPath("psql")
	if err != nil {
		g.Trace("psql not found in path. Using cursor...")
		return conn.StreamRows(sql)
	}

	if conn.BaseConn.GetProp("allow_bulk_export") != "true" {
		return conn.StreamRows(sql)
	}

	stdOutReader, err := conn.CopyToStdout(sql)
	if err != nil {
		return ds, err
	}

	csv := iop.CSV{Reader: stdOutReader}
	ds, err = csv.ReadStream()

	return ds, err
}

// BulkImportStream inserts a stream into a table
func (conn *PostgresConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	var columns iop.Columns

	mux := ds.Context.Mux
	if df := ds.Df(); df != nil {
		mux = df.Context.Mux
	}

	table, err := ParseTableName(tableFName, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not get  table name for imoprt")
		return
	}

	// set OnSchemaChange
	if df := ds.Df(); df != nil && cast.ToBool(conn.GetProp("adjust_column_type")) {
		df.OnColumnChanged = func(col iop.Column) error {

			// sleep to allow transaction to close
			time.Sleep(300 * time.Millisecond)

			mux.Lock()
			defer mux.Unlock()

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
			mux.Lock()
			columns, err = conn.GetColumns(tableFName, batch.Columns.Names(true, false)...)
			mux.Unlock()
			if err != nil {
				return count, g.Error(err, "could not get matching list of columns from table")
			}

			err = batch.Shape(columns)
			if err != nil {
				return count, g.Error(err, "could not shape batch stream")
			}
		}

		err = func() error {

			// COPY needs a transaction
			if conn.Tx() == nil {
				err = conn.Begin()
				if err != nil {
					return g.Error(err, "could not begin")
				}
				defer conn.Rollback()
			}

			stmt, err := conn.Prepare(pq.CopyInSchema(table.Schema, table.Name, batch.Columns.Names()...))
			if err != nil {
				g.Trace("%s: %#v", table, columns.Names())
				return g.Error(err, "could not prepare statement")
			}

			for row := range batch.Rows {
				// g.PP(batch.Columns.MakeRec(row))
				count++
				// Do insert
				mux.Lock()
				_, err := stmt.Exec(row...)
				mux.Unlock()
				if err != nil {
					ds.Context.CaptureErr(g.Error(err, "could not COPY into table %s", tableFName))
					ds.Context.Cancel()
					g.DebugLow("error for rec: %s", g.Pretty(batch.Columns.MakeRec(row)))
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

	g.Trace("COPY %d ROWS", count)
	return count, nil
}

// CastColumnForSelect casts to the correct target column type
func (conn *PostgresConn) CastColumnForSelect(srcCol iop.Column, tgtCol iop.Column) (selectStr string) {
	qName := conn.Self().Quote(srcCol.Name)

	switch {
	case srcCol.IsString() && !tgtCol.IsString():
		selectStr = g.F("%s::%s as %s", qName, tgtCol.DbType, qName)
	case srcCol.IsString() && strings.ToLower(tgtCol.DbType) == "uuid":
		selectStr = g.F("%s::%s as %s", qName, tgtCol.DbType, qName)
	default:
		selectStr = qName
	}

	return selectStr
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *PostgresConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	indexSQL := g.R(
		conn.GetTemplateValue("core.create_unique_index"),
		"index", strings.Join(pkFields, "_")+"_idx",
		"table", tgtTable,
		"cols", strings.Join(pkFields, ", "),
	)

	// in order to use on conflict, the target table needs
	//  a unique index on the PK. We will not use it since
	// it complicates matters
	sqlTempl := `
	{indexSQL};
	insert into {tgt_table}
	({insert_fields})
	select {src_fields} from {src_table} src
	on conflict ({pk_fields})
	DO UPDATE 
	SET {set_fields}
	`

	tempTable := g.RandSuffix("temp", 5)

	tempIndexSQL := g.R(
		conn.GetTemplateValue("core.create_unique_index"),
		"index", tempTable+"_idx",
		"table", tempTable,
		"cols", strings.Join(pkFields, ", "),
	)

	sqlTempl = `
	create temporary table {temp_table} as
	with src_table as (
		select {src_fields} from {src_table}
	)
	, updates as (
		update {tgt_table} tgt
		set {set_fields}
		from src_table src
		where {src_tgt_pk_equal}
		returning tgt.*
	)
	select * from updates;

	{tempIndexSQL};

	with src_table as (
		select {src_fields} from {src_table}
	)
	insert into {tgt_table}
	({insert_fields})
	select {src_fields} from src_table src
	where not exists (
		select 1
		from {temp_table} upd
		where {src_upd_pk_equal}
	)
	`

	sql = g.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"temp_table", tempTable,
		"indexSQL", indexSQL,
		"tempIndexSQL", tempIndexSQL,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"src_upd_pk_equal", strings.ReplaceAll(upsertMap["src_tgt_pk_equal"], "tgt.", "upd."),
		"src_fields", upsertMap["src_fields"],
		"pk_fields", upsertMap["pk_fields"],
		// "set_fields", strings.ReplaceAll(upsertMap["set_fields"], "src.", "excluded."),
		"set_fields", upsertMap["set_fields"],
		"insert_fields", upsertMap["insert_fields"],
	)

	return
}
