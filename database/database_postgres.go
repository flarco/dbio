package database

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os/exec"
	"strings"

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
	conn.BaseConn.Type = PostgresDbType
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
			conn.Context().Cancel()
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

	schema, table := SplitTableFullName(tableFName)

	columns, err := conn.GetColumns(tableFName, ds.GetFields()...)
	if err != nil {
		err = g.Error(err, "could not get list of columns from table")
		return
	}

	ds, err = ds.Shape(columns)
	if err != nil {
		err = g.Error(err, "could not shape stream")
		return
	}

	colNames := ColumnNames(columns)
	err = conn.Begin(&sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: false})
	if err != nil {
		err = g.Error(err, "Could not begin transaction")
		return
	}

	stmt, err := conn.Tx().Prepare(pq.CopyInSchema(schema, table, colNames...))
	if err != nil {
		g.Trace("%s: %#v", table, colNames)
		return count, g.Error(err, "could not prepare statement")
	}

	for row := range ds.Rows {
		count++
		// Do insert
		_, err := stmt.Exec(row...)
		if err != nil {
			conn.Tx().Rollback()
			ds.Context.Cancel()
			conn.Context().Cancel()
			g.Trace("error for row: %#v", row)
			return count, g.Error(err, "could not execute statement")
		}
	}

	ds.SetEmpty()

	_, err = stmt.Exec()
	if err != nil {
		conn.Tx().Rollback()
		return count, g.Error(err, "could not execute statement")
	}

	err = stmt.Close()
	if err != nil {
		return count, g.Error(err, "could not close transaction")
	}

	err = conn.Commit()
	if err != nil {
		return count, g.Error(err, "could not commit transaction")
	}

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

// Upsert inserts / updates from a srcTable into a target table.
// Assuming the srcTable has some or all of the tgtTable fields with matching types
func (conn *PostgresConn) Upsert(srcTable string, tgtTable string, pkFields []string) (rowAffCnt int64, err error) {

	upsertMap, err := conn.Self().GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
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

	err = conn.Begin(&sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: false})
	if err != nil {
		err = g.Error(err, "Could not begin transaction for upsert")
		return
	}

	_, err = conn.Tx().ExecContext(conn.Context().Ctx, indexSQL)
	if err != nil && !strings.Contains(err.Error(), "already") {
		err = g.Error(err, "Could not execute upsert from %s to %s -> %s", srcTable, tgtTable, indexSQL)
		return
	}

	sqlTempl := `
	insert into {tgt_table}
	({insert_fields})
	select {src_fields} from {src_table} src
	on conflict ({pk_fields})
	DO UPDATE 
	SET {set_fields}
	`

	sql := g.R(
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
	res, err := conn.Tx().ExecContext(conn.Context().Ctx, sql)
	if err != nil {
		err = g.Error(err, "Could not execute upsert from %s to %s -> %s", srcTable, tgtTable, sql)
		return
	}

	rowAffCnt, err = res.RowsAffected()
	if err != nil {
		rowAffCnt = -1
	}

	err = conn.Commit()
	if err != nil {
		err = g.Error(err, "Could not commit upsert transaction")
		return
	}

	return
}
