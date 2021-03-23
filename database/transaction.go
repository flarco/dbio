package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/cast"
)

// Transaction is a database transaction
type Transaction struct {
	Tx      *sqlx.Tx
	Context *g.Context
	Conn    Connection
	log     []string
}

type Result struct {
	rowsAffected int64
}

func (r Result) LastInsertId() (int64, error) {
	return 0, nil
}

func (r Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// Commit commits connection wide transaction
func (t *Transaction) Commit() (err error) {
	select {
	case <-t.Context.Ctx.Done():
		t.Rollback()
		err = t.Context.Err()
		return
	default:
		err = t.Tx.Commit()
		if err != nil {
			err = g.Error(err, "could not commit Tx\n%s", strings.Join(t.log, "\n -----------------------\n"))
		}
	}
	return
}

// Rollback rolls back connection wide transaction
func (t *Transaction) Rollback() (err error) {
	if t == nil || t.Tx == nil {
		return
	}
	err = t.Tx.Rollback()
	if err != nil {
		err = g.Error(err, "could not rollback Tx")
	}
	return
}

// Prepare prepares the statement
func (t *Transaction) Prepare(query string) (stmt *sql.Stmt, err error) {
	stmt, err = t.Tx.PrepareContext(t.Context.Ctx, query)
	if err != nil {
		err = g.Error(err, "could not prepare Tx")
	}
	return
}

// Exec runs a sql query, returns `error`
func (t *Transaction) Exec(sql string, args ...interface{}) (result sql.Result, err error) {
	result, err = t.ExecContext(t.Context.Ctx, sql, args...)
	if err != nil {
		err = g.Error(err, "Could not execute SQL")
	}
	return
}

// QueryContext queries rows
func (t *Transaction) QueryContext(ctx context.Context, q string, args ...interface{}) (result *sqlx.Rows, err error) {
	t.log = append(t.log, q)
	if !strings.Contains(q, noTraceKey) {
		g.Debug(q)
	}
	result, err = t.Tx.QueryxContext(ctx, q, args...)
	if err != nil {
		err = g.Error(err, "Error executing query")
	}
	return
}

// ExecContext runs a sql query with context, returns `error`
func (t *Transaction) ExecContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {
	if strings.TrimSpace(q) == "" {
		err = g.Error("Empty Query")
		return
	}

	if !strings.Contains(q, noTraceKey) {
		g.Debug(CleanSQL(t.Conn, q), args...)
	}

	t.log = append(t.log, q)
	result, err = t.Tx.ExecContext(ctx, q, args...)
	if err != nil {
		err = g.Error(err, "Error executing "+CleanSQL(t.Conn, q))
	}

	return
}

// ExecMultiContext runs multiple sql queries with context, returns `error`
func (t *Transaction) ExecMultiContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {

	Res := Result{rowsAffected: 0}

	eG := g.ErrorGroup{}
	for _, sql := range ParseSQLMultiStatements(q) {
		res, err := t.ExecContext(ctx, sql, args...)
		if err != nil {
			eG.Capture(g.Error(err, "Error executing query"))
		} else {
			ra, _ := res.RowsAffected()
			g.Debug("RowsAffected: %d", ra)
			Res.rowsAffected = Res.rowsAffected + ra
		}
	}

	err = eG.Err()
	result = Res

	return
}

// DisableTrigger disables a trigger
func (t *Transaction) DisableTrigger(tableName, triggerName string) (err error) {
	template := t.Conn.GetTemplateValue("core.disable_trigger")
	sql := g.R(
		template,
		"table", tableName,
		"trigger", triggerName,
	)
	if sql == "" {
		return
	}
	_, err = t.Exec(sql)
	if err != nil {
		return g.Error(err, "could not disable trigger %s on %s", triggerName, tableName)
	}
	return
}

// EnableTrigger enables a trigger
func (t *Transaction) EnableTrigger(tableName, triggerName string) (err error) {
	template := t.Conn.GetTemplateValue("core.enable_trigger")
	sql := g.R(
		template,
		"table", tableName,
		"trigger", triggerName,
	)
	if sql == "" {
		return
	}
	_, err = t.Exec(sql)
	if err != nil {
		return g.Error(err, "could not enable trigger %s on %s", triggerName, tableName)
	}
	return
}

// UpsertStream inserts a stream into a table in batch
func (t *Transaction) UpsertStream(tableFName string, ds *iop.Datastream, pk []string) (count uint64, err error) {

	// create tmp table first
	return
}

// InsertStream inserts a stream into a table
func (t *Transaction) InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	count, err = InsertStream(t.Conn, t, tableFName, ds)
	if err != nil {
		err = g.Error(err, "Could not insert into %s", tableFName)
	}
	return
}

// InsertBatchStream inserts a stream into a table in batch
func (t *Transaction) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	count, err = InsertBatchStream(t.Conn, t, tableFName, ds)
	if err != nil {
		err = g.Error(err, "Could not batch insert into %s", tableFName)
	}
	return
}

// Upsert does an upsert from source table into target table
func (t *Transaction) Upsert(sourceTable, targetTable string, pkFields []string) (count uint64, err error) {
	cnt, err := Upsert(t.Conn, t, sourceTable, targetTable, pkFields)
	if err != nil {
		err = g.Error(err, "Could not upsert from %s into %s", sourceTable, targetTable)
	}
	count = cast.ToUint64(cnt)
	return
}

// InsertStream inserts a stream
func InsertStream(conn Connection, tx *Transaction, tableFName string, ds *iop.Datastream) (count uint64, err error) {
	// make sure fields match
	columns, err := conn.GetSQLColumns(tableFName)
	if err != nil {
		err = g.Error(err, "could not get column list")
		return
	}
	insFields, err := conn.ValidateColumnNames(columns.Names(), ds.GetFields(), true)
	if err != nil {
		err = g.Error(err, "columns mismatch")
		return
	}

	insertTemplate := conn.Self().GenerateInsertStatement(tableFName, insFields, 1)

	var stmt *sql.Stmt
	if tx != nil {
		stmt, err = tx.Prepare(insertTemplate)
	} else {
		stmt, err = conn.Prepare(insertTemplate)
	}

	if err != nil {
		err = g.Error(err, "Could not prepate statement")
		return
	}
	for row := range ds.Rows {
		count++
		// Do insert
		_, err = stmt.ExecContext(ds.Context.Ctx, row...)
		if err != nil {
			return count, g.Error(
				err,
				fmt.Sprintf("Insert: %s\nFor Row: %#v", insertTemplate, row),
			)
		}
	}

	return count, nil
}

// InsertBatchStream inserts a stream into a table in batch
func InsertBatchStream(conn Connection, tx *Transaction, tableFName string, ds *iop.Datastream) (count uint64, err error) {
	context := conn.Context()
	if tx != nil {
		context = tx.Context
	}

	// make sure fields match
	columns, err := conn.GetSQLColumns(tableFName)
	if err != nil {
		err = g.Error(err, "could not get column list")
		return
	}

	batchSize := cast.ToInt(conn.GetTemplateValue("variable.batch_values")) / len(columns)

	ds, err = ds.Shape(columns)
	if err != nil {
		err = g.Error(err, "could not shape stream")
		return
	}

	insFields, err := conn.ValidateColumnNames(columns.Names(), ds.GetFields(), true)
	if err != nil {
		err = g.Error(err, "columns mismatch")
		return
	}

	insertBatch := func(rows [][]interface{}) error {
		var err error
		defer context.Wg.Write.Done()
		insertTemplate := conn.Self().GenerateInsertStatement(tableFName, insFields, len(rows))

		// open statement
		var stmt *sql.Stmt
		if tx != nil {
			stmt, err = tx.Prepare(insertTemplate)
		} else {
			stmt, err = conn.Prepare(insertTemplate)
		}
		if err != nil {
			err = g.Error(err, "Error in PrepareContext")
			context.CaptureErr(err)
			context.Cancel()
			return context.Err()
		}

		vals := []interface{}{}
		for _, row := range rows {
			vals = append(vals, row...)
		}

		// Do insert
		_, err = stmt.ExecContext(ds.Context.Ctx, vals...)
		if err != nil {
			batchErrStr := g.F("Batch Size: %d rows x %d cols = %d", len(rows), len(rows[0]), len(rows)*len(rows[0]))
			if len(insertTemplate) > 3000 {
				insertTemplate = insertTemplate[:3000]
			}
			if len(rows) > 10 {
				rows = rows[:10]
			}
			g.Debug(g.F(
				"%s\n%s \n%s",
				err.Error(), batchErrStr,
				fmt.Sprintf("Insert: %s", insertTemplate),
				// fmt.Sprintf("\n\nRows: %#v", rows),
			))
			context.CaptureErr(err)
			context.Cancel()
			return context.Err()
		}

		// close statement
		err = stmt.Close()
		if err != nil {
			err = g.Error(
				err,
				fmt.Sprintf("stmt.Close: %s", insertTemplate),
			)
			context.CaptureErr(err)
			context.Cancel()
		}
		return context.Err()
	}

	batchRows := [][]interface{}{}
	g.Trace("batchRows")
	for row := range ds.Rows {
		batchRows = append(batchRows, row)
		count++
		if len(batchRows) == batchSize {
			g.Trace("batchSize %d", len(batchRows))
			select {
			case <-context.Ctx.Done():
				return count, context.Err()
			case <-ds.Context.Ctx.Done():
				return count, ds.Context.Err()
			default:
				context.Wg.Write.Add()
				go insertBatch(batchRows)
			}

			batchRows = [][]interface{}{}
		}
	}

	// remaining batch
	g.Trace("remaining batch")
	if len(batchRows) > 0 {
		context.Wg.Write.Add()
		err = insertBatch(batchRows)
		if err != nil {
			return count - cast.ToUint64(len(batchRows)), g.Error(err, "insertBatch")
		}
	}

	context.Wg.Write.Wait()
	err = context.Err()
	ds.SetEmpty()

	if err != nil {
		ds.Context.Cancel()
		return count - cast.ToUint64(batchSize), g.Error(err, "insertBatch")
	}

	if ds.Err() != nil {
		return count, g.Error(ds.Err(), "context error")
	}

	return count, nil
}

// Upsert upserts from source table into target table
func Upsert(conn Connection, tx *Transaction, sourceTable, targetTable string, pkFields []string) (count int64, err error) {

	q, err := conn.GenerateUpsertSQL(sourceTable, targetTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert sql")
		return
	}

	var result sql.Result
	if tx != nil {
		result, err = tx.ExecMultiContext(tx.Context.Ctx, q)
	} else {
		result, err = conn.ExecMulti(q)
	}
	if err != nil {
		err = g.Error(err, "Could not upsert")
		return
	}

	count, err = result.RowsAffected()
	if err != nil {
		count = -1
	}

	return
}
