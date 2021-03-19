package database

import (
	"context"
	"database/sql"
	"strings"

	"github.com/flarco/g"
	"github.com/jmoiron/sqlx"
)

type Transaction struct {
	Tx      *sqlx.Tx
	Context *g.Context
	conn    Connection
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
	err = t.Tx.Commit()
	if err != nil {
		err = g.Error(err, "could not commit Tx")
	}
	return
}

// Rollback rolls back connection wide transaction
func (t *Transaction) Rollback() (err error) {
	err = t.Tx.Rollback()
	t.Tx = nil
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

// ExecContext runs a sql query with context, returns `error`
func (t *Transaction) ExecContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {
	if strings.TrimSpace(q) == "" {
		err = g.Error("Empty Query")
		return
	}

	result, err = t.Tx.ExecContext(ctx, q, args...)
	if err != nil {
		err = g.Error(err, "Error executing "+CleanSQL(t.conn, q))
	}

	return
}

// ExecMultiContext runs multiple sql queries with context, returns `error`
func (t *Transaction) ExecMultiContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {

	Res := Result{rowsAffected: 0}

	for _, sql := range ParseSQLMultiStatements(q) {
		res, err := t.ExecContext(ctx, sql, args...)
		if err != nil {
			err = g.Error(err, "Error executing query")
		} else {
			ra, _ := res.RowsAffected()
			Res.rowsAffected = Res.rowsAffected + ra
		}
	}

	result = Res

	return
}
