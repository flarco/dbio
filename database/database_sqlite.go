package database

import (
	"database/sql"
	"strings"

	"github.com/flarco/g"
)

// SQLiteConn is a Google Big Query connection
type SQLiteConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *SQLiteConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = SQLiteDbType

	var instance Connection
	instance = conn
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// Upsert inserts / updates from a srcTable into a target table.
// Assuming the srcTable has some or all of the tgtTable fields with matching types
func (conn *SQLiteConn) Upsert(srcTable string, tgtTable string, pkFields []string) (rowAffCnt int64, err error) {

	upsertMap, err := conn.Self().GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	_, table := SplitTableFullName(tgtTable)

	indexSQL := g.R(
		conn.GetTemplateValue("core.create_unique_index"),
		"index", strings.Join(pkFields, "_")+"_idx",
		"table", table,
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
	INSERT INTO {tgt_table} as tgt
		({insert_fields}) 
	SELECT {src_fields}
	FROM {src_table} as src
	WHERE true
	ON CONFLICT ({pk_fields})
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
