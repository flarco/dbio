package database

import (
	"context"
	"database/sql"
	"strings"

	"github.com/flarco/dbio"

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
func (conn *ClickhouseConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (tx Transaction, err error) {
	// context := g.NewContext(ctx)

	// CH does not support transactions at the moment
	// Tx := &BlankTransaction{Conn: conn.Self(), context: &context}

	return nil, nil
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
