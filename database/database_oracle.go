package database

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/flarco/dbio"

	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/flarco/g/csv"
	"github.com/spf13/cast"
	"github.com/xo/dburl"
)

// OracleConn is a Postgres connection
type OracleConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *OracleConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbOracle
	conn.BaseConn.defaultPort = 1521

	conn.SetProp("allow_bulk_import", "true")

	var instance Connection
	instance = conn
	conn.BaseConn.instance = &instance

	// set MAX_DECIMALS to import for numeric types
	os.Setenv("MAX_DECIMALS", "9")

	return conn.BaseConn.Init()
}

// BulkImportStream bulk import stream
func (conn *OracleConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	_, err = exec.LookPath("sqlldr")
	if err != nil {
		err = g.Error(err, "sqlldr not found in path. Using cursor...")
		return conn.BaseConn.InsertBatchStream(tableFName, ds)
	}

	if conn.GetProp("allow_bulk_import") != "true" {
		return conn.BaseConn.InsertBatchStream(tableFName, ds)
	}

	// needs to get columns to shape stream
	columns, err := conn.GetSQLColumns(tableFName)
	if err != nil {
		err = g.Error(err, "could not get column list")
		return
	}

	ds, err = ds.Shape(columns)
	if err != nil {
		err = g.Error(err, "could not shape stream")
		return
	}

	// logic to insert rows with values containing new line chars
	// addFilePath is additional rows to be inserted
	countTot, err := conn.SQLLoad(tableFName, ds)
	if err != nil {
		return 0, g.Error(err, "Error with SQLLoad")
	}

	return countTot, nil
}

// SQLLoad uses sqlldr to Bulk Import
// cat test1.csv | sqlldr system/oracle@oracle.host:1521/xe control=sqlldr.ctl log=/dev/stdout bad=/dev/stderr
// cannot import when newline in value. Need to scan for new lines.
func (conn *OracleConn) SQLLoad(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	var stderr, stdout bytes.Buffer
	url, err := dburl.Parse(conn.URL)
	if err != nil {
		err = g.Error(err, "Error dburl.Parse(conn.URL)")
		return
	}

	ctlPath := fmt.Sprintf(
		"/tmp/oracle.%d.%s.sqlldr.ctl",
		time.Now().Unix(),
		g.RandString(g.AlphaRunes, 3),
	)

	// write to ctlPath
	ctlStr := g.R(
		conn.BaseConn.GetTemplateValue("core.sqlldr"),
		"table", tableFName,
		"columns", conn.getColumnsString(ds),
	)
	err = ioutil.WriteFile(
		ctlPath,
		[]byte(ctlStr),
		0755,
	)
	if err != nil {
		err = g.Error(err, "Error writing to "+ctlPath)
		return
	}

	password, _ := url.User.Password()
	hostPort := url.Host
	sid := strings.ReplaceAll(url.Path, "/", "")
	credHost := fmt.Sprintf(
		"%s/%s@%s/%s", url.User.Username(),
		password, hostPort, sid,
	)

	proc := exec.Command(
		"sqlldr",
		credHost,
		"control="+ctlPath,
		"discardmax=0",
		"errors=0",
		"data=/dev/stdin",
		"log=/dev/stdout",
		"bad=/dev/stderr",
	)

	stdIn, pu := sqlLoadCsvReader(ds)
	proc.Stderr = &stderr
	proc.Stdout = &stdout
	proc.Stdin = stdIn

	// run and wait for finish
	cmdStr := strings.ReplaceAll(strings.Join(proc.Args, " "), credHost, "****")
	g.Trace(cmdStr)
	err = proc.Run()

	// Delete ctrl file
	defer os.Remove(ctlPath)

	if err != nil {
		err = g.Error(
			err,
			fmt.Sprintf(
				"Oracle Import Command:\n%s\n\nControl File:\n%s\n\nOracle Import Error:%s\n%s",
				cmdStr, ctlStr, stderr.String(), stdout.String(),
			),
		)
		return ds.Count, err
	}

	if ds.Err() != nil {
		return ds.Count, g.Error(ds.Err(), "context error")
	}

	// transformation to correctly post process quotes, newlines, and delimiter afterwards
	setCols := []string{}
	for c := range pu.cols {
		col := ds.Columns[c]
		expr := fmt.Sprintf(
			`REPLACE(REPLACE(%s, chr(13)), '~/N/~', chr(10))`,
			conn.Quote(col.Name),
		)
		setCols = append(
			setCols, fmt.Sprintf(`%s = %s`, conn.Quote(col.Name), expr),
		)
	}

	// do update statement if needed
	if len(setCols) > 0 {
		setColsStr := strings.Join(setCols, ", ")
		sql := fmt.Sprintf(`UPDATE %s SET %s`, tableFName, setColsStr)
		_, err = conn.Exec(sql)
		if err != nil {
			err = g.Error(err, "could not apply post update query")
			return
		}
	}
	return ds.Count, err
}

func (conn *OracleConn) getColumnsString(ds *iop.Datastream) string {
	columnsString := ""
	for _, col := range ds.Columns {
		expr := ""
		if col.Type == "datetime" || col.Type == "date" {
			expr = fmt.Sprintf(
				`"TO_DATE(:%s, 'YYYY-MM-DD HH24:MI:SS')"`,
				strings.ToUpper(col.Name),
			)
		} else if col.Type == "timestamp" || col.Type == "timestampz" {
			expr = fmt.Sprintf(
				`"TO_TIMESTAMP(:%s, 'YYYY-MM-DD HH24:MI:SS.FF3')"`,
				strings.ToUpper(col.Name),
			)
		} else if col.IsString() {
			expr = g.F("char(400000) NULLIF %s=BLANKS", col.Name)
		}
		columnsString += fmt.Sprintf("  %s %s,\n", col.Name, expr)
	}
	return strings.TrimSuffix(columnsString, ",\n")
}

// sqlLoadCsvReader creates a Reader with with a newline checker
// for SQLoad.
func sqlLoadCsvReader(ds *iop.Datastream) (*io.PipeReader, *struct{ cols map[int]int }) {
	pu := &struct{ cols map[int]int }{map[int]int{}}
	pipeR, pipeW := io.Pipe()

	go func() {
		c := uint64(0) // local counter
		w := csv.NewWriter(pipeW)

		b, err := w.Write(ds.GetFields())
		ds.AddBytes(int64(b))
		if err != nil {
			ds.Context.CaptureErr(g.Error(err, "Error writing ds.Fields"))
			ds.Context.Cancel()
			pipeW.Close()
		}

		for row0 := range ds.Rows {
			c++
			// convert to csv string
			row := make([]string, len(row0))
			for i, val := range row0 {
				if val == nil {
					row[i] = ""
					continue
				}

				valS := cast.ToString(val)
				if strings.Contains(valS, "\n") {
					valS = strings.ReplaceAll(valS, "\r", "")
					valS = strings.ReplaceAll(valS, "\n", `~/N/~`)
					pu.cols[i] = i
				}

				if ds.Columns[i].Type == "datetime" || ds.Columns[i].Type == "date" {
					// casting unsafely, but has been determined by ParseString
					// convert to Oracle Time format
					val = ds.Sp.CastValWithoutStats(i, val, ds.Columns[i].Type)
					valS = val.(time.Time).Format("2006-01-02 15:04:05")
				} else if ds.Columns[i].Type == "timestamp" {
					// convert to Oracle Timestamp format
					val = ds.Sp.CastValWithoutStats(i, val, ds.Columns[i].Type)
					valS = val.(time.Time).Format("2006-01-02 15:04:05.000")
				}
				row[i] = valS
			}

			b, err := w.Write(row)
			ds.AddBytes(int64(b))
			if err != nil {
				ds.Context.CaptureErr(g.Error(err, "Error w.Write(row)"))
				ds.Context.Cancel()
				break
			}
			w.Flush()

		}
		ds.SetEmpty()

		pipeW.Close()
	}()

	return pipeR, pu
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *OracleConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	sqlTempl := `
	MERGE INTO {tgt_table} tgt
	USING (SELECT * FROM {src_table}) src
	ON ({src_tgt_pk_equal})
	WHEN MATCHED THEN
		UPDATE SET {set_fields}
	WHEN NOT MATCHED THEN
		INSERT ({insert_fields}) VALUES ({src_fields})
	`

	sql = g.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"set_fields", upsertMap["set_fields"],
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", strings.ReplaceAll(upsertMap["placehold_fields"], "ph.", "src."),
	)

	return
}

// GenerateInsertStatement returns the proper INSERT statement
func (conn *OracleConn) GenerateInsertStatement(tableName string, fields []string, numRows int) string {

	values := make([]string, len(fields))
	qFields := make([]string, len(fields)) // quoted fields

	intos := []string{}
	c := 0
	for n := 0; n < numRows; n++ {
		for i, field := range fields {
			c++
			valField := field
			if len(valField) > 28 {
				valField = valField[:28]
			}
			values[i] = conn.bindVar(i+1, strings.ReplaceAll(valField, `"`, ""), n, c)
			qFields[i] = conn.Self().Quote(field)
		}

		// for Oracle
		intos = append(intos, g.R(
			"INTO {table} ({fields}) VALUES ({values})",
			"table", tableName,
			"fields", strings.Join(qFields, ", "),
			"values", strings.Join(values, ","),
		))
	}

	statement := g.R(
		`INSERT ALL {intosStr} SELECT 1 FROM DUAL`,
		"intosStr", strings.Join(intos, "\n"),
	)
	return statement
}
