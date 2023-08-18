package database

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/flarco/dbio"
	"github.com/flarco/dbio/filesys"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

// DuckDbConn is a Duck DB connection
type DuckDbConn struct {
	BaseConn
	URL string
}

var DuckDbVersion = "0.8.1"

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

func (conn *DuckDbConn) Connect(timeOut ...int) (err error) {
	conn.SetProp("connected", "true")
	return nil
}

// EnsureBinDuckDB ensures duckdb binary exists
// if missing, downloads and uses
func EnsureBinDuckDB() (binPath string, err error) {
	if version := os.Getenv("DUCKDB_VERSION"); version != "" {
		DuckDbVersion = version
	}

	folderPath := path.Join(g.UserHomeDir(), "duckdb", DuckDbVersion)
	extension := lo.Ternary(runtime.GOOS == "windows", ".exe", "")
	binPath = path.Join(folderPath, "duckdb"+extension)
	found := g.PathExists(binPath)

	checkVersion := func() (bool, error) {

		out, err := exec.Command(binPath, "-version").Output()
		if err != nil {
			return false, g.Error(err, "could not get version for duckdb")
		}

		if strings.HasPrefix(string(out), "v"+DuckDbVersion) {
			return true, nil
		}

		return false, nil
	}

	// TODO: check version if found
	if found {
		ok, err := checkVersion()
		if err != nil {
			return "", g.Error(err, "error checking version for duckdb")
		}
		found = ok // so it can re-download if mismatch
	}

	if !found {
		// we need to download it ourselves
		var downloadURL string
		zipPath := path.Join(g.UserHomeDir(), "duckdb.zip")

		switch runtime.GOOS + "/" + runtime.GOARCH {

		case "windows/amd64":
			downloadURL = "https://github.com/duckdb/duckdb/releases/download/v{version}/duckdb_cli-windows-amd64.zip"

		case "windows/386":
			downloadURL = "https://github.com/duckdb/duckdb/releases/download/v{version}/duckdb_cli-windows-i386.zip"

		case "darwin/386", "darwin/arm", "darwin/arm64":
			downloadURL = "https://github.com/duckdb/duckdb/releases/download/v{version}/duckdb_cli-osx-universal.zip"

		case "linux/386":
			downloadURL = "https://github.com/duckdb/duckdb/releases/download/v{version}/duckdb_cli-linux-i386.zip"

		case "linux/amd64":
			downloadURL = "https://github.com/duckdb/duckdb/releases/download/v{version}/duckdb_cli-linux-amd64.zip"

		default:
			return "", g.Error("OS %s/%s not handled", runtime.GOOS, runtime.GOARCH)
		}

		downloadURL = g.R(downloadURL, "version", DuckDbVersion)

		g.Info("downloading duckdb %s for %s/%s", DuckDbVersion, runtime.GOOS, runtime.GOARCH)
		err = net.DownloadFile(downloadURL, zipPath)
		if err != nil {
			return "", g.Error(err, "Unable to download duckdb binary")
		}

		paths, err := iop.Unzip(zipPath, folderPath)
		if err != nil {
			return "", g.Error(err, "Error unzipping duckdb zip")
		}

		if !g.PathExists(binPath) {
			return "", g.Error("cannot find %s, paths are: %s", binPath, g.Marshal(paths))
		}
	}

	_, err = checkVersion()
	if err != nil {
		return "", g.Error(err, "error checking version for duckdb")
	}

	return binPath, nil
}

// ExecContext runs a sql query with context, returns `error`
func (conn *DuckDbConn) ExecMultiContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	return conn.ExecContext(ctx, sql, args...)
}

type duckDbResult struct {
	TotalRows uint64
	res       driver.Result
}

func (conn *DuckDbConn) getCmd(sql string) (cmd *exec.Cmd, sqlPath string, err error) {

	bin, err := EnsureBinDuckDB()
	if err != nil {
		return cmd, "", g.Error(err, "could not get duckdb binary")
	}

	sqlPath, err = writeTempSQL(sql)
	if err != nil {
		return cmd, "", g.Error(err, "could not create temp sql file for duckdb")
	}

	dbPathU, err := net.NewURL(conn.BaseConn.URL)
	if err != nil {
		os.Remove(sqlPath)
		err = g.Error(err, "could not get sqlite file path")
		return
	}
	dbPath := strings.ReplaceAll(conn.GetURL(), "?"+dbPathU.U.RawQuery, "")

	cmd = exec.Command(bin)
	cmd.Args = append(cmd.Args, dbPath, g.F(`.read %s`, sqlPath))

	return cmd, sqlPath, nil
}

func (r duckDbResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r duckDbResult) RowsAffected() (int64, error) {
	return cast.ToInt64(r.TotalRows), nil
}

func (conn *DuckDbConn) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	var stderr bytes.Buffer

	if len(args) > 0 {
		for i, arg := range args {
			ph := g.F("$%d", i+1)

			switch val := arg.(type) {
			case int, int64, int8, int32, int16:
				sql = strings.Replace(sql, ph, fmt.Sprintf("%d", val), 1)
			case float32, float64:
				sql = strings.Replace(sql, ph, fmt.Sprintf("%f", val), 1)
			case time.Time:
				sql = strings.Replace(sql, ph, fmt.Sprintf("'%s'", val.Format("2006-01-02 15:04:05")), 1)
			case nil:
				sql = strings.Replace(sql, ph, "NULL", 1)
			default:
				v := strings.ReplaceAll(cast.ToString(val), "\n", "\\n")
				v = strings.ReplaceAll(v, "'", "\\'")
				sql = strings.Replace(sql, ph, fmt.Sprintf("'%s'", v), 1)
			}
		}
	}

	cmd, sqlPath, err := conn.getCmd(sql)
	if err != nil {
		return result, g.Error(err, "could not get cmd duckdb")
	}

	if strings.Contains(sql, noDebugKey) {
		g.Trace(sql)
	} else {
		g.DebugLow(CleanSQL(conn, sql), args...)
	}

	cmd.Stderr = &stderr

	conn.Context().Mux.Lock()
	out, err := cmd.Output()
	time.Sleep(100 * time.Millisecond) // so that cmd releases process
	conn.Context().Mux.Unlock()

	os.Remove(sqlPath) // delete sql temp file

	if err != nil {
		errText := g.F("could not exec SQL for duckdb: %s\n%s\n%s", string(out), stderr.String(), sql)
		if strings.Contains(errText, "version number") {
			errText = "Please set the DuckDB version with environment variable DUCKDB_VERSION. Example: DUCKDB_VERSION=0.6.0\n" + errText
		}
		return result, g.Error(err, errText)
	} else if exitCode := cmd.ProcessState.ExitCode(); exitCode != 0 {
		errText := g.F("could not exec SQL for duckdb: %s\n%s\n%s", string(out), stderr.String(), sql)
		if strings.Contains(errText, "version number") {
			errText = "Please set the DuckDB version with environment variable DUCKDB_VERSION. Example: DUCKDB_VERSION=0.6.0\n" + errText
		}
		err = g.Error("exit code is %d", exitCode)
		return result, g.Error(err, errText)

	}

	result = duckDbResult{}

	return
}

func (conn *DuckDbConn) StreamRowsContext(ctx context.Context, sql string, options ...map[string]interface{}) (ds *iop.Datastream, err error) {

	cmd, sqlPath, err := conn.getCmd(sql)
	if err != nil {
		return ds, g.Error(err, "could not get cmd duckdb")
	}
	defer func() { os.Remove(sqlPath) }()

	if strings.Contains(sql, noDebugKey) {
		g.Trace(sql)
	} else {
		g.DebugLow(sql)
	}

	cmd.Args = append(cmd.Args, "-csv")

	conn.Context().Mux.Lock()

	stdOutReader, err := cmd.StdoutPipe()
	if err != nil {
		return ds, g.Error(err, "could not get stdout for duckdb")
	}

	stdErrReader, err := cmd.StderrPipe()
	if err != nil {
		return ds, g.Error(err, "could not get stderr for duckdb")
	}

	err = cmd.Start()
	if err != nil {
		return ds, g.Error(err, "could not exec SQL for duckdb")
	}
	ds = iop.NewDatastream(iop.Columns{})
	ds.Defer(func() { conn.Context().Mux.Unlock() })

	err = ds.ConsumeCsvReader(stdOutReader)
	if err != nil {
		return ds, g.Error(err, "could not read output stream")
	}

	errOut, err := io.ReadAll(stdErrReader)
	if err != nil {
		return ds, g.Error(err, "could not read error stream")
	} else if errOutS := string(errOut); errOutS != "" {
		return ds, g.Error(errOutS)
	}

	return
}

// Close closes the connection
func (conn *DuckDbConn) Close() error {
	return nil
}

// InsertBatchStream inserts a stream into a table in batch
func (conn *DuckDbConn) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BulkImportStream(tableFName, ds)
}

// InsertStream demonstrates loading data into a BigQuery table using a file on the local filesystem.
func (conn *DuckDbConn) InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BulkImportStream(tableFName, ds)
}

// BulkImportStream inserts a stream into a table
func (conn *DuckDbConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	defer ds.Close()
	var columns iop.Columns

	// FIXME: batching works better when transactions are closed
	// seems, when the appender is closed, the transaction is closed as well
	conn.Commit()

	table, err := ParseTableName(tableFName, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not get table name for imoprt")
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

		// write to temp CSV
		tempDir := strings.TrimRight(strings.TrimRight(os.TempDir(), "/"), "\\")
		csvPath := path.Join(tempDir, g.NewTsID("sqlite.temp")+".csv")

		// set header false
		cfgMap := ds.GetConfig()
		cfgMap["header"] = "false"
		cfgMap["delimiter"] = ","
		cfgMap["datetime_format"] = "2006-01-02 15:04:05.000000-07:00"
		ds.SetConfig(cfgMap)

		if runtime.GOOS == "windows" {
			fs, err := filesys.NewFileSysClient(dbio.TypeFileLocal)
			if err != nil {
				err = g.Error(err, "could not obtain client for temp file")
				return 0, err
			}

			_, err = fs.Write("file://"+csvPath, ds.NewCsvReader(0, 0))
			if err != nil {
				err = g.Error(err, "could not write to temp file")
				return 0, err
			}
		} else {
			csvPath = "/dev/stdin"
		}

		columnNames := lo.Map(columns.Names(true, true), func(col string, i int) string {
			name, _ := ParseColumnName(col, conn.Type)
			return name
		})

		sqlLines := []string{
			// g.F(".separator ,"),
			// g.F(".import --csv %s %s", csvPath, table.Name),
			g.F(`insert into %s (%s) select * from read_csv('%s', delim=',', header=False, auto_detect=True);`, table.Name, strings.Join(columnNames, ", "), csvPath),
		}

		sql := strings.Join(sqlLines, ";\n")
		cmd, sqlPath, err := conn.getCmd(sql)
		if err != nil {
			os.Remove(csvPath)
			return count, g.Error(err, "could not get cmd duckdb")
		}

		if strings.Contains(sql, noDebugKey) {
			g.Trace(sql)
		} else {
			g.DebugLow(sql)
		}

		if csvPath == "/dev/stdin" {
			cmd.Stdin = ds.NewCsvReader(0, 0)
		}

		var stderr bytes.Buffer
		cmd.Stderr = &stderr

		conn.Context().Mux.Lock()
		out, err := cmd.Output()
		conn.Context().Mux.Unlock()
		time.Sleep(100 * time.Millisecond) // so that cmd releases process

		if csvPath != "/dev/stdin" {
			os.Remove(csvPath) // delete csv file
		}
		os.Remove(sqlPath) // delete sql temp file

		stderrVal := stderr.String()
		if err != nil {
			return count, g.Error(err, "could not ingest for duckdb: %s\n%s", string(out), stderrVal)
		} else if strings.Contains(stderrVal, "expected") {
			return count, g.Error("could not ingest for duckdb: %s\n%s", string(out), stderrVal)
		}
	}

	return ds.Count, nil
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
