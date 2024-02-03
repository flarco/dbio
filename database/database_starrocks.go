package database

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/flarco/dbio"
	"github.com/flarco/dbio/filesys"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g/net"
	"github.com/samber/lo"
	"github.com/spf13/cast"

	"github.com/flarco/g"
	"github.com/xo/dburl"
)

// StarRocksConn is a StarRocks connection
type StarRocksConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *StarRocksConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbStarRocks
	conn.BaseConn.defaultPort = 9030

	// Turn off Bulk export for now
	// the LoadDataOutFile needs special circumstances
	conn.BaseConn.SetProp("allow_bulk_export", "false")

	// InsertBatchStream is faster than LoadDataInFile
	if conn.BaseConn.GetProp("allow_bulk_import") == "" {
		conn.BaseConn.SetProp("allow_bulk_import", "false")
	}

	var instance Connection
	instance = conn
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// GetURL returns the processed URL
func (conn *StarRocksConn) GetURL(newURL ...string) string {
	connURL := conn.BaseConn.URL
	if len(newURL) > 0 {
		connURL = newURL[0]
	}

	connURL = strings.Replace(connURL, "starrocks://", "mysql://", 1)
	u, err := dburl.Parse(connURL)
	if err != nil {
		g.LogError(err, "could not parse StarRocks URL")
		return connURL
	}

	// add parseTime
	u.Query().Add("parseTime", "true")

	return u.DSN
}

// InsertBatchStream inserts a stream into a table in batch
func (conn *StarRocksConn) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	var columns iop.Columns
	batchSize := cast.ToInt(conn.GetTemplateValue("variable.batch_values")) / len(ds.Columns)
	context := conn.Context()

	// in case schema change is needed, cannot alter while inserting
	mux := ds.Context.Mux
	if df := ds.Df(); df != nil {
		mux = df.Context.Mux
	}

	insertBatch := func(bColumns iop.Columns, rows [][]interface{}) error {
		defer context.Wg.Write.Done()

		mux.Lock()
		defer mux.Unlock()

		insFields, err := conn.ValidateColumnNames(columns.Names(), bColumns.Names(), true)
		if err != nil {
			return g.Error(err, "columns mismatch")
		}
		valuesSlice := []string{}
		valCount := 0
		for _, row := range rows {
			rowVals := lo.Map(row, func(val any, i int) string {
				valCount++
				newVal := ds.Sp.CastToString(i, val, ds.Columns[i].Type)
				newVal = strings.ReplaceAll(newVal, `"`, `""`)
				newVal = strings.ReplaceAll(newVal, `\`, `\\`)
				switch {
				case ds.Columns[i].Type.IsNumber():
					return newVal
				case ds.Columns[i].Type.IsBool():
					return newVal
				default:
					return `"` + newVal + `"`
				}
			})
			valuesSlice = append(valuesSlice, "("+strings.Join(rowVals, ", ")+")")
		}

		sql := g.R(
			"INSERT OVERWRITE {table} ({fields}) VALUES {values} "+noDebugKey,
			"table", tableFName,
			"fields", strings.Join(insFields, ", "),
			"values", strings.Join(valuesSlice, ",\n"),
		)
		_, err = conn.ExecContext(ds.Context.Ctx, sql)
		if err != nil {
			batchErrStr := g.F("Batch Size: %d rows x %d cols = %d (%d vals)", len(rows), len(bColumns), len(rows)*len(bColumns), valCount)
			g.Trace(g.F(
				"%s\n%s \n%s \n%s",
				err.Error(), batchErrStr,
				fmt.Sprintf("Insert SQL: %s", sql),
				fmt.Sprintf("\n\nRows: %#v", lo.Map(rows, func(row []any, i int) string {
					return g.F("len(row[%d]) = %d", i, len(row))
				})),
			))
			context.CaptureErr(err)
			context.Cancel()
			return context.Err()
		}

		return nil
	}

	batchRows := [][]any{}
	var batch *iop.Batch

	for batch = range ds.BatchChan {

		if batch.ColumnsChanged() || batch.IsFirst() {
			// make sure fields match
			mux.Lock()
			columns, err = conn.GetColumns(tableFName, batch.Columns.Names()...)
			if err != nil {
				err = g.Error(err, "could not get column list")
				return
			}
			mux.Unlock()

			// err = batch.Shape(columns)
			// if err != nil {
			// 	return count, g.Error(err, "could not shape batch stream")
			// }
		}

		for row := range batch.Rows {
			batchRows = append(batchRows, row)
			count++
			if len(batchRows) == batchSize {
				context.Wg.Write.Add()
				select {
				case <-context.Ctx.Done():
					return count, context.Err()
				case <-ds.Context.Ctx.Done():
					return count, ds.Context.Err()
				default:
					insertBatch(batch.Columns, batchRows)
				}

				// reset
				batchRows = [][]interface{}{}
			}
		}

	}

	// remaining batch
	if len(batchRows) > 0 {
		g.Trace("remaining batchSize %d", len(batchRows))
		context.Wg.Write.Add()
		err = insertBatch(batch.Columns, batchRows)
		if err != nil {
			return count - cast.ToUint64(len(batchRows)), g.Error(err, "insertBatch")
		}
	}

	return
}

// GenerateDDL genrate a DDL based on a dataset
func (conn *StarRocksConn) GenerateDDL(tableFName string, data iop.Dataset, temporary bool) (string, error) {
	pkCols := data.Columns.GetKeys(iop.PrimaryKey)
	if len(pkCols) == 0 {
		return "", g.Error("did not provide primary key for creating StarRocks table")
	}

	sql, err := conn.BaseConn.GenerateDDL(tableFName, data, temporary)
	if err != nil {
		return sql, g.Error(err)
	}

	// replace keys
	pkColNames := lo.Map(pkCols.Names(), func(col string, i int) string { return conn.Quote(col) })
	sql = strings.ReplaceAll(sql, "{primary_key}", strings.Join(pkColNames, ", "))

	return sql, nil
}

// BulkImportFlow inserts a flow of streams into a table.
func (conn *StarRocksConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	defer df.CleanUp()

	if feURL := conn.GetProp("fe_url"); feURL != "" {
		return conn.StreamLoad(feURL, tableFName, df)
	}

	g.Warn("Using INSERT mode which is meant for small datasets. Please set the `fe_url` for loading large datasets via Stream Load mode. See https://docs.slingdata.io/connections/database-connections/starrocks")

	return conn.BaseConn.BulkImportFlow(tableFName, df)
}

// StreamLoad bulk loads
// https://docs.starrocks.io/docs/loading/StreamLoad/
// https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/STREAM_LOAD/
func (conn *StarRocksConn) StreamLoad(feURL, tableFName string, df *iop.Dataflow) (count uint64, err error) {

	connURL, err := net.NewURL(conn.BaseConn.URL)
	if err != nil {
		return count, g.Error(err, "invalid conn url")
	}

	user := connURL.U.User.Username()
	password, _ := connURL.U.User.Password()

	// if no user / password provided
	applyCreds := func(u *url.URL) string {
		urlStr := u.String()
		uUser := u.User.Username()
		uPassword, _ := u.User.Password()

		noCredPrefix := g.F("%s://%s", u.Scheme, u.Hostname())
		if strings.HasPrefix(urlStr, noCredPrefix) && uUser == "" {
			urlStr = strings.ReplaceAll(
				urlStr,
				u.Hostname(),
				g.F("%s:%s@%s", user, url.QueryEscape(password), u.Hostname()),
			)
		} else if uUser != "" && uPassword == "" && password != "" {
			urlStr = strings.ReplaceAll(
				urlStr,
				g.F("%s@%s", user, u.Hostname()),
				g.F("%s:%s@%s", user, url.QueryEscape(password), u.Hostname()),
			)
		}
		return urlStr
	}

	fu, err := net.NewURL(feURL)
	if err != nil {
		return count, g.Error(err, "invalid url for FE")
	}

	table, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return count, g.Error(err, "could not parse table: %s", tableFName)
	}

	g.Info("importing into StarRocks via stream load")

	fs, err := filesys.NewFileSysClient(dbio.TypeFileLocal, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for Local")
		return
	}

	// set format to JSON
	fs.SetProp("format", "json")
	fs.SetProp("file_max_rows", "500000")
	fs.SetProp("file_max_bytes", "50000000")

	localPath := path.Join(getTempFolder(), "starrocks", table.Schema, table.Name, g.NowFileStr())
	err = filesys.Delete(fs, localPath)
	if err != nil {
		return count, g.Error(err, "Could not Delete: "+localPath)
	}

	fileReadyChn := make(chan filesys.FileReady, 10)
	go func() {
		_, err = fs.WriteDataflowReady(df, localPath, fileReadyChn)
		if err != nil {
			g.LogError(err, "error writing dataflow to local storage: "+localPath)
			df.Context.CaptureErr(g.Error(err, "error writing dataflow to local storage: "+localPath))
			df.Context.Cancel()
			return
		}
	}()

	headers := map[string]string{
		"Expect":  "100-continue",
		"columns": strings.Join(df.Columns.Names(), ", "),

		// CSV doesn't work well for multi-line values
		// "format":           "CSV",
		// "column_separator": ",",
		// "enclose":          `"`,
		// "escape":           `"`,

		"format":            "JSON",
		"strip_outer_array": "true",
	}

	loadFromLocal := func(localFile filesys.FileReady, tableFName string) {
		defer conn.Context().Wg.Write.Done()
		g.Debug("loading %s [%s] %s", localFile.URI, humanize.Bytes(cast.ToUint64(localFile.BytesW)), localFile.BatchID)

		defer os.Remove(localFile.URI)
		reader, err := os.Open(localFile.URI)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "could not open temp file: %s", localFile.URI))
		}

		apiURL := strings.TrimSuffix(applyCreds(fu.U), "/") + g.F("/api/%s/%s/_stream_load", table.Schema, table.Name)
		resp, respBytes, err := net.ClientDo(http.MethodPut, apiURL, reader, headers)
		if resp != nil && resp.StatusCode >= 300 && resp.StatusCode <= 399 {
			redirectUrl, _ := resp.Location()
			if redirectUrl != nil {
				g.Debug("FE url redirected to %s://%s", redirectUrl.Scheme, redirectUrl.Host)
				redirectUrlStr := strings.ReplaceAll(redirectUrl.String(), "127.0.0.1", fu.U.Hostname())
				redirectUrl, _ = url.Parse(redirectUrlStr)
				reader, _ = os.Open(localFile.URI) // re-open file since it would be closed
				_, respBytes, err = net.ClientDo(http.MethodPut, applyCreds(redirectUrl), reader, headers)
			}
		}

		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Error loading from %s into %s\n%s", localFile.URI, tableFName, string(respBytes)))
			df.Context.Cancel()
		} else {
			g.Debug("stream-load completed for %s => %s", localFile.URI, string(respBytes))
		}
	}

	for localFile := range fileReadyChn {
		conn.Context().Wg.Write.Add()
		go loadFromLocal(localFile, tableFName)
	}

	conn.Context().Wg.Write.Wait()
	if df.Err() != nil {
		return df.Count(), g.Error(df.Err(), "Error importing to StarRocks")
	}

	return df.Count(), nil
}
