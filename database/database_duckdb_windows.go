//go:build windows
// +build windows

package database

import (
	"context"
	"database/sql"
	"path"
	"runtime"
	"strings"

	"github.com/flarco/dbio"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/samber/lo"
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
	URL := strings.ReplaceAll(connURL, "duckdb://", "")
	return URL
}

// EnsureBinDuckDB ensures duckdb binary exists
// if missing, downloads and uses
func EnsureBinDuckDB(version string) (binPath string, err error) {
	folderPath := path.Join(g.UserHomeDir(), "duckdb")
	extension := lo.Ternary(runtime.GOOS == "windows", ".exe", "")
	binPath = path.Join(g.UserHomeDir(), "duckdb", "duckdb"+extension)
	found := g.PathExists(binPath)

	// TODO: check version if found
	if found {

	}

	if !found {
		// we need to download it ourselves
		var downloadURL string
		zipPath := path.Join(g.UserHomeDir(), "duckdb.zip")

		switch runtime.GOOS {
		case "windows":
			downloadURL = "https://github.com/duckdb/duckdb/releases/download/v0.7.0/duckdb_cli-windows-amd64.zip"
		case "darwin":
			downloadURL = "https://github.com/duckdb/duckdb/releases/download/v0.7.0/duckdb_cli-osx-universal.zip"
		case "linux":
			downloadURL = "https://github.com/duckdb/duckdb/releases/download/v0.7.0/duckdb_cli-linux-amd64.zip"
		default:
			return "", g.Error("OS %s not handled", runtime.GOOS)
		}

		g.Info("downloading duckdb %s for %s", version, runtime.GOOS)
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

	return binPath, nil
}

// ExecContext runs a sql query with context, returns `error`
func (conn *DuckDbConn) ExecMultiContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	return conn.ExecContext(ctx, sql, args...)
}

func (conn *DuckDbConn) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	return
}

func (conn *DuckDbConn) StreamRowsContext(ctx context.Context, sql string, options ...map[string]interface{}) (ds *iop.Datastream, err error) {
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
	return count, nil
}
