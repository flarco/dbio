package database

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/flarco/dbio"
	"github.com/flarco/dbio/filesys"
	"github.com/spf13/cast"

	"github.com/flarco/g"

	"github.com/psanford/sqlite3vfs"
	"github.com/psanford/sqlite3vfshttp"
)

// SQLiteConn is a Google Big Query connection
type SQLiteConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *SQLiteConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbSQLite

	var instance Connection
	instance = conn
	conn.BaseConn.instance = &instance

	// handle S3 url
	if err := conn.setHttpURL(); err != nil {
		return g.Error(err, "could not set http url")
	}

	return conn.BaseConn.Init()
}

// GetURL returns the processed URL
func (conn *SQLiteConn) GetURL(newURL ...string) string {
	connURL := conn.BaseConn.URL
	if len(newURL) > 0 {
		connURL = newURL[0]
	}

	// fix scheme
	URL := strings.ReplaceAll(
		connURL,
		"sqlite://",
		"file:",
	)

	if httpURL := conn.GetProp("http_url"); httpURL != "" {
		URL = URL + "?vfs=httpvfs&mode=ro"
	}

	return URL
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *SQLiteConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	_, indexTable := SplitTableFullName(tgtTable)

	indexSQL := g.R(
		conn.GetTemplateValue("core.create_unique_index"),
		"index", strings.Join(pkFields, "_")+"_idx",
		"table", indexTable,
		"cols", strings.Join(pkFields, ", "),
	)

	_, err = conn.Exec(indexSQL)
	if err != nil {
		err = g.Error(err, "could not create unique index")
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

func (conn *SQLiteConn) setHttpURL() (err error) {

	httpURL := conn.GetProp("http_url")

	// handle S3 url
	if strings.HasPrefix(httpURL, "s3://") {

		expireDur := time.Minute
		if val := conn.GetProp("pre_signed_duration"); val != "" {
			mins := cast.ToInt64(val)
			expireDur = time.Duration(mins) * time.Minute
		}

		// need to generate pre-signed URL
		props := g.MapToKVArr(conn.Props())
		fs, err := filesys.NewFileSysClientFromURL(httpURL, props...)
		if err != nil {
			return g.Error(err, "could not connect to s3 bucket")
		}

		s3Fs, ok := fs.(*filesys.S3FileSysClient)
		if !ok {
			return g.Error("Could not convert to S3FileSysClient")
		}

		// check access
		paths, err := s3Fs.List(httpURL)
		if err != nil {
			return g.Error(err, "could not access s3 files")
		} else if len(paths) == 0 {
			return g.Error("Did not find any files with URL provided")
		}

		httpURL, err = s3Fs.GenerateS3PreSignedURL(httpURL, expireDur)
		if err != nil {
			return g.Error(err, "could not create Pre-Signed HTTP URL for s3 file")
		}
		conn.SetProp("http_url", httpURL)
	}

	if httpURL != "" {
		vfs := sqlite3vfshttp.HttpVFS{
			URL: httpURL,
			RoundTripper: &roundTripper{
				referer:   os.Getenv("DBIO_APP"),
				userAgent: os.Getenv("DBIO_APP"),
			},
		}

		err = sqlite3vfs.RegisterVFS("httpvfs", &vfs)
		if err != nil {
			return g.Error(err, "register vfs err")
		}
	}

	return nil
}

type roundTripper struct {
	referer   string
	userAgent string
}

func (rt *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if rt.referer != "" {
		req.Header.Set("Referer", rt.referer)
	}

	if rt.userAgent != "" {
		req.Header.Set("User-Agent", rt.userAgent)
	}

	tr := http.DefaultTransport

	if req.URL.Scheme == "file" {
		path := req.URL.Path
		root := filepath.Dir(path)
		base := filepath.Base(path)
		tr = http.NewFileTransport(http.Dir(root))
		req.URL.Path = base
	}

	return tr.RoundTrip(req)
}
