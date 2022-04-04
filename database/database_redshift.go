package database

import (
	"fmt"
	"strings"

	"github.com/flarco/dbio"

	"github.com/flarco/dbio/filesys"

	"github.com/dustin/go-humanize"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/cast"
)

// RedshiftConn is a Redshift connection
type RedshiftConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *RedshiftConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbRedshift
	conn.BaseConn.defaultPort = 5439

	var instance Connection
	instance = conn
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

func (conn *RedshiftConn) ConnString() string {
	return strings.ReplaceAll(conn.URL, "redshift://", "postgres://")
}

func isRedshift(URL string) (isRs bool) {
	db, err := sqlx.Open("postgres", URL)
	if err != nil {
		return isRs
	}
	res, err := db.Queryx("select version() v")
	if err != nil {
		return isRs
	}
	res.Next()
	row, err := res.SliceScan()
	if err != nil {
		return isRs
	}
	if strings.Contains(strings.ToLower(cast.ToString(row[0])), "redshift") {
		isRs = true
	}
	db.Close()
	return isRs
}

// Unload unloads a query to S3
func (conn *RedshiftConn) Unload(sqls ...string) (s3Path string, err error) {

	if conn.GetProp("AWS_BUCKET") == "" {
		return "", g.Error("need to set AWS_BUCKET")
	}

	AwsID := conn.GetProp("AWS_ACCESS_KEY_ID")
	AwsAccessKey := conn.GetProp("AWS_SECRET_ACCESS_KEY")

	g.Info("unloading from redshift to s3")
	unload := func(sql string, s3PathPart string) {

		defer conn.Context().Wg.Write.Done()

		sql = strings.ReplaceAll(strings.ReplaceAll(sql, "\n", " "), "'", "''")

		unloadSQL := g.R(
			conn.template.Core["copy_to_s3"],
			"sql", sql,
			"s3_path", s3PathPart,
			"aws_access_key_id", AwsID,
			"aws_secret_access_key", AwsAccessKey,
			"parallel", conn.GetProp("DBIO_PARALLEL"),
		)
		_, err = conn.Exec(unloadSQL)
		if err != nil {
			cleanSQL := strings.ReplaceAll(unloadSQL, AwsID, "*****")
			cleanSQL = strings.ReplaceAll(cleanSQL, AwsAccessKey, "*****")
			err = g.Error(err, fmt.Sprintf("SQL Error for %s:\n%s", s3PathPart, cleanSQL))
			conn.Context().CaptureErr(err)
		}

	}

	s3Fs, err := filesys.NewFileSysClient(dbio.TypeFileS3, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Unable to create S3 Client")
		return
	}

	s3Path = fmt.Sprintf("s3://%s/%s/stream/%s.csv", conn.GetProp("AWS_BUCKET"), filePathStorageSlug, cast.ToString(g.Now()))

	s3Fs.Delete(s3Path)
	for i, sql := range sqls {
		s3PathPart := fmt.Sprintf("%s/u%02d-", s3Path, i+1)
		conn.Context().Wg.Write.Add()
		go unload(sql, s3PathPart)
	}

	conn.Context().Wg.Write.Wait()
	err = conn.Context().Err()

	if err == nil {
		g.Debug("Unloaded to %s", s3Path)
	}

	return s3Path, err
}

// BulkExportStream reads in bulk
func (conn *RedshiftConn) BulkExportStream(sql string) (ds *iop.Datastream, err error) {

	df, err := conn.BulkExportFlow(sql)
	if err != nil {
		return ds, g.Error(err, "Could not export: \n"+sql)
	}

	return iop.MergeDataflow(df), nil
}

// BulkExportFlow reads in bulk
func (conn *RedshiftConn) BulkExportFlow(sqls ...string) (df *iop.Dataflow, err error) {

	columns, err := conn.GetSQLColumns(sqls...)
	if err != nil {
		err = g.Error(err, "Could not get columns.")
		return
	}

	s3Path, err := conn.Unload(sqls...)
	if err != nil {
		err = g.Error(err, "Could not unload.")
		return
	}

	fs, err := filesys.NewFileSysClient(dbio.TypeFileS3, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for S3")
		return
	}

	df, err = fs.ReadDataflow(s3Path)
	if err != nil {
		err = g.Error(err, "Could not read S3 Path for UNLOAD: "+s3Path)
		return
	}
	df.SetColumns(columns)
	df.Inferred = true
	df.Defer(func() { fs.Delete(s3Path) })

	return
}

// BulkImportFlow inserts a flow of streams into a table.
// For redshift we need to create CSVs in S3 and then use the COPY command.
func (conn *RedshiftConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	defer df.CleanUp()

	settingMppBulkImportFlow(conn, iop.GzipCompressorType)
	if conn.GetProp("AWS_BUCKET") == "" {
		return count, g.Error("Need to set 'AWS_BUCKET' to copy to redshift")
	}

	s3Path := fmt.Sprintf(
		"s3://%s/%s/%s",
		conn.GetProp("AWS_BUCKET"),
		filePathStorageSlug,
		tableFName,
	)

	s3Fs, err := filesys.NewFileSysClient(dbio.TypeFileS3, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for S3")
		return
	}

	err = s3Fs.Delete(s3Path)
	if err != nil {
		return count, g.Error(err, "Could not Delete: "+s3Path)
	}

	df.Defer(func() { s3Fs.Delete(s3Path) }) // cleanup

	g.Info("writing to s3 for redshift import")
	bw, err := s3Fs.WriteDataflow(df, s3Path)
	if err != nil {
		return df.Count(), g.Error(err, "error writing to s3")
	}
	g.Debug("total written: %s to %s", humanize.Bytes(cast.ToUint64(bw)), s3Path)

	_, err = conn.CopyFromS3(tableFName, s3Path)
	if err != nil {
		return df.Count(), g.Error(err, "error copygin into redshift from s3")
	}

	return df.Count(), nil
}

// BulkImportStream inserts a stream into a table.
// For redshift we need to create CSVs in S3 and then use the COPY command.
func (conn *RedshiftConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	df, err := iop.MakeDataFlow(ds)
	if err != nil {
		err = g.Error(err, "Could not MakeDataFlow")
		return
	}

	return conn.BulkImportFlow(tableFName, df)
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *RedshiftConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	srcTgtPkEqual := strings.ReplaceAll(
		upsertMap["src_tgt_pk_equal"], "src.", srcTable+".",
	)
	srcTgtPkEqual = strings.ReplaceAll(
		srcTgtPkEqual, "tgt.", tgtTable+".",
	)

	sqlTempl := `
	DELETE FROM {tgt_table}
	USING {src_table}
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
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", upsertMap["src_fields"],
		"src_tgt_pk_equal", srcTgtPkEqual,
	)
	return
}

// CopyFromS3 uses the COPY INTO Table command from AWS S3
func (conn *RedshiftConn) CopyFromS3(tableFName, s3Path string) (count uint64, err error) {
	AwsID := conn.GetProp("AWS_ACCESS_KEY_ID")
	AwsAccessKey := conn.GetProp("AWS_SECRET_ACCESS_KEY")
	if AwsID == "" || AwsAccessKey == "" {
		err = g.Error("Need to set 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' to copy to snowflake from S3")
		return
	}

	g.Info("copying into redshift from s3")
	g.Debug("url: " + s3Path)
	sql := g.R(
		conn.template.Core["copy_from_s3"],
		"tgt_table", tableFName,
		"s3_path", s3Path,
		"aws_access_key_id", AwsID,
		"aws_secret_access_key", AwsAccessKey,
	)
	_, err = conn.Exec(sql)
	if err != nil {
		return 0, g.Error(err, "SQL Error:\n"+CleanSQL(conn, sql))
	}

	return 0, nil
}
