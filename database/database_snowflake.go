package database

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"

	"github.com/flarco/dbio"

	"github.com/flarco/dbio/filesys"
	"github.com/snowflakedb/gosnowflake"

	"github.com/dustin/go-humanize"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/spf13/cast"
)

// SnowflakeConn is a Snowflake connection
type SnowflakeConn struct {
	BaseConn
	URL        string
	Warehouse  string
	CopyMethod string
}

// Init initiates the object
func (conn *SnowflakeConn) Init() error {
	var sfLog = gosnowflake.GetLogger()
	sfLog.SetOutput(ioutil.Discard)

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbSnowflake
	conn.CopyMethod = "STAGE"

	if s := conn.GetProp("schema"); s != "" {
		conn.SetProp("schema", s)
	}

	if m := conn.GetProp("CopyMethod"); m != "" {
		conn.CopyMethod = conn.GetProp("CopyMethod")
	}

	var instance Connection
	instance = conn
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()

}

func (conn *SnowflakeConn) ConnString() string {
	connString := conn.URL

	if m := conn.GetProp("CopyMethod"); m != "" {
		connString = strings.ReplaceAll(connString, "CopyMethod="+m, "")
	}

	if strings.HasSuffix(connString, "?") {
		connString = connString[0 : len(connString)-1]
	}

	connString = strings.ReplaceAll(
		connString,
		"snowflake://",
		"",
	)

	return connString
}

// Connect connects to the database
func (conn *SnowflakeConn) Connect(timeOut ...int) error {
	err := conn.BaseConn.Connect(timeOut...)
	if err != nil {
		return err
	}
	if cast.ToBool(conn.GetProp("POOL_USED")) {
		return nil
	}

	// Get Warehouse
	data, err := conn.Query("SHOW WAREHOUSES" + noTraceKey)
	if err != nil {
		return g.Error(err, "could not SHOW WAREHOUSES")
	}
	if len(data.Rows) > 0 {
		conn.SetProp("warehouse", cast.ToString(data.Rows[0][0]))
	}

	if conn.GetProp("schema") != "" {
		_, err = conn.Exec("USE SCHEMA " + conn.GetProp("schema") + noTraceKey)
	}
	if conn.GetProp("role") != "" {
		_, err = conn.Exec("USE ROLE " + conn.GetProp("role") + noTraceKey)
	}
	return err
}

func (conn *SnowflakeConn) getOrCreateStage(schema string) string {
	if conn.GetProp("internalStage") == "" {
		defStaging := "sling_staging"
		if schema == "" {
			schema = conn.GetProp("schema")
		}
		conn.Exec("USE SCHEMA " + schema + noTraceKey)
		_, err := conn.Exec("CREATE STAGE IF NOT EXISTS " + defStaging + noTraceKey)
		if err != nil {
			g.Warn("Tried to create Internal Snowflake Stage but failed.\n" + g.ErrMsgSimple(err))
			return ""
		}
		conn.SetProp("schema", schema)
		conn.SetProp("internalStage", defStaging)
	}
	return conn.GetProp("internalStage")
}

// BulkExportFlow reads in bulk
func (conn *SnowflakeConn) BulkExportFlow(sqls ...string) (df *iop.Dataflow, err error) {

	df = iop.NewDataflow()

	columns, err := conn.GetSQLColumns(sqls...)
	if err != nil {
		err = g.Error(err, "Could not get columns.")
		return
	}

	filePath := ""

	switch conn.CopyMethod {
	case "AZURE":
		filePath, err = conn.CopyToAzure(sqls...)
		if err != nil {
			err = g.Error(err, "Could not copy to S3.")
			return
		}
	case "AWS":
		filePath, err = conn.CopyToS3(sqls...)
		if err != nil {
			err = g.Error(err, "Could not copy to S3.")
			return
		}
	// case "STAGE":
	// 	// TODO: This is not working, buggy driver. Use SQL Rows stream
	// 	if conn.getOrCreateStage("") != "" {
	// 		filePath, err = conn.UnloadViaStage(sqls...)
	// 	} else {
	// 		g.Warn("could not get or create stage. Using cursor stream.")
	// 		return conn.BaseConn.BulkExportFlow(sqls...)
	// 	}
	default:
		return conn.BaseConn.BulkExportFlow(sqls...)
	}

	fs, err := filesys.NewFileSysClientFromURL(filePath, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Could not get fs client")
		return
	}

	df, err = fs.ReadDataflow(filePath)
	if err != nil {
		err = g.Error(err, "Could not read "+filePath)
		return
	}
	df.SetColumns(columns)
	df.Inferred = true
	df.Defer(func() { filesys.Delete(fs, filePath) })

	return
}

// CopyToS3 exports a query to an S3 location
func (conn *SnowflakeConn) CopyToS3(sqls ...string) (s3Path string, err error) {

	AwsID := conn.GetProp("AWS_ACCESS_KEY_ID")
	AwsAccessKey := conn.GetProp("AWS_SECRET_ACCESS_KEY")
	if AwsID == "" || AwsAccessKey == "" {
		err = g.Error("Need to set 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' to copy to S3 from snowflake")
		return
	}

	unload := func(sql string, s3PathPart string) {

		defer conn.Context().Wg.Write.Done()

		unloadSQL := g.R(
			conn.template.Core["copy_to_s3"],
			"sql", sql,
			"s3_path", s3PathPart,
			"aws_access_key_id", AwsID,
			"aws_secret_access_key", AwsAccessKey,
		)
		_, err = conn.Exec(unloadSQL)
		if err != nil {
			err = g.Error(err, fmt.Sprintf("SQL Error for %s", s3PathPart))
			conn.Context().CaptureErr(err)
		}

	}

	s3Bucket := conn.GetProp("AWS_BUCKET")
	s3Fs, err := filesys.NewFileSysClient(dbio.TypeFileS3, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for S3")
		return
	}

	s3Path = fmt.Sprintf("s3://%s/%s/stream/%s.csv", s3Bucket, filePathStorageSlug, cast.ToString(g.Now()))

	filesys.Delete(s3Fs, s3Path)
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

// CopyToAzure exports a query to an Azure location
func (conn *SnowflakeConn) CopyToAzure(sqls ...string) (azPath string, err error) {
	if !conn.BaseConn.credsProvided("AZURE") {
		err = g.Error("Need to set 'AZURE_SAS_SVC_URL', 'AZURE_CONTAINER' and 'AZURE_ACCOUNT' to copy from snowflake to azure")
		return
	}

	azToken, err := getAzureToken(conn)
	if err != nil {
		return "", g.Error(err)
	}

	unload := func(sql string, azPathPart string) {

		defer conn.Context().Wg.Write.Done()

		unloadSQL := g.R(
			conn.template.Core["copy_to_azure"],
			"sql", sql,
			"azure_path", azPath,
			"azure_sas_token", azToken,
		)

		conn.SetProp("azure_sas_token", azToken)
		_, err = conn.Exec(unloadSQL)
		if err != nil {
			err = g.Error(err, fmt.Sprintf("SQL Error for %s", azPathPart))
			conn.Context().CaptureErr(err)
		}

	}

	azFs, err := filesys.NewFileSysClient(dbio.TypeFileAzure, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for S3")
		return
	}

	azPath = fmt.Sprintf(
		"azure://%s.blob.core.windows.net/%s/%s-%s",
		conn.GetProp("AZURE_ACCOUNT"),
		conn.GetProp("AZURE_CONTAINER"),
		filePathStorageSlug,
		cast.ToString(g.Now()),
	)

	filesys.Delete(azFs, azPath)
	for i, sql := range sqls {
		azPathPart := fmt.Sprintf("%s/u%02d-", azPath, i+1)
		conn.Context().Wg.Write.Add()
		go unload(sql, azPathPart)
	}

	conn.Context().Wg.Write.Wait()
	err = conn.Context().Err()

	if err == nil {
		g.Debug("Unloaded to %s", azPath)
	}

	return azPath, err
}

// BulkImportFlow bulk import flow
func (conn *SnowflakeConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	defer df.CleanUp()

	settingMppBulkImportFlow(conn, iop.ZStandardCompressorType)

	switch conn.CopyMethod {
	case "AWS":
		return conn.CopyViaAWS(tableFName, df)
	case "AZURE":
		return conn.CopyViaAzure(tableFName, df)
	default:
	}

	table, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return 0, g.Error(err, "could not parse table name: "+tableFName)
	}

	stage := conn.getOrCreateStage(table.Schema)
	if stage != "" {
		return conn.CopyViaStage(tableFName, df)
	}

	// if conn.BaseConn.credsProvided("AWS") {
	// 	return conn.CopyViaAWS(tableFName, df)
	// } else if conn.BaseConn.credsProvided("AZURE") {
	// 	return conn.CopyViaAzure(tableFName, df)
	// }

	if err == nil && stage == "" {
		err = g.Error("Need to permit internal staging, or provide AWS/Azure creds")
		return 0, err
	}

	g.Debug("AWS/Azure creds not provided. Using cursor")
	for ds := range df.StreamCh {
		c, err := conn.BaseConn.InsertBatchStream(tableFName, ds)
		if err != nil {
			return 0, g.Error(err, "could not insert")
		}
		count += c
	}

	return count, nil
}

// BulkImportStream bulk import stream
func (conn *SnowflakeConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	df, err := iop.MakeDataFlow(ds)
	if err != nil {
		err = g.Error(err, "Could not MakeDataFlow")
		return
	}
	return conn.BulkImportFlow(tableFName, df)
}

// CopyViaAWS uses the Snowflake COPY INTO Table command from AWS S3
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
func (conn *SnowflakeConn) CopyViaAWS(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	settingMppBulkImportFlow(conn, iop.ZStandardCompressorType)
	if conn.GetProp("AWS_BUCKET") == "" {
		err = g.Error("Need to set 'AWS_BUCKET' to copy to snowflake from S3")
		return
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

	err = filesys.Delete(s3Fs, s3Path)
	if err != nil {
		return count, g.Error(err, "Could not Delete: "+s3Path)
	}

	df.Defer(func() { filesys.Delete(s3Fs, s3Path) }) // cleanup

	g.Info("writing to s3 for snowflake import")
	bw, err := s3Fs.WriteDataflow(df, s3Path)
	if err != nil {
		return df.Count(), g.Error(err, "Error in FileSysWriteDataflow")
	}
	g.DebugLow("total written: %s to %s", humanize.Bytes(cast.ToUint64(bw)), s3Path)

	return df.Count(), conn.CopyFromS3(tableFName, s3Path)
}

// CopyFromS3 uses the Snowflake COPY INTO Table command from AWS S3
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
func (conn *SnowflakeConn) CopyFromS3(tableFName, s3Path string) (err error) {
	AwsID := conn.GetProp("AWS_ACCESS_KEY_ID")
	AwsAccessKey := conn.GetProp("AWS_SECRET_ACCESS_KEY")
	if AwsID == "" || AwsAccessKey == "" {
		err = g.Error("Need to set 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' to copy to snowflake from S3")
		return
	}

	sql := g.R(
		conn.template.Core["copy_from_s3"],
		"table", tableFName,
		"s3_path", s3Path,
		"aws_access_key_id", AwsID,
		"aws_secret_access_key", AwsAccessKey,
	)

	g.Info("copying into snowflake from s3")
	g.Debug("url: " + s3Path)
	_, err = conn.Exec(sql)
	if err != nil {
		return g.Error(err, "SQL Error")
	}

	return nil
}

// CopyViaAzure uses the Snowflake COPY INTO Table command from Azure
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
func (conn *SnowflakeConn) CopyViaAzure(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	settingMppBulkImportFlow(conn, iop.ZStandardCompressorType)
	if !conn.BaseConn.credsProvided("AZURE") {
		err = g.Error("Need to set 'AZURE_SAS_SVC_URL', 'AZURE_CONTAINER' and 'AZURE_ACCOUNT' to copy to snowflake from azure")
		return
	}

	azPath := fmt.Sprintf(
		"azure://%s.blob.core.windows.net/%s/%s-%s",
		conn.GetProp("AZURE_ACCOUNT"),
		conn.GetProp("AZURE_CONTAINER"),
		filePathStorageSlug,
		tableFName,
	)

	azFs, err := filesys.NewFileSysClient(dbio.TypeFileAzure, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for S3")
		return
	}

	err = filesys.Delete(azFs, azPath)
	if err != nil {
		return count, g.Error(err, "Could not Delete: "+azPath)
	}

	df.Defer(func() { filesys.Delete(azFs, azPath) }) // cleanup

	g.Info("writing to azure for snowflake import")
	bw, err := azFs.WriteDataflow(df, azPath)
	if err != nil {
		return df.Count(), g.Error(err, "Error in FileSysWriteDataflow")
	}
	g.DebugLow("total written: %s to %s", humanize.Bytes(cast.ToUint64(bw)), azPath)

	return df.Count(), conn.CopyFromAzure(tableFName, azPath)
}

// CopyFromAzure uses the Snowflake COPY INTO Table command from Azure
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
func (conn *SnowflakeConn) CopyFromAzure(tableFName, azPath string) (err error) {
	azToken, err := getAzureToken(conn)
	if err != nil {
		return g.Error(err)
	}

	sql := g.R(
		conn.template.Core["copy_from_azure"],
		"table", tableFName,
		"azure_path", azPath,
		"azure_sas_token", azToken,
	)

	g.Info("copying into snowflake from azure")
	g.Debug("url: " + azPath)
	conn.SetProp("azure_sas_token", azToken)
	_, err = conn.Exec(sql)
	if err != nil {
		return g.Error(err, "SQL Error")
	}

	return nil
}

func (conn *SnowflakeConn) UnloadViaStage(sqls ...string) (filePath string, err error) {

	stageFolderPath := fmt.Sprintf(
		"@%s/%s/%s",
		conn.GetProp("internalStage"),
		filePathStorageSlug,
		cast.ToString(g.Now()),
	)

	// Write the each stage file to temp file, read to ds
	folderPath := path.Join(os.TempDir(), "snowflake", "get", g.NowFileStr())
	unload := func(sql string, stagePartPath string) {

		defer conn.Context().Wg.Write.Done()

		unloadSQL := g.R(
			conn.template.Core["copy_to_stage"],
			"sql", sql,
			"stage_path", stagePartPath,
		)

		_, err = conn.Exec(unloadSQL)
		if err != nil {
			err = g.Error(err, "SQL Error for %s", stagePartPath)
			conn.Context().CaptureErr(err)
		}
	}

	conn.Exec("REMOVE " + stageFolderPath)
	defer conn.Exec("REMOVE " + stageFolderPath)
	for i, sql := range sqls {
		stagePathPart := fmt.Sprintf("%s/u%02d-", stageFolderPath, i+1)
		conn.Context().Wg.Write.Add()
		go unload(sql, stagePathPart)
	}

	conn.Context().Wg.Write.Wait()
	err = conn.Context().Err()
	if err != nil {
		err = g.Error(err, "Could not unload to stage files")
		return
	}

	// get stream
	data, err := conn.Query("LIST " + stageFolderPath)
	if err != nil {
		err = g.Error(err, "Could not LIST for %s", stageFolderPath)
		conn.Context().CaptureErr(err)
		return
	}

	process := func(index int, stagePath string) {
		defer conn.Context().Wg.Write.Done()
		filePath := path.Join(folderPath, cast.ToString(index))
		err := conn.GetFile(stagePath, filePath)
		if conn.Context().CaptureErr(err) {
			return
		}
	}

	// this continues to read with 2 concurrent streams at most
	for i, rec := range data.Records() {
		name := cast.ToString(rec["name"])
		conn.Context().Wg.Write.Add()
		go process(i, "@"+name)
	}
	conn.Context().Wg.Write.Wait()

	g.Debug("Unloaded to %s", stageFolderPath)

	return folderPath, err
}

// CopyViaStage uses the Snowflake COPY INTO Table command
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
func (conn *SnowflakeConn) CopyViaStage(tableFName string, df *iop.Dataflow) (count uint64, err error) {

	if conn.GetProp("internalStage") == "" {
		return 0, g.Error("Prop internalStage is required")
	}

	if conn.GetProp("schema") == "" {
		table, err := ParseTableName(tableFName, conn.Type)
		if err != nil {
			return 0, g.Error(err, "could not parse table name: "+tableFName)
		}
		if table.Schema == "" {
			return 0, g.Error("Prop schema is required")
		}
		conn.SetProp("schema", table.Schema)
	}

	// Write the ds to a temp file
	folderPath := path.Join(os.TempDir(), "snowflake", "put", g.NowFileStr())
	if err != nil {
		log.Fatal(err)
	}

	// delete folder when done
	df.Defer(func() { os.RemoveAll(folderPath) })

	fileReadyChn := make(chan filesys.FileReady, 10000)
	go func() {
		fs, err := filesys.NewFileSysClient(dbio.TypeFileLocal, conn.PropArr()...)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Could not get fs client for Local"))
			return
		}

		_, err = fs.WriteDataflowReady(df, folderPath, fileReadyChn)

		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Error writing dataflow to disk: "+folderPath))
			return
		}

	}()

	// Import to staging
	stageFolderPath := g.F("@%s.%s/%s/%s", conn.GetProp("schema"), conn.GetProp("internalStage"), tableFName, g.NowFileStr())
	conn.Exec("USE SCHEMA " + conn.GetProp("schema"))
	_, err = conn.Exec("REMOVE " + stageFolderPath)
	if err != nil {
		err = g.Error(err, "REMOVE: "+stageFolderPath)
		return
	}
	df.Defer(func() { conn.Exec("REMOVE " + stageFolderPath) })

	doPut := func(file filesys.FileReady) (stageFilePath string) {
		if !cast.ToBool(os.Getenv("KEEP_TEMP_FILES")) {
			defer os.Remove(file.URI)
		}
		os.Chmod(file.URI, 0777) // make file readeable everywhere
		err = conn.PutFile(file.URI, stageFolderPath)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Error copying to Snowflake Stage: "+conn.GetProp("internalStage")))
		}
		pathArr := strings.Split(file.URI, "/")
		fileName := pathArr[len(pathArr)-1]
		stageFilePath = g.F("%s/%s", stageFolderPath, fileName)
		return stageFilePath
	}

	doCopy := func(file filesys.FileReady) {
		defer conn.Context().Wg.Write.Done()
		stageFilePath := doPut(file)

		if df.Err() != nil {
			return
		}

		tgtColumns := make([]string, len(file.Columns))
		for i, name := range file.Columns.Names() {
			colName, _ := ParseColumnName(name, conn.GetType())
			tgtColumns[i] = conn.Quote(colName)
		}

		srcColumns := make([]string, len(file.Columns))
		for i := range file.Columns {
			srcColumns[i] = g.F("T.$%d", i+1)
		}

		sql := g.R(
			conn.template.Core["copy_from_stage"],
			"table", tableFName,
			"tgt_columns", strings.Join(tgtColumns, ", "),
			"src_columns", strings.Join(srcColumns, ", "),
			"stage_path", stageFilePath,
		)
		_, err = conn.Exec(sql)
		if err != nil {
			err = g.Error(err, "Error with COPY INTO")
			df.Context.CaptureErr(err)
		}
	}

	for file := range fileReadyChn {
		conn.Context().Wg.Write.Add()
		go doCopy(file)
	}

	conn.Context().Wg.Write.Wait()

	if df.Err() != nil {
		return 0, g.Error(df.Err())
	}

	return df.Count(), nil
}

// GetFile Copies from a staging location to a local file or folder
func (conn *SnowflakeConn) GetFile(internalStagePath, fPath string) (err error) {
	query := g.F(
		"GET file://%s %s auto_compress=false overwrite=true",
		fPath, internalStagePath,
	)

	_, err = conn.Exec(query)
	if err != nil {
		err = g.Error(err, "could not GET file %s", internalStagePath)
		return
	}

	return
}

// PutFile Copies a local file or folder into a staging location
func (conn *SnowflakeConn) PutFile(fPath string, internalStagePath string) (err error) {
	query := g.F(
		"PUT file://%s %s PARALLEL=1 AUTO_COMPRESS=FALSE",
		fPath, internalStagePath,
	)

	_, err = conn.Exec(query)
	if err != nil {
		err = g.Error(err, "could not PUT file %s", fPath)
		return
	}

	return
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *SnowflakeConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	sqlTempl := `
	MERGE INTO {tgt_table} tgt
	USING (SELECT *	FROM {src_table}) src
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

// GetColumnsFull returns columns for given table. `tableName` should
// include schema and table, example: `schema1.table2`
// fields should be `schema_name|table_name|table_type|column_name|data_type|column_id`
func (conn *SnowflakeConn) GetColumnsFull(tableFName string) (data iop.Dataset, err error) {
	table, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return data, g.Error(err, "could not parse table name: "+tableFName)
	}

	data1, err := conn.SumbitTemplate(
		"single", conn.template.Metadata, "columns_full",
		g.M("schema", table.Schema, "table", table.Name),
	)
	if err != nil {
		return data1, err
	}

	data.SetFields([]string{"schema_name", "table_name", "column_name", "data_type", "position"})
	for i, rec := range data1.Records() {
		dataType, _, _ := parseSnowflakeDataType(rec)
		data.Append([]interface{}{rec["schema_name"], rec["table_name"], rec["column_name"], dataType, i + 1})
	}
	return data, nil
}

// GetDatabases returns the list of databases
func (conn *SnowflakeConn) GetDatabases() (data iop.Dataset, err error) {
	data1, err := conn.BaseConn.GetDatabases()
	if err != nil {
		return data1, err
	}

	return data1.Pick("name"), nil
}

// GetSchemas returns schemas
func (conn *SnowflakeConn) GetSchemas() (data iop.Dataset, err error) {
	// fields: [schema_name]
	data1, err := conn.BaseConn.GetSchemas()
	if err != nil {
		return data1, err
	}

	return data1.Pick("schema_name"), nil
}

// GetTables returns tables
func (conn *SnowflakeConn) GetTables(schema string) (data iop.Dataset, err error) {
	// fields: [table_name]
	data1, err := conn.BaseConn.GetTables(schema)
	if err != nil {
		return data1, err
	}

	return data1.Pick("table_name"), nil
}

// GetTables returns tables
func (conn *SnowflakeConn) GetViews(schema string) (data iop.Dataset, err error) {
	// fields: [table_name]
	data1, err := conn.BaseConn.GetViews(schema)
	if err != nil {
		return data1, err
	}

	return data1.Pick("table_name"), nil
}

func parseSnowflakeDataType(rec map[string]any) (dataType string, precision, scale int) {
	dataType = "UNKNOWN"
	typeJSON := g.M()
	err := g.Unmarshal(cast.ToString(rec["data_type"]), &typeJSON)
	if err == nil {
		dataType = cast.ToString(typeJSON["type"])
		precision = cast.ToInt(typeJSON["precision"])
		scale = cast.ToInt(typeJSON["scale"])
	}
	return
}
