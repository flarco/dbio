package database

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/flarco/dbio"
	"github.com/flarco/g/sizedwaitgroup"

	"github.com/flarco/dbio/filesys"

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

	if s := conn.GetProp("schema"); s != "" {
		conn.URL = strings.ReplaceAll(conn.URL, "schema="+s, "")
		conn.SetProp("schema", s)
	} else {
		// conn.SetProp("schema", "public") // default schema
	}

	if m := conn.GetProp("CopyMethod"); m != "" {
		conn.URL = strings.ReplaceAll(conn.URL, "CopyMethod="+m, "")
		conn.CopyMethod = conn.GetProp("CopyMethod")
	}

	if strings.HasSuffix(conn.URL, "?") {
		conn.URL = conn.URL[0 : len(conn.URL)-1]
	}

	URL := strings.ReplaceAll(
		conn.URL,
		"snowflake://",
		"",
	)

	conn.BaseConn.URL = URL
	conn.BaseConn.Type = dbio.TypeDbSnowflake

	var instance Connection
	instance = conn
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()

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
	data, err := conn.Query("SHOW WAREHOUSES")
	if err != nil {
		return g.Error(err, "could not SHOW WAREHOUSES")
	}
	if len(data.Rows) > 0 {
		conn.SetProp("warehouse", cast.ToString(data.Rows[0][0]))
	}

	if conn.GetProp("schema") != "" {
		_, err = conn.Exec("USE SCHEMA " + conn.GetProp("schema"))
	}
	return err
}

func (conn *SnowflakeConn) getOrCreateStage(schema string) string {
	if conn.GetProp("internalStage") == "" {
		defStaging := "sling_staging"
		if schema == "" {
			schema = conn.GetProp("schema")
		}
		conn.Exec("USE SCHEMA " + schema)
		_, err := conn.Exec("CREATE STAGE IF NOT EXISTS " + defStaging)
		if err != nil {
			g.Warn("Tried to create Internal Snowflake Stage but failed.\n" + g.ErrMsg(err))
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
	case "STAGE":
		if conn.getOrCreateStage("") != "" {
			return conn.UnloadViaStage(sqls...)
		}
		err = g.Error("could not get or create stage")
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
	df.Defer(func() { fs.Delete(filePath) })

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

	azFs.Delete(azPath)
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
	settingMppBulkImportFlow(conn)

	switch conn.CopyMethod {
	case "AWS":
		return conn.CopyViaAWS(tableFName, df)
	case "AZURE":
		return conn.CopyViaAzure(tableFName, df)
	default:
	}

	schema, _ := SplitTableFullName(tableFName)
	stage := conn.getOrCreateStage(schema)
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

	g.Debug("snowsql not found in path & AWS/Azure creds not provided. Using cursor")
	ds := iop.MergeDataflow(df)
	return conn.BaseConn.InsertBatchStream(tableFName, ds)
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
	settingMppBulkImportFlow(conn)
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

	err = s3Fs.Delete(s3Path)
	if err != nil {
		return count, g.Error(err, "Could not Delete: "+s3Path)
	}

	defer func() { s3Fs.Delete(s3Path) }() // cleanup

	g.Info("writing to s3 for snowflake import")
	bw, err := s3Fs.WriteDataflow(df, s3Path)
	if err != nil {
		return df.Count(), g.Error(err, "Error in FileSysWriteDataflow")
	}
	g.Debug("total written: %s to %s", humanize.Bytes(cast.ToUint64(bw)), s3Path)

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
		return g.Error(err, "SQL Error:\n"+CleanSQL(conn, sql))
	}

	return nil
}

// CopyViaAzure uses the Snowflake COPY INTO Table command from Azure
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
func (conn *SnowflakeConn) CopyViaAzure(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	settingMppBulkImportFlow(conn)
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

	err = azFs.Delete(azPath)
	if err != nil {
		return count, g.Error(err, "Could not Delete: "+azPath)
	}

	defer func() { azFs.Delete(azPath) }() // cleanup

	g.Info("writing to azure for snowflake import")
	bw, err := azFs.WriteDataflow(df, azPath)
	if err != nil {
		return df.Count(), g.Error(err, "Error in FileSysWriteDataflow")
	}
	g.Debug("total written: %s to %s", humanize.Bytes(cast.ToUint64(bw)), azPath)

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
		return g.Error(err, "SQL Error:\n"+CleanSQL(conn, sql))
	}

	return nil
}

func (conn *SnowflakeConn) UnloadViaStage(sqls ...string) (df *iop.Dataflow, err error) {

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

	stageFolderPath := fmt.Sprintf(
		"@%s/%s/%s",
		conn.GetProp("internalStage"),
		filePathStorageSlug,
		cast.ToString(g.Now()),
	)

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

	g.Debug("Unloaded to %s", stageFolderPath)

	// get stream
	data, err := conn.Query("LIST " + stageFolderPath)
	if err != nil {
		err = g.Error(err, "Could not LIST for %s", stageFolderPath)
		conn.Context().CaptureErr(err)
		return
	}

	fs, err := filesys.NewFileSysClient(dbio.TypeFileLocal, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for Local")
		return
	}

	// Write the each stage file to temp file, read to ds
	folderPath := fmt.Sprintf(
		"/tmp/snowflake.get.%d.%s.csv",
		time.Now().Unix(),
		g.RandString(g.AlphaRunes, 3),
	)

	// GET and copy to local, read CSV stream and add to df
	df = iop.NewDataflow()

	// defer folder deletion
	df.Defer(func() { os.RemoveAll(folderPath) })

	wg := sizedwaitgroup.New(2) // 2 concurent file streams at most?
	process := func(index int, stagePath string) {
		defer wg.Done()
		filePath := g.F("%s/%d", folderPath, index)
		err := conn.GetFile(stagePath, filePath)
		if conn.Context().CaptureErr(err) {
			return
		}
		// file is ready to be read
		fDf, err := fs.ReadDataflow(filePath)
		if conn.Context().CaptureErr(err) {
			return
		}

		// push to dataflow
		df.PushStreams(iop.MergeDataflow(fDf))
	}

	// this continues to read with 2 concurrent streams at most
	go func() {
		for i, rec := range data.Records() {
			name := cast.ToString(rec["name"])
			wg.Add()
			go process(i, "@"+name)
		}
		wg.Wait()
	}()

	return
}

// CopyViaStage uses the Snowflake COPY INTO Table command
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
func (conn *SnowflakeConn) CopyViaStage(tableFName string, df *iop.Dataflow) (count uint64, err error) {

	if conn.GetProp("internalStage") == "" {
		return 0, g.Error("Prop internalStage is required")
	}

	if conn.GetProp("schema") == "" {
		schema, _ := SplitTableFullName(tableFName)
		if schema == "" {
			return 0, g.Error("Prop schema is required")
		}
		conn.SetProp("schema", schema)
	}

	// Write the ds to a temp file
	folderPath := fmt.Sprintf(
		"/tmp/snowflake.put.%d.%s.csv",
		time.Now().Unix(),
		g.RandString(g.AlphaRunes, 3),
	)

	// delete folder when done
	defer os.RemoveAll(folderPath)

	fileReadyChn := make(chan string, 10000)
	go func() {
		fs, err := filesys.NewFileSysClient(dbio.TypeFileLocal, conn.PropArr()...)
		if err != nil {
			err = g.Error(err, "Could not get fs client for Local")
			return
		}

		_, err = fs.WriteDataflowReady(df, folderPath, fileReadyChn)

		if err != nil {
			err = g.Error(err, "Error writing dataflow to disk: "+folderPath)
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
	defer conn.Exec("REMOVE " + stageFolderPath)

	doPut := func(filePath string) {
		defer os.Remove(filePath)
		defer conn.Context().Wg.Write.Done()
		os.Chmod(filePath, 0777) // make file readeable everywhere
		err = conn.PutFile(filePath, stageFolderPath)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Error copying to Snowflake Stage: "+conn.GetProp("internalStage")))
		}
	}

	for filePath := range fileReadyChn {
		conn.Context().Wg.Write.Add()
		go doPut(filePath)
	}

	conn.Context().Wg.Write.Wait()

	if df.Context.Err() != nil {
		return 0, g.Error(df.Context.Err())
	}

	// COPY INTO Table
	sql := g.R(
		conn.template.Core["copy_from_stage"],
		"table", tableFName,
		"stage_path", stageFolderPath,
	)

	_, err = conn.Exec(sql)
	if err != nil {
		err = g.Error(err, "Error with COPY INTO:\n"+sql)
		return
	}

	return df.Count(), nil
}

// GetFile Copies from a staging location to a local file or folder
func (conn *SnowflakeConn) GetFile(internalStagePath, fPath string) (err error) {
	query := g.F(
		"GET %s file://%s PARALLEL=20",
		internalStagePath, fPath,
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
		"PUT file://%s %s PARALLEL=20",
		fPath, internalStagePath,
	)

	_, err = conn.Exec(query)
	if err != nil {
		err = g.Error(err, "could not PUT file %s", fPath)
		return
	}

	return
}

func selectFromDataset(data iop.Dataset, colIDs []int) (newData iop.Dataset) {
	newData = iop.NewDataset(data.Columns)
	newData.Rows = make([][]interface{}, len(data.Rows))

	for i, row := range data.Rows {
		newRow := make([]interface{}, len(colIDs))
		for j, c := range colIDs {
			if c+1 > len(row) {
				continue
			}
			newRow[j] = row[c]
		}
		newData.Rows[i] = newRow
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
