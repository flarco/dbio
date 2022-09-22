package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/flarco/dbio"
	"github.com/flarco/dbio/filesys"
	"github.com/flarco/g/net"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/spf13/cast"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// BigQueryConn is a Google Big Query connection
type BigQueryConn struct {
	BaseConn
	URL       string
	Client    *bigquery.Client
	ProjectID string
	DatasetID string
	Location  string
	Datasets  []string
}

// Init initiates the object
func (conn *BigQueryConn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbBigQuery

	u, err := net.NewURL(conn.BaseConn.URL)
	if err != nil {
		return g.Error(err, "could not parse bigquery url")
	}

	conn.ProjectID = conn.GetProp("project")
	if conn.ProjectID == "" {
		conn.ProjectID = u.U.Host
	}

	conn.DatasetID = conn.GetProp("dataset")
	if conn.DatasetID == "" {
		conn.DatasetID = conn.GetProp("schema")
	}

	var instance Connection
	instance = conn
	conn.BaseConn.instance = &instance

	err = conn.BaseConn.Init()
	if err != nil {
		err = g.Error(err, "could not initialize connection")
		return err
	}

	// Google BigQuery has limits
	// https://cloud.google.com/bigquery/quotas
	conn.Context().SetConcurencyLimit(5)
	conn.SetProp("FILE_MAX_ROWS", "1000000") // hard code?

	if conn.GetProp("GC_KEY_FILE") == "" {
		conn.SetProp("GC_KEY_FILE", conn.GetProp("keyfile")) // dbt style
	}

	// set MAX_DECIMALS to fix bigquery import for numeric types
	os.Setenv("MAX_DECIMALS", "9")

	return nil
}

func (conn *BigQueryConn) getNewClient(timeOut ...int) (client *bigquery.Client, err error) {
	var authOption option.ClientOption
	var credJsonBody string

	to := 15
	if len(timeOut) > 0 {
		to = timeOut[0]
	}

	if val := conn.GetProp("GC_KEY_BODY"); val != "" {
		credJsonBody = val
		authOption = option.WithCredentialsJSON([]byte(val))
	} else if val := conn.GetProp("GC_KEY_FILE"); val != "" {
		authOption = option.WithCredentialsFile(val)
		b, err := ioutil.ReadFile(val)
		if err != nil {
			return client, g.Error(err, "could not read google cloud key file")
		}
		credJsonBody = string(b)
	} else if val := conn.GetProp("GC_CRED_API_KEY"); val != "" {
		authOption = option.WithAPIKey(val)
	} else if val := conn.GetProp("GOOGLE_APPLICATION_CREDENTIALS"); val != "" {
		authOption = option.WithCredentialsFile(val)
		b, err := ioutil.ReadFile(val)
		if err != nil {
			return client, g.Error(err, "could not read google cloud key file")
		}
		credJsonBody = string(b)
	} else {
		err = g.Error("no Google credentials provided")
		return
	}

	if conn.ProjectID == "" && credJsonBody != "" {
		m := g.M()
		g.Unmarshal(credJsonBody, &m)
		conn.ProjectID = cast.ToString(m["project_id"])
	}

	ctx, cancel := context.WithTimeout(conn.BaseConn.Context().Ctx, time.Duration(to)*time.Second)
	defer cancel()
	return bigquery.NewClient(ctx, conn.ProjectID, authOption)
}

// Connect connects to the database
func (conn *BigQueryConn) Connect(timeOut ...int) error {
	var err error
	conn.Client, err = conn.getNewClient(timeOut...)
	if err != nil {
		return g.Error(err, "Failed to connect to client")
	}

	// get list of datasets
	it := conn.Client.Datasets(conn.Context().Ctx)
	for {
		dataset, err := it.Next()
		if err == iterator.Done {
			err = nil
			break
		} else if err != nil {
			return g.Error(err, "Failed to get datasets")
		}

		conn.Datasets = append(conn.Datasets, dataset.DatasetID)
		if conn.Location == "" {
			md, _ := dataset.Metadata(conn.Context().Ctx)
			conn.Location = md.Location
		}
	}

	return conn.BaseConn.Connect()
}

// NewTransaction creates a new transaction
func (conn *BigQueryConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (tx Transaction, err error) {
	// context := g.NewContext(ctx)

	// _, err = conn.ExecContext(ctx, "BEGIN")
	// if err != nil {
	// 	return nil, g.Error(err, "could not begin Tx")
	// }

	// BQ does not support transactions at the moment
	// Tx := &BlankTransaction{Conn: conn.Self(), context: &context}
	// conn.tx = Tx

	return nil, nil
}

type bqResult struct {
	TotalRows uint64
	res       driver.Result
}

func (r bqResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r bqResult) RowsAffected() (int64, error) {
	return cast.ToInt64(r.TotalRows), nil
}

// ExecContext runs a sql query with context, returns `error`
func (conn *BigQueryConn) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {

	if len(args) > 0 {
		for _, arg := range args {
			switch val := arg.(type) {
			case int, int64, int8, int32, int16:
				sql = strings.Replace(sql, "?", fmt.Sprintf("%d", val), 1)
			case float32, float64:
				sql = strings.Replace(sql, "?", fmt.Sprintf("%f", val), 1)
			case time.Time:
				sql = strings.Replace(sql, "?", fmt.Sprintf("'%s'", val.Format("2006-01-02 15:04:05")), 1)
			case nil:
				sql = strings.Replace(sql, "?", "NULL", 1)
			case []byte:
				if len(val) == 0 {
					sql = strings.Replace(sql, "?", "NULL", 1)

				} else {
					newdata := base64.StdEncoding.EncodeToString(val)
					sql = strings.Replace(sql, "?", fmt.Sprintf("FROM_BASE64('%s')", newdata), 1)
				}
			default:
				v := strings.ReplaceAll(cast.ToString(val), "\n", "\\n")
				v = strings.ReplaceAll(v, "'", "\\'")
				sql = strings.Replace(sql, "?", fmt.Sprintf("'%s'", v), 1)
			}
		}
	}

	res := bqResult{}
	exec := func(sql string) error {
		noTrace := strings.Contains(sql, noTraceKey)
		if !noTrace {
			g.Debug(sql)
		}

		q := conn.Client.Query(sql)
		q.QueryConfig = bigquery.QueryConfig{
			Q:                sql,
			DefaultDatasetID: conn.GetProp("schema"),
		}
		it, err := q.Read(ctx)
		if err != nil {
			if strings.Contains(sql, noTraceKey) && !g.IsDebugLow() {
				return g.Error(err, "SQL Error")
			}
			return g.Error(err, "SQL Error for:\n"+sql)
		}

		if err != nil {
			if strings.Contains(sql, noTraceKey) && !g.IsDebugLow() {
				return g.Error(err, "Error executing query")
			} else {
				return g.Error(err, "Error executing "+CleanSQL(conn, sql))
			}
		} else {
			res.TotalRows = it.TotalRows + res.TotalRows
		}
		return nil
	}

	for _, sql := range ParseSQLMultiStatements(sql) {
		err = exec(sql)
		if err != nil {
			err = g.Error(err, "Error executing query")
		}
	}
	result = res

	return
}

type bQTypeCols struct {
	numericCols  []int
	datetimeCols []int
	dateCols     []int
	boolCols     []int
	timeCols     []int
}

func processBQTypeCols(row []interface{}, bqTC *bQTypeCols, ds *iop.Datastream) []interface{} {
	for _, j := range bqTC.numericCols {
		var vBR *big.Rat
		vBR, ok := row[j].(*big.Rat)
		if ok {
			row[j], _ = vBR.Float64()
		}
	}
	for _, j := range bqTC.datetimeCols {
		if row[j] != nil {
			vDT, ok := row[j].(civil.DateTime)
			if ok {
				row[j], _ = ds.Sp.ParseTime(vDT.Date.String() + " " + vDT.Time.String())
			}
		}
	}
	for _, j := range bqTC.dateCols {
		if row[j] != nil {
			vDT, ok := row[j].(civil.Date)
			if ok {
				row[j], _ = ds.Sp.ParseTime(vDT.String())
			}
		}
	}
	for _, j := range bqTC.timeCols {
		if row[j] != nil {
			vDT, ok := row[j].(civil.Time)
			if ok {
				row[j], _ = ds.Sp.ParseTime(vDT.String())
			}
		}
	}
	for _, j := range bqTC.boolCols {
		if row[j] != nil {
			vB, ok := row[j].(bool)
			if ok {
				row[j] = vB
			}
		}
	}
	return row
}

// StreamRowsContext streams the rows of a sql query with context, returns `result`, `error`
func (conn *BigQueryConn) getItColumns(itSchema bigquery.Schema) (cols iop.Columns, bQTC bQTypeCols) {
	cols = make(iop.Columns, len(itSchema))
	NativeTypeMap := conn.template.NativeTypeMap
	for i, field := range itSchema {
		dbType := strings.ToLower(string(field.Type))
		dbType = strings.Split(dbType, "<")[0]
		dbType = strings.Split(dbType, "(")[0]

		var Type iop.ColumnType
		if matchedType, ok := NativeTypeMap[dbType]; ok {
			Type = iop.ColumnType(matchedType)
		} else {
			if dbType != "" {
				g.Warn("type '%s' not mapped for col '%s': %#v", dbType, field.Name, field.Type)
			}
			Type = iop.StringType // default as string
		}

		cols[i] = iop.Column{
			Name:     field.Name,
			Position: i + 1,
			Type:     Type,
			DbType:   dbType,
		}
		if g.In(field.Type, bigquery.NumericFieldType, bigquery.FloatFieldType) {
			bQTC.numericCols = append(bQTC.numericCols, i)
		} else if field.Type == "DATETIME" || field.Type == bigquery.TimestampFieldType {
			bQTC.datetimeCols = append(bQTC.datetimeCols, i)
		} else if field.Type == "DATE" {
			bQTC.dateCols = append(bQTC.dateCols, i)
		} else if field.Type == bigquery.TimeFieldType {
			bQTC.timeCols = append(bQTC.timeCols, i)
		}
	}
	return
}

func (conn *BigQueryConn) StreamRowsContext(ctx context.Context, sql string, limit ...int) (ds *iop.Datastream, err error) {
	bQTC := bQTypeCols{}
	Limit := uint64(0) // infinite
	if len(limit) > 0 && limit[0] != 0 {
		Limit = cast.ToUint64(limit[0])
	}

	start := time.Now()
	if strings.TrimSpace(sql) == "" {
		g.Warn("Empty Query")
		return ds, nil
	}

	noTrace := strings.Contains(sql, noTraceKey)
	if !noTrace {
		g.Debug(sql)
	}
	queryContext := g.NewContext(ctx)
	q := conn.Client.Query(sql)
	q.QueryConfig = bigquery.QueryConfig{
		Q:                sql,
		DefaultDatasetID: conn.GetProp("schema"),
	}

	it, err := q.Read(queryContext.Ctx)
	if err != nil {
		if strings.Contains(sql, noTraceKey) && !g.IsDebugLow() {
			err = g.Error(err, "SQL Error")
		} else {
			err = g.Error(err, "SQL Error for:\n"+sql)
		}
		return
	}

	conn.Data.SQL = sql
	conn.Data.Duration = time.Since(start).Seconds()
	conn.Data.Rows = [][]interface{}{}
	conn.Data.NoTrace = !strings.Contains(sql, noTraceKey)

	// need to fetch first row to get schema
	var values []bigquery.Value
	err = it.Next(&values)
	if err != nil && err != iterator.Done {
		return ds, g.Error(err, "Failed to scan")
	}
	conn.Data.Columns, bQTC = conn.getItColumns(it.Schema)

	if err == iterator.Done {
		ds = iop.NewDatastreamContext(queryContext.Ctx, conn.Data.Columns)
		ds.SetReady()
		ds.Close()
		return ds, nil
	}

	nextFunc := func(it2 *iop.Iterator) bool {
		if Limit > 0 && it2.Counter >= Limit {
			return false
		}

		err := it.Next(&values)
		if err == iterator.Done {
			return false
		} else if err != nil {
			ds.Context.CaptureErr(g.Error(err, "Failed to scan"))
			ds.Context.Cancel()
			return false
		}

		it2.Row = make([]interface{}, len(ds.Columns))
		for i := range values {
			it2.Row[i] = values[i]
		}
		it2.Row = processBQTypeCols(it2.Row, &bQTC, ds)
		return true
	}

	ds = iop.NewDatastreamIt(queryContext.Ctx, conn.Data.Columns, nextFunc)
	ds.NoTrace = !strings.Contains(sql, noTraceKey)
	ds.Inferred = InferDBStream

	// add first row pulled to buffer
	row := make([]interface{}, len(ds.Columns))
	for i := range values {
		row[i] = values[i]
	}
	ds.Buffer = append(ds.Buffer, processBQTypeCols(row, &bQTC, ds))

	err = ds.Start()
	if err != nil {
		queryContext.Cancel()
		return ds, g.Error(err, "could start datastream")
	}

	return
}

// Close closes the connection
func (conn *BigQueryConn) Close() error {
	err := conn.Client.Close()
	if err != nil {
		return err
	}
	return conn.BaseConn.Close()
}

// InsertBatchStream inserts a stream into a table in batch
func (conn *BigQueryConn) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BulkImportStream(tableFName, ds)
}

// InsertStream demonstrates loading data into a BigQuery table using a file on the local filesystem.
func (conn *BigQueryConn) InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BulkImportStream(tableFName, ds)
}

func getBqSchema(columns []iop.Column) (schema bigquery.Schema) {
	schema = make([]*bigquery.FieldSchema, len(columns))
	mapping := map[iop.ColumnType]bigquery.FieldType{
		iop.ColumnType(""): bigquery.StringFieldType,
		iop.StringType:     bigquery.StringFieldType,
		iop.TextType:       bigquery.StringFieldType,
		iop.JsonType:       bigquery.JSONFieldType,
		iop.BoolType:       bigquery.BooleanFieldType,
		iop.BinaryType:     bigquery.BytesFieldType,
		iop.DateType:       bigquery.TimestampFieldType,
		iop.DatetimeType:   bigquery.TimestampFieldType,
		iop.FloatType:      bigquery.FloatFieldType,
		iop.SmallIntType:   bigquery.IntegerFieldType,
		iop.IntegerType:    bigquery.IntegerFieldType,
		iop.BigIntType:     bigquery.IntegerFieldType,
		// https://stackoverflow.com/questions/55904464/big-query-does-now-cast-automatically-long-decimal-values-to-numeric-when-runni
		iop.DecimalType: bigquery.NumericFieldType,
		// "decimal":   bigquery.FloatFieldType,
		iop.TimeType:       bigquery.TimeFieldType,
		iop.TimestampType:  bigquery.TimestampFieldType,
		iop.TimestampzType: bigquery.TimestampFieldType,
	}

	for i, col := range columns {
		g.Trace("bigquery.Schema for %s (%s) -> %#v", col.Name, col.Type, mapping[col.Type])
		schema[i] = &bigquery.FieldSchema{
			Name: col.Name,
			Type: mapping[col.Type],
		}
	}
	return
}

// BulkImportFlow inserts a flow of streams into a table.
// For redshift we need to create CSVs in S3 and then use the COPY command.
func (conn *BigQueryConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	defer df.CleanUp()

	settingMppBulkImportFlow(conn, iop.GzipCompressorType)

	gcBucket := conn.GetProp("GC_BUCKET")

	if gcBucket == "" {
		return count, g.Error("Need to set 'GC_BUCKET' to copy to google storage")
	}
	fs, err := filesys.NewFileSysClient(dbio.TypeFileGoogle, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for GCS")
		return
	}

	gcsPath := fmt.Sprintf(
		"gs://%s/%s/%s.csv",
		gcBucket,
		filePathStorageSlug,
		tableFName,
	)

	err = filesys.Delete(fs, gcsPath)
	if err != nil {
		return count, g.Error(err, "Could not Delete: "+gcsPath)
	}

	df.Defer(func() { filesys.Delete(fs, gcsPath) })

	g.Info("importing into bigquery via google storage")

	fileReadyChn := make(chan string, 10)

	go func() {
		_, err = fs.WriteDataflowReady(df, gcsPath, fileReadyChn)

		if err != nil {
			g.LogError(err, "error writing dataflow to google storage: "+gcsPath)
			df.Context.Cancel()
			conn.Context().Cancel()
			return
		}

	}()

	copyFromGCS := func(gcsURI string, tableFName string) {
		defer conn.Context().Wg.Write.Done()
		g.Debug("Loading %s", gcsURI)

		err := conn.CopyFromGCS(gcsURI, tableFName, df.Columns)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Error copying from %s into %s", gcsURI, tableFName))
			df.Context.Cancel()
			conn.Context().Cancel()
		}
	}

	inferred := false
	for gcsPartPath := range fileReadyChn {
		if !inferred {
			// the schema matters with using the load tool
			// so let's make sure we infer once again
			df.Inferred = false
			df.SyncStats()
			inferred = true
		}

		time.Sleep(2 * time.Second) // max 5 load jobs per 10 secs
		conn.Context().Wg.Write.Add()
		go copyFromGCS(gcsPartPath, tableFName)
	}

	conn.Context().Wg.Write.Wait()
	if df.Err() != nil {
		return df.Count(), g.Error(df.Context.Err(), "Error importing to BigQuery")
	}

	return df.Count(), nil
}

// BulkImportStream demonstrates loading data into a BigQuery table using a file on the local filesystem.
func (conn *BigQueryConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	df, err := iop.MakeDataFlow(ds)
	if err != nil {
		err = g.Error(err, "Could not MakeDataFlow")
		return
	}

	return conn.BulkImportFlow(tableFName, df)
}

// CopyFromGCS into bigquery from google storage
func (conn *BigQueryConn) CopyFromGCS(gcsURI string, tableFName string, dsColumns []iop.Column) error {
	client, err := conn.getNewClient()
	if err != nil {
		return g.Error(err, "Failed to connect to client")
	}
	defer client.Close()

	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.FieldDelimiter = ","
	gcsRef.AllowQuotedNewlines = true
	gcsRef.Quote = `"`
	gcsRef.SkipLeadingRows = 1
	gcsRef.Schema = getBqSchema(dsColumns)
	if strings.HasSuffix(strings.ToLower(gcsURI), ".gz") {
		gcsRef.Compression = bigquery.Gzip
	}
	gcsRef.MaxBadRecords = 0

	schema, tableID := SplitTableFullName(tableFName)
	loader := client.Dataset(schema).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteAppend

	job, err := loader.Run(conn.Context().Ctx)
	if err != nil {
		return g.Error(err, "Error in loader.Execute")
	}
	status, err := job.Wait(conn.Context().Ctx)
	if err != nil {
		return g.Error(err, "Error in task.Wait")
	}

	if status.Err() != nil {
		conn.Context().CaptureErr(err)
		for _, e := range status.Errors {
			conn.Context().CaptureErr(*e)
		}
		return g.Error(conn.Context().Err(), "Error in Import Task")
	}

	return nil
}

// BulkExportFlow reads in bulk
func (conn *BigQueryConn) BulkExportFlow(sqls ...string) (df *iop.Dataflow, err error) {

	gsURL, err := conn.Unload(sqls...)
	if err != nil {
		err = g.Error(err, "Could not unload.")
		return
	}

	fs, err := filesys.NewFileSysClient(dbio.TypeFileGoogle, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for GCS")
		return
	}

	df, err = fs.ReadDataflow(gsURL)
	if err != nil {
		err = g.Error(err, "Could not read "+gsURL)
		return
	}

	df.Defer(func() { filesys.Delete(fs, gsURL) })

	return
}

// CopyToGCS demonstrates using an export task to
// write the contents of a table into Cloud Storage as CSV.
// func (conn *BigQueryConn) CopyFromGS(tableFName string, gcsURI string) error {
// 	ctx, cancel := context.WithTimeout(conn.Context().Ctx, time.Second*50)
// 	defer cancel()
// 	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rc.Close()

// 	data, err := ioutil.ReadAll(rc)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return data, nil
// }

// Unload to Google Cloud Storage
func (conn *BigQueryConn) Unload(sqls ...string) (gsPath string, err error) {
	gcBucket := conn.GetProp("GC_BUCKET")
	if gcBucket == "" {
		err = g.Error("Must provide prop 'GC_BUCKET'")
		return
	}

	doExport := func(sql string, gsPartURL string) {
		defer conn.Context().Wg.Write.Done()

		// create temp table
		tableName := g.F(
			"pg_home.tmp_%s",
			g.RandString(g.AlphaRunes, 5),
		)

		_, err := conn.Exec(g.F(
			"create table `%s.%s` as \n%s",
			conn.ProjectID,
			tableName, sql,
		))
		if err != nil {
			conn.Context().CaptureErr(g.Error(err, "Could not create table"))
			return
		}

		bucket := conn.GetProp("GC_BUCKET")
		if bucket == "" {
			err = g.Error("need to provide prop 'GC_BUCKET'")
			return
		}

		// gcsURI := g.F("gs://%s/%s.csv/*", bucket, tableName)

		// export
		err = conn.CopyToGCS(tableName, gsPartURL)
		if err != nil {
			conn.Context().CaptureErr(g.Error(err, "Could not Copy to GS"))
		}

		// drop temp table
		err = conn.DropTable(tableName)
		if err != nil {
			conn.Context().CaptureErr(g.Error(err, "Could not Drop table: "+tableName))
		}

	}

	gsFs, err := filesys.NewFileSysClient(dbio.TypeFileGoogle, conn.PropArr()...)
	if err != nil {
		conn.Context().CaptureErr(g.Error(err, "Unable to create GCS Client"))
	}

	gsPath = fmt.Sprintf("gs://%s/%s/stream/%s.csv", gcBucket, filePathStorageSlug, cast.ToString(g.Now()))

	filesys.Delete(gsFs, gsPath)

	for i, sql := range sqls {
		gsPathPart := fmt.Sprintf("%s/part%02d-*", gsPath, i+1)
		conn.Context().Wg.Write.Add()
		go doExport(sql, gsPathPart)
	}

	conn.Context().Wg.Write.Wait()
	err = conn.Context().Err()

	if err == nil {
		g.Debug("Unloaded to %s", gsPath)
	}

	return gsPath, err
}

// CopyToGCS Copy table to gc storage
func (conn *BigQueryConn) CopyToGCS(tableFName string, gcsURI string) error {
	client, err := conn.getNewClient()
	if err != nil {
		return g.Error(err, "Failed to connect to client")
	}
	defer client.Close()

	if strings.ToUpper(conn.GetProp("COMPRESSION")) == "GZIP" {
		gcsURI = gcsURI + ".gz"
	}
	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.FieldDelimiter = ","
	gcsRef.AllowQuotedNewlines = true
	gcsRef.Quote = `"`
	if strings.ToUpper(conn.GetProp("COMPRESSION")) == "GZIP" {
		gcsRef.Compression = bigquery.Gzip
	}
	gcsRef.MaxBadRecords = 0

	schema, tableID := SplitTableFullName(tableFName)
	extractor := client.DatasetInProject(conn.ProjectID, schema).Table(tableID).ExtractorTo(gcsRef)
	extractor.DisableHeader = false
	// You can choose to run the task in a specific location for more complex data locality scenarios.
	// Ex: In this example, source dataset and GCS bucket are in the US.
	extractor.Location = "US"

	job, err := extractor.Run(conn.Context().Ctx)
	if err != nil {
		return g.Error(err, "Error in extractor.Execute")
	}
	status, err := job.Wait(conn.Context().Ctx)
	if err != nil {
		return g.Error(err, "Error in task.Wait")
	}
	if err := status.Err(); err != nil {
		conn.Context().CaptureErr(err)
		for _, e := range status.Errors {
			conn.Context().CaptureErr(*e)
		}
		return g.Error(conn.Context().Err(), "Error in Export Task")
	}

	g.Info("wrote to %s", gcsURI)
	return nil
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *BigQueryConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	sqlTempl := `
	DELETE FROM {tgt_table} tgt
	WHERE EXISTS (
			SELECT 1
			FROM {src_table} src
			WHERE {src_tgt_pk_equal}
	)
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
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", upsertMap["src_fields"],
	)

	return
}

// GetDatabases returns databases
func (conn *BigQueryConn) GetDatabases() (iop.Dataset, error) {
	// fields: [name]
	data := iop.NewDataset(iop.NewColumnsFromFields("name"))
	data.Append([]interface{}{conn.ProjectID})
	return data, nil
}

// GetSchemas returns schemas
func (conn *BigQueryConn) GetSchemas() (iop.Dataset, error) {
	// fields: [schema_name]
	data := iop.NewDataset(iop.NewColumnsFromFields("schema_name"))
	for _, dataset := range conn.Datasets {
		data.Append([]interface{}{dataset})
	}
	return data, nil
}

// GetSchemata obtain full schemata info for a schema and/or table in current database
func (conn *BigQueryConn) GetSchemata(schemaName, tableName string) (Schemata, error) {

	schemata := Schemata{
		Databases: map[string]Database{},
	}

	datasets := conn.Datasets
	if schemaName != "" {
		datasets = []string{schemaName}
	}

	currDatabase := conn.ProjectID
	schemas := map[string]Schema{}

	ctx := g.NewContext(conn.context.Ctx, 5)

	getOneSchemata := func(values map[string]interface{}) error {
		defer ctx.Wg.Read.Done()
		schemaData, err := conn.SumbitTemplate(
			"single", conn.template.Metadata, "schemata",
			values,
		)
		if err != nil {
			return g.Error(err, "Could not GetSchemata for "+schemaName)
		}

		defer ctx.Unlock()
		ctx.Lock()

		for _, rec := range schemaData.Records() {
			schemaName = cast.ToString(rec["schema_name"])
			tableName := cast.ToString(rec["table_name"])
			columnName := cast.ToString(rec["column_name"])
			dataType := strings.ToLower(cast.ToString(rec["data_type"]))
			dataType = strings.Split(dataType, "(")[0]
			dataType = strings.Split(dataType, "<")[0]

			// if any of the names contains a period, skip. This messes with the keys
			if strings.Contains(tableName, ".") ||
				strings.Contains(schemaName, ".") ||
				strings.Contains(columnName, ".") {
				continue
			}

			switch v := rec["is_view"].(type) {
			case int64, float64:
				if cast.ToInt64(rec["is_view"]) == 0 {
					rec["is_view"] = false
				} else {
					rec["is_view"] = true
				}
			case string:
				if cast.ToBool(rec["is_view"]) {
					rec["is_view"] = true
				} else {
					rec["is_view"] = false
				}

			default:
				_ = fmt.Sprint(v)
				_ = rec["is_view"]
			}

			schema := Schema{
				Name:   schemaName,
				Tables: map[string]Table{},
			}

			table := Table{
				Name:     tableName,
				Schema:   schemaName,
				Database: currDatabase,
				IsView:   cast.ToBool(rec["is_view"]),
				Columns:  iop.Columns{},
			}

			if _, ok := schemas[strings.ToLower(schema.Name)]; ok {
				schema = schemas[strings.ToLower(schema.Name)]
			}

			if _, ok := schemas[strings.ToLower(schema.Name)].Tables[strings.ToLower(tableName)]; ok {
				table = schemas[strings.ToLower(schema.Name)].Tables[strings.ToLower(tableName)]
			}

			column := iop.Column{
				Name:     columnName,
				Type:     iop.ColumnType(conn.template.NativeTypeMap[dataType]),
				Table:    tableName,
				Schema:   schemaName,
				Database: currDatabase,
				Position: cast.ToInt(schemaData.Sp.ProcessVal(rec["position"])),
				DbType:   dataType,
			}

			table.Columns = append(table.Columns, column)

			schema.Tables[strings.ToLower(tableName)] = table
			schemas[strings.ToLower(schema.Name)] = schema
		}

		schemata.Databases[strings.ToLower(currDatabase)] = Database{
			Name:    currDatabase,
			Schemas: schemas,
		}
		return nil
	}

	for _, dataset := range datasets {
		g.Debug("getting schemata for %s", dataset)
		values := g.M("schema", dataset)
		if tableName != "" {
			values["table"] = tableName
		}

		ctx.Wg.Read.Add()
		go getOneSchemata(values)
	}

	ctx.Wg.Read.Wait()

	return schemata, nil
}
