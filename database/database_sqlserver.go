package database

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/flarco/dbio"

	"github.com/flarco/dbio/filesys"

	"github.com/dustin/go-humanize"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/cast"
	"github.com/xo/dburl"
)

// MsSQLServerConn is a Microsoft SQL Server connection
type MsSQLServerConn struct {
	BaseConn
	URL        string
	isAzureSQL bool
	isAzureDWH bool
}

// Init initiates the object
func (conn *MsSQLServerConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbSQLServer
	conn.BaseConn.defaultPort = 1433

	version := getVersion(conn.GetURL())
	if strings.Contains(strings.ToLower(version), "sql azure") {
		conn.isAzureSQL = true
		conn.Type = dbio.TypeDbAzure
	} else if strings.Contains(strings.ToLower(version), "azure sql data warehouse") {
		conn.isAzureDWH = true
		conn.Type = dbio.TypeDbAzureDWH
	}

	conn.SetProp("use_bcp_map_parallel", "false")

	var instance Connection
	instance = conn
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// GetURL returns the processed URL
func (conn *MsSQLServerConn) GetURL(newURL ...string) string {
	connURL := conn.BaseConn.URL
	if len(newURL) > 0 {
		connURL = newURL[0]
	}
	url, err := dburl.Parse(connURL)
	if err != nil {
		g.LogError(err, "could not parse SQL Server URL")
		return connURL
	}

	user := url.User.Username()
	password, _ := url.User.Password()
	port := url.Port()
	host := strings.ReplaceAll(url.Host, ":"+port, "")
	database := strings.ReplaceAll(url.Path, "/", "")

	return fmt.Sprintf(
		"server=%s;user id=%s;password=%s;port=%s;database=%s;",
		host, user, password, port, database,
	)
}

func getVersion(URL string) (version string) {
	db, err := sqlx.Open("mssql", URL)
	if err != nil {
		return
	}
	res, err := db.Queryx("select @@version v")
	if err != nil {
		return
	}
	res.Next()
	row, err := res.SliceScan()
	if err != nil {
		return
	}
	version = cast.ToString(row[0])
	db.Close()
	return
}

// BulkImportFlow bulk import flow
func (conn *MsSQLServerConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	if conn.isAzureDWH {
		return conn.CopyViaAzure(tableFName, df)
	}

	_, err = exec.LookPath("bcp")
	if err != nil {
		g.Trace("bcp not found in path. Using cursor...")
		ds := iop.MergeDataflow(df)
		return conn.BaseConn.InsertBatchStream(tableFName, ds)
	}

	importStream := func(ds *iop.Datastream) {
		defer df.Context.Wg.Write.Done()
		_, err = conn.BulkImportStream(tableFName, ds)
		if err != nil {
			df.Context.CaptureErr(g.Error(err))
			df.Context.Cancel()
		}
	}

	for ds := range df.StreamCh {
		df.Context.Wg.Write.Add()
		go importStream(ds)
	}

	df.Context.Wg.Write.Wait()

	return df.Count(), df.Context.Err()
}

// BulkImportStream bulk import stream
func (conn *MsSQLServerConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	_, err = exec.LookPath("bcp")
	if err != nil {
		g.Trace("bcp not found in path. Using cursor...")
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

	return conn.BcpImportStreamParrallel(tableFName, ds)
}

// BcpImportStreamParrallel uses goroutine to import partitioned files
func (conn *MsSQLServerConn) BcpImportStreamParrallel(tableFName string, ds *iop.Datastream) (count uint64, err error) {

	fileRowLimit := cast.ToInt(conn.GetProp("FILE_MAX_ROWS"))
	if fileRowLimit == 0 {
		fileRowLimit = 200000
	}

	doImport := func(tableFName string, nDs *iop.Datastream) {
		defer ds.Context.Wg.Write.Done()

		_, err := conn.BcpImportStream(tableFName, nDs)
		ds.Context.CaptureErr(err)
	}

	for nDs := range ds.Chunk(cast.ToUint64(fileRowLimit)) {
		ds.Context.Wg.Write.Add()
		go doImport(tableFName, nDs)
	}

	ds.Context.Wg.Write.Wait()

	return ds.Count, ds.Err()
}

// BcpImportStream Import using bcp tool
// https://docs.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver15
// bcp dbo.test1 in '/tmp/LargeDataset.csv' -S tcp:sqlserver.host,51433 -d master -U sa -P 'password' -c -t ',' -b 5000
// Limitation: if comma or delimite is in field, it will error.
// need to use delimiter not in field, or do some other transformation
func (conn *MsSQLServerConn) BcpImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	var stderr, stdout bytes.Buffer
	url, err := dburl.Parse(conn.URL)
	if err != nil {
		return
	}

	// Write the ds to a temp file
	filePath := fmt.Sprintf(
		"/tmp/sqlserver.bcp.%d.%s.csv",
		time.Now().Unix(),
		g.RandString(g.AlphaRunes, 3),
	)

	csv := iop.CSV{Path: filePath, Delimiter: ','}
	delimiterRep := `$~d$~`
	quoteRep := `$~q$~`
	newlRep := `$~n$~`
	postUpdateCol := map[int]uint64{}

	// transformation to correctly post process quotes, newlines, and delimiter afterwards
	// https://stackoverflow.com/questions/782353/sql-server-bulk-insert-of-csv-file-with-inconsistent-quotes
	// reduces performance by ~25%, but is correct, and still 10x faster then INSERT INTO with batch VALUES
	// If we use the parallel way, we gain back the speed by using more power. We also loose order.
	transf := func(row []interface{}) (nRow []interface{}) {
		nRow = row
		for i, val := range row {

			switch v := val.(type) {
			case string:
				nRow[i] = strings.ReplaceAll(
					val.(string), string(csv.Delimiter), delimiterRep,
				)
				nRow[i] = strings.ReplaceAll(
					nRow[i].(string), `"`, quoteRep,
				)
				nRow[i] = strings.ReplaceAll(
					nRow[i].(string), "\n", newlRep,
				)
				if nRow[i].(string) != val.(string) {
					if _, ok := postUpdateCol[i]; ok {
						postUpdateCol[i]++
					} else {
						postUpdateCol[i] = 1
					}
				}
			default:
				_ = v
			}
		}
		return
	}

	var csvRowCnt uint64
	if conn.GetProp("use_bcp_map_parallel") == "true" {
		csvRowCnt, err = csv.WriteStream(ds.MapParallel(transf, 20)) // faster but we loose order
	} else {
		csvRowCnt, err = csv.WriteStream(ds.Map(ds.Columns, transf))
	}

	// delete csv
	defer os.Remove(filePath)

	if err != nil {
		ds.Context.CaptureErr(g.Error(err, "Error csv.WriteStream(ds) to "+filePath))
		ds.Context.Cancel()
		return 0, ds.Context.Err()
	} else if csvRowCnt == 0 {
		// no data from source
		return 0, nil
	}

	// Import to Database
	batchSize := 50000
	password, _ := url.User.Password()
	port := url.Port()
	host := strings.ReplaceAll(url.Host, ":"+port, "")
	database := strings.ReplaceAll(url.Path, "/", "")
	user := url.User.Username()
	hostPort := fmt.Sprintf("tcp:%s,%s", host, port)

	proc := exec.Command(
		"bcp",
		tableFName,
		"in", filePath,
		"-S", hostPort,
		"-d", database,
		"-U", user,
		"-P", password,
		"-t", string(csv.Delimiter),
		"-m", "1",
		"-c",
		"-q",
		"-b", cast.ToString(batchSize),
		"-F", "2",
		"-e", "/dev/stderr",
	)
	proc.Stderr = &stderr
	proc.Stdout = &stdout

	// run and wait for finish
	cmdStr := strings.ReplaceAll(strings.ReplaceAll(strings.Join(proc.Args, " "), password, "****"), hostPort, "****")
	g.Trace(cmdStr)
	err = proc.Run()

	// get count
	regex := *regexp.MustCompile(`(?s)(\d+) rows copied.`)
	res := regex.FindStringSubmatch(stdout.String())
	if len(res) == 2 {
		count = cast.ToUint64(res[1])
	}

	if err != nil {
		err = g.Error(
			err,
			fmt.Sprintf(
				"SQL Server BCP Import Command -> %s\nSQL Server BCP Import Error  -> %s\n%s",
				cmdStr, stderr.String(), stdout.String(),
			),
		)
		ds.Context.CaptureErr(err)
		ds.Context.Cancel()
		return
	}

	// post process if strings have been modified
	if len(postUpdateCol) > 0 {
		setCols := []string{}
		for i, col := range ds.Columns {
			if _, ok := postUpdateCol[i]; !ok {
				continue
			}

			replExpr1 := g.R(
				`REPLACE(CONVERT(VARCHAR(MAX), {field}), '{delimiterRep}', '{delimiter}')`,
				"field", col.Name,
				"delimiterRep", delimiterRep,
				"delimiter", string(csv.Delimiter),
			)
			replExpr2 := g.R(
				`REPLACE({replExpr1}, '{quoteRep}', '{quote}')`,
				"replExpr1", replExpr1,
				"quoteRep", quoteRep,
				"quote", `"`,
			)
			replExpr3 := g.R(
				`REPLACE({replExpr2}, '{newlRep}', {newl})`,
				"replExpr2", replExpr2,
				"newlRep", newlRep,
				"newl", `CHAR(10)`,
			)
			setCols = append(
				setCols, fmt.Sprintf(`%s = %s`, col.Name, replExpr3),
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
	}

	return
}

// BcpExport exports data to datastream
func (conn *MsSQLServerConn) BcpExport() (err error) {
	return
}

// sqlcmd -S localhost -d BcpSampleDB -U sa -P <your_password> -I -Q "SELECT * FROM TestEmployees;"

// EXPORT
// bcp TestEmployees out ~/test_export.txt -S localhost -U sa -P <your_password> -d BcpSampleDB -c -t ','
// bcp dbo.test1 out ~/test_export.csv -S server.database.windows.net -U user -P 'password' -d db1 -c -t ',' -q -b 10000

// importing from blob
// https://azure.microsoft.com/en-us/updates/files-from-azure-blob-storage-to-sql-database/

//UPSERT
// https://vladmihalcea.com/how-do-upsert-and-merge-work-in-oracle-sql-server-postgresql-and-mysql/

// GenerateUpsertSQL generates the upsert SQL
func (conn *MsSQLServerConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

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
		INSERT ({insert_fields}) VALUES ({src_fields});
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

// CopyViaAzure uses the Azure DWH COPY INTO Table command
func (conn *MsSQLServerConn) CopyViaAzure(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	if !conn.BaseConn.credsProvided("AZURE") {
		err = g.Error("Need to set 'AZURE_SAS_SVC_URL', 'AZURE_CONTAINER' and 'AZURE_ACCOUNT' to copy to azure dwh from container")
		return
	}

	azPath := fmt.Sprintf(
		"https://%s.blob.core.windows.net/%s/%s-%s",
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

	err = azFs.Delete(azPath + "*")
	if err != nil {
		return count, g.Error(err, "Could not Delete: "+azPath)
	}

	// defer func() { azFs.Delete(azPath + "*") }() // cleanup

	fileReadyChn := make(chan string, 10000)
	go func() {
		var bw int64
		bw, err = azFs.WriteDataflowReady(df, azPath, fileReadyChn)
		g.Debug("total written: %s to %s", humanize.Bytes(cast.ToUint64(bw)), azPath)

		if err != nil {
			err = g.Error(err, "Error writing dataflow to azure blob: "+azPath)
			return
		}
	}()

	g.Info("writing to azure container for import")

	doCopy := func(filePath string) {
		defer df.Context.Wg.Write.Done()
		cnt, err := conn.CopyFromAzure(tableFName, filePath)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "could not copy to azure dwh"))
		} else {
			count += cnt
		}
	}

	for filePath := range fileReadyChn {
		df.Context.Wg.Write.Add()
		go doCopy(filePath)
	}

	df.Context.Wg.Write.Wait()
	if df.Err() != nil {
		err = g.Error(df.Context.Err())
	}

	return df.Count(), err
}

// CopyFromAzure uses the COPY INTO Table command from Azure
// https://docs.microsoft.com/en-us/sql/t-sql/statements/copy-into-transact-sql?view=azure-sqldw-latest
func (conn *MsSQLServerConn) CopyFromAzure(tableFName, azPath string) (count uint64, err error) {
	azSasURL := conn.GetProp("AZURE_SAS_SVC_URL")
	if azSasURL == "" {
		err = g.Error("Need to set 'AZURE_SAS_SVC_URL' to copy to azure dwh from container")
		return
	}

	azSasURLArr := strings.Split(azSasURL, "?")
	if len(azSasURLArr) != 2 {
		err = g.Error(
			g.Error("Invalid provided AZURE_SAS_SVC_URL"),
			"",
		)
		return
	}
	azToken := azSasURLArr[1]

	sql := g.R(
		conn.template.Core["copy_from_azure"],
		"table", tableFName,
		"azure_path", azPath,
		"azure_sas_token", azToken,
		"date_format", "ymd",
	)
	conn.SetProp("azToken", azToken) // to not log it in debug logging

	g.Info("copying into azure DWH")
	g.Debug("url: " + azPath)
	_, err = conn.Exec(sql)
	if err != nil {
		conn.SetProp("azToken", azToken)
		return 0, g.Error(err, "SQL Error:\n"+CleanSQL(conn, sql))
	}

	return 0, nil
}
