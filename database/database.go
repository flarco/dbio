package database

import (
	"bufio"
	"context"
	"database/sql"
	"embed"
	"fmt"
	"math"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/flarco/dbio"

	"github.com/flarco/dbio/filesys"

	"github.com/flarco/dbio/env"
	"github.com/flarco/g"

	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/flarco/dbio/iop"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/snowflakedb/gosnowflake"
	_ "github.com/solcates/go-sql-bigquery"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// Connection is the Base interface for Connections
type Connection interface {
	BaseURL() string
	Begin(options ...*sql.TxOptions) error
	BeginContext(ctx context.Context, options ...*sql.TxOptions) error
	BulkExportFlow(sqls ...string) (*iop.Dataflow, error)
	BulkExportStream(sql string) (*iop.Datastream, error)
	BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error)
	BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error)
	CastColumnForSelect(srcColumn iop.Column, tgtColumn iop.Column) string
	CastColumnsForSelect(srcColumns iop.Columns, tgtColumns iop.Columns) []string
	Close() error
	Commit() error
	CompareChecksums(tableName string, columns iop.Columns) (err error)
	Connect(timeOut ...int) error
	Context() *g.Context
	CreateTemporaryTable(tableName string, cols iop.Columns) (err error)
	CreateTable(tableName string, cols iop.Columns, tableDDL string) (err error)
	Db() *sqlx.DB
	DbX() *DbX
	DropTable(...string) error
	DropView(...string) error
	ExecMulti(sql string, args ...interface{}) (result sql.Result, err error)
	ExecMultiContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error)
	Exec(sql string, args ...interface{}) (result sql.Result, err error)
	ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error)
	GenerateDDL(tableFName string, data iop.Dataset, temporary bool) (string, error)
	GenerateInsertStatement(tableName string, fields []string, numRows int) string
	GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error)
	GetColumns(tableFName string, fields ...string) (iop.Columns, error)
	GetColumnsFull(string) (iop.Dataset, error)
	GetColumnStats(tableName string, fields ...string) (columns iop.Columns, err error)
	GetCount(string) (uint64, error)
	GetDDL(string) (string, error)
	GetGormConn(config *gorm.Config) (*gorm.DB, error)
	GetIndexes(string) (iop.Dataset, error)
	GetNativeType(col iop.Column) (nativeType string, err error)
	GetPrimaryKeys(string) (iop.Dataset, error)
	GetProp(string) string
	GetSchemata(string, string) (Schemata, error)
	GetSchemas() (iop.Dataset, error)
	GetSQLColumns(sqls ...string) (columns iop.Columns, err error)
	GetTables(string) (iop.Dataset, error)
	GetTemplateValue(path string) (value string)
	GetType() dbio.Type
	GetURL(newURL ...string) string
	GetViews(string) (iop.Dataset, error)
	Init() error
	InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error)
	InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error)
	Kill() error
	LoadTemplates() error
	MustExec(sql string, args ...interface{}) (result sql.Result)
	NewTransaction(ctx context.Context, options ...*sql.TxOptions) (*Transaction, error)
	OptimizeTable(tableName string, columns iop.Columns) (err error)
	Prepare(query string) (stmt *sql.Stmt, err error)
	Props() map[string]string
	PropsArr() []string
	Query(sql string, limit ...int) (iop.Dataset, error)
	QueryContext(ctx context.Context, sql string, limit ...int) (iop.Dataset, error)
	Quote(field string, normalize ...bool) string
	RenameTable(table string, newTable string) (err error)
	Rollback() error
	ProcessTemplate(level, text string, values map[string]interface{}) (sql string, err error)
	RunAnalysis(string, map[string]interface{}) (iop.Dataset, error)
	GetAnalysis(string, map[string]interface{}) (string, error)
	Schemata() Schemata
	Self() Connection
	setContext(ctx context.Context, concurrency int)
	SetProp(string, string)
	SetTx(*Transaction)
	StreamRecords(sql string) (<-chan map[string]interface{}, error)
	StreamRows(sql string, limit ...int) (*iop.Datastream, error)
	StreamRowsContext(ctx context.Context, sql string, limit ...int) (ds *iop.Datastream, err error)
	SwapTable(srcTable string, tgtTable string) (err error)
	TableExists(tableFName string) (exists bool, err error)
	Template() Template
	Tx() *Transaction
	Unquote(string) string
	Upsert(srcTable string, tgtTable string, pkFields []string) (rowAffCnt int64, err error)
	ValidateColumnNames(tgtColName []string, colNames []string, quote bool) (newColNames []string, err error)
	Base() *BaseConn
}

// BaseConn is a database connection
type BaseConn struct {
	Connection
	URL         string
	Type        dbio.Type // the type of database for sqlx: postgres, mysql, sqlite
	db          *sqlx.DB
	tx          *Transaction
	Data        iop.Dataset
	defaultPort int
	instance    *Connection
	context     g.Context
	template    Template
	schemata    Schemata
	properties  map[string]string
	sshClient   *iop.SSHClient
	Log         []string
}

// Table represents a schemata table
type Table struct {
	Name       string `json:"name"`
	Schema     string `json:"schema"`
	FullName   string `json:"full_name"`
	IsView     bool   `json:"is_view"` // whether is a view
	Columns    iop.Columns
	ColumnsMap map[string]*iop.Column
}

// Schema represents a schemata schema
type Schema struct {
	Name   string `json:"name"`
	Tables map[string]Table
}

// Schemata contains the full schema for a connection
type Schemata struct {
	Schemas map[string]Schema
	Tables  map[string]*Table // all tables with full name lower case (schema.table)
}

// Template is a database YAML template
type Template struct {
	Core           map[string]string
	Metadata       map[string]string
	Analysis       map[string]string
	Function       map[string]string `yaml:"function"`
	GeneralTypeMap map[string]string `yaml:"general_type_map"`
	NativeTypeMap  map[string]string `yaml:"native_type_map"`
	NativeStatsMap map[string]bool   `yaml:"native_stat_map"`
	Variable       map[string]string
}

// Pool is a pool of connections
type Pool struct {
	Dbs map[string]*sqlx.DB
	Mux sync.Mutex
}

var (
	// UseBulkExportFlowCSV to use BulkExportFlowCSV
	UseBulkExportFlowCSV = false

	SampleSize = 900

	ddlDefDecScale  = 6
	ddlDefDecLength = 20

	ddlMaxDecLength = 30
	ddlMaxDecScale  = 9

	ddlMinDecScale = 4

	filePathStorageSlug = "temp"

	noTraceKey = " -- nT --"

	connPool = Pool{Dbs: map[string]*sqlx.DB{}}
	usePool  = os.Getenv("DBIO_USE_POOL") == "TRUE"
)

//go:embed templates/*
var templatesFolder embed.FS

func init() {
	if os.Getenv("DBIO_SAMPLE_SIZE") != "" {
		SampleSize = cast.ToInt(os.Getenv("DBIO_SAMPLE_SIZE"))
	}
	if os.Getenv("DBIO_FILEPATH_SLUG") != "" {
		filePathStorageSlug = os.Getenv("DBIO_FILEPATH_SLUG")
	}
}

// NewConn return the most proper connection for a given database
func NewConn(URL string, props ...string) (Connection, error) {
	return NewConnContext(context.Background(), URL, props...)
}

// NewConnContext return the most proper connection for a given database with context
// props are provided as `"Prop1=Value1", "Prop2=Value2", ...`
func NewConnContext(ctx context.Context, URL string, props ...string) (Connection, error) {
	var conn Connection

	// URL is actually just DB Name in this case
	// see TestEnvURL
	OrigURL := URL
	if newURL := os.Getenv(strings.TrimLeft(URL, "$")); newURL != "" {
		URL = newURL
	}

	if !strings.Contains(URL, ":") {
		err := g.Error("could not detect URL")
		return nil, g.Error(err, "invalid URL")
	}

	// Add / Extract Props from URL
	u, err := url.Parse(URL)
	if err == nil {
		for _, propStr := range strings.Split(u.RawQuery, `&`) {
			props = append(props, propStr)
			for _, p := range env.Vars() {
				if strings.HasPrefix(propStr, p+"=") {
					URL = strings.ReplaceAll(URL, propStr, "")
				}
			}
		}
	} else {
		return nil, g.Error(err, "could not parse URL")
	}

	concurrency := 10
	if strings.HasPrefix(URL, "postgres") {
		if strings.Contains(URL, "redshift.amazonaws.com") {
			conn = &RedshiftConn{URL: URL}
		} else {
			conn = &PostgresConn{URL: URL}
		}
	} else if strings.HasPrefix(URL, "redshift") {
		conn = &RedshiftConn{URL: URL}
	} else if strings.HasPrefix(URL, "sqlserver:") {
		conn = &MsSQLServerConn{URL: URL}
	} else if strings.HasPrefix(URL, "mysql:") {
		conn = &MySQLConn{URL: URL}
	} else if strings.HasPrefix(URL, "oracle:") {
		conn = &OracleConn{URL: URL}
		// concurrency = 2
	} else if strings.HasPrefix(URL, "bigquery:") {
		conn = &BigQueryConn{URL: URL}
	} else if strings.HasPrefix(URL, "clickhouse:") {
		conn = &ClickhouseConn{URL: URL}
	} else if strings.HasPrefix(URL, "snowflake") {
		conn = &SnowflakeConn{URL: URL}
	} else if strings.HasPrefix(URL, "file:") {
		conn = &SQLiteConn{URL: URL}
	} else {
		conn = &BaseConn{URL: URL}
	}
	conn.setContext(ctx, concurrency)

	// Add / Extract provided Props
	for _, propStr := range props {
		// g.Trace("setting connection prop -> " + propStr)
		arr := strings.Split(propStr, "=")
		if len(arr) == 1 && arr[0] != "" {
			conn.SetProp(arr[0], "")
		} else if len(arr) == 2 {
			conn.SetProp(arr[0], arr[1])
		} else if len(arr) > 2 {
			val := strings.Join(arr[1:], "=")
			conn.SetProp(arr[0], val)
		}
	}

	// Init
	conn.SetProp("orig_url", OrigURL)
	err = conn.Init()

	return conn, err
}

// GetSlingEnv return sling Env Data
func GetSlingEnv() map[string]string {
	slingEnvs := map[string]string{}

	for _, env := range os.Environ() {
		key := strings.Split(env, "=")[0]
		value := strings.ReplaceAll(env, key+"=", "")

		keyUpper := strings.ToUpper(key)
		if strings.HasPrefix(keyUpper, "DBIO_") {
			slingEnvs[keyUpper] = value
		}
	}

	return slingEnvs
}

func getDriverName(dbType dbio.Type) (driverName string) {
	switch dbType {
	case dbio.TypeDbPostgres, dbio.TypeDbRedshift:
		driverName = "postgres"
	case dbio.TypeDbMySQL:
		driverName = "mysql"
	case dbio.TypeDbOracle:
		driverName = "godror"
	case dbio.TypeDbBigQuery:
		driverName = "bigquery"
	case dbio.TypeDbSnowflake:
		driverName = "snowflake"
	case dbio.TypeDbSQLite:
		driverName = "sqlite3"
	case dbio.TypeDbSQLServer, dbio.TypeDbAzure:
		driverName = "sqlserver"
	default:
		driverName = dbType.String()
	}
	return
}

func getDialector(conn Connection) (driverDialector gorm.Dialector) {
	switch conn.GetType() {
	case dbio.TypeDbPostgres, dbio.TypeDbRedshift:
		driverDialector = postgres.Open(conn.BaseURL())
	case dbio.TypeDbSQLite:
		driverDialector = sqlite.Open(conn.BaseURL())
	default:
		g.LogError(g.Error("No Gorm Dialector found for %s", conn.GetType()))
	}
	return
}

// BaseURL returns the base Conn
func (conn *BaseConn) Base() *BaseConn {
	return conn
}

// BaseURL returns the base URL with default port
func (conn *BaseConn) BaseURL() string {
	return conn.URL
}

// GetURL returns the processed URL
func (conn *BaseConn) GetURL(newURL ...string) string {
	if len(newURL) > 0 {
		return newURL[0]
	}
	return conn.URL
}

// Init initiates the connection object & add default port if missing
func (conn *BaseConn) Init() (err error) {
	if conn.instance == nil {
		var instance Connection
		instance = conn
		conn.instance = &instance
	}

	// sets all the available env vars as default if not already set
	for k, v := range env.Vars() {
		if conn.GetProp(k) == "" {
			conn.SetProp(k, v)
		}
	}

	if conn.defaultPort != 0 {
		connU, err := url.Parse(conn.URL)
		if err != nil {
			return g.Error(err, "could not parse connection URL")
		}
		connHost := connU.Hostname()
		connPort := cast.ToInt(connU.Port())
		if connPort == 0 {
			conn.URL = strings.ReplaceAll(
				conn.URL, g.F("@%s", connHost),
				g.F("@%s:%d", connHost, conn.defaultPort),
			)
		}
	}

	err = conn.LoadTemplates()
	if err != nil {
		return err
	}
	conn.SetProp("connected", "false")
	return nil
}

func (conn *BaseConn) setContext(ctx context.Context, concurrency int) {
	conn.context = g.NewContext(ctx, concurrency)
}

// Self returns the respective connection Instance
// This is useful to refer back to a subclass method
// from the superclass level. (Aka overloading)
func (conn *BaseConn) Self() Connection {
	return *conn.instance
}

// Db returns the sqlx db object
func (conn *BaseConn) Db() *sqlx.DB {
	return conn.db
}

// DbX returns the DbX object
func (conn *BaseConn) DbX() *DbX {
	return &DbX{db: conn.db}
}

// Tx returns the current sqlx tx object
func (conn *BaseConn) Tx() *Transaction {
	return conn.tx
}

// GetType returns the type db object
func (conn *BaseConn) GetType() dbio.Type {
	return conn.Type
}

// Context returns the db context
func (conn *BaseConn) Context() *g.Context {
	return &conn.context
}

// Schemata returns the Schemata object
func (conn *BaseConn) Schemata() Schemata {
	return conn.schemata
}

// Template returns the Template object
func (conn *BaseConn) Template() Template {
	return conn.template
}

// GetProp returns the value of a property
func (conn *BaseConn) GetProp(key string) string {
	conn.context.Mux.Lock()
	defer conn.context.Mux.Unlock()
	return conn.properties[strings.ToLower(key)]
}

// SetProp sets the value of a property
func (conn *BaseConn) SetProp(key string, val string) {
	conn.context.Mux.Lock()
	defer conn.context.Mux.Unlock()
	if conn.properties == nil {
		conn.properties = map[string]string{}
	}
	conn.properties[strings.ToLower(key)] = val
}

// PropArr returns an array of properties
func (conn *BaseConn) PropArr() []string {
	props := []string{}
	for k, v := range conn.properties {
		props = append(props, g.F("%s=%s", k, v))
	}
	return props
}

// Props returns a map properties
func (conn *BaseConn) Props() map[string]string {
	return conn.properties
}

// Kill kill the database connection
func (conn *BaseConn) Kill() error {
	conn.context.Cancel()
	conn.SetProp("connected", "false")
	return nil
}

// Connect connects to the database
func (conn *BaseConn) Connect(timeOut ...int) (err error) {
	conn.schemata = Schemata{
		Schemas: map[string]Schema{},
		Tables:  map[string]*Table{},
	}

	to := 15
	if len(timeOut) > 0 && timeOut[0] != 0 {
		to = timeOut[0]
	}

	usePool = os.Getenv("DBIO_USE_POOL") == "TRUE"
	// g.Trace("conn.Type: %s", conn.Type)
	// g.Trace("conn.URL: " + conn.Self().GetURL())
	if conn.Type == "" {
		return g.Error("Invalid URL? conn.Type needs to be specified")
	}

	connURL := conn.URL
	// start SSH Tunnel with SSH_TUNNEL prop
	if sshURL := conn.GetProp("SSH_TUNNEL"); sshURL != "" {
		sshU, err := url.Parse(sshURL)
		if err != nil {
			return g.Error(err, "could not parse SSH_TUNNEL URL")
		}

		connU, err := url.Parse(connURL)
		if err != nil {
			return g.Error(err, "could not parse connection URL for SSH forwarding")
		}

		sshHost := sshU.Hostname()
		sshPort := cast.ToInt(sshU.Port())
		if sshPort == 0 {
			sshPort = 22
		}
		sshUser := sshU.User.Username()
		sshPassword, _ := sshU.User.Password()

		connHost := connU.Hostname()
		connPort := cast.ToInt(connU.Port())
		if connPort == 0 {
			connPort = conn.defaultPort
			connURL = strings.ReplaceAll(
				connURL, g.F("@%s", connHost),
				g.F("@%s:%d", connHost, connPort),
			)
		}

		conn.sshClient = &iop.SSHClient{
			Host:       sshHost,
			Port:       sshPort,
			User:       sshUser,
			Password:   sshPassword,
			TgtHost:    connHost,
			TgtPort:    connPort,
			PrivateKey: conn.GetProp("SSH_PRIVATE_KEY"),
		}

		localPort, err := conn.sshClient.OpenPortForward()
		if err != nil {
			return g.Error(err, "could not connect to ssh server")
		}

		connURL = strings.ReplaceAll(
			connURL, g.F("@%s:%d", connHost, connPort),
			g.F("@127.0.0.1:%d", localPort),
		)
		g.Trace("new connection URL: " + conn.Self().GetURL(connURL))
	}

	if conn.Type != dbio.TypeDbBigQuery && conn.tx == nil {
		connURL = conn.Self().GetURL(connURL)
		connPool.Mux.Lock()
		db, poolOk := connPool.Dbs[connURL]
		connPool.Mux.Unlock()

		if !usePool || !poolOk {
			db, err = sqlx.Open(getDriverName(conn.Type), connURL)
			if err != nil {
				return g.Error(err, "Could not connect to DB: "+getDriverName(conn.Type))
			}
		} else {
			conn.SetProp("POOL_USED", cast.ToString(poolOk))
		}

		conn.db = db

		// 15 sec timeout
		pingCtx, cancel := context.WithTimeout(conn.Context().Ctx, time.Duration(to)*time.Second)
		_ = cancel // lint complaint
		_ = pingCtx

		err = conn.db.PingContext(pingCtx)
		if err != nil {
			return g.Error(err, "Could not ping DB")
		} else if !usePool {
			g.Info(`connected to %s`, conn.Type)
		}

		// add to pool after successful connection
		if usePool && !poolOk {
			connPool.Mux.Lock()
			connPool.Dbs[connURL] = db
			connPool.Mux.Unlock()

			// expire the connection from pool after 10 minutes of
			timer := time.NewTimer(time.Duration(10*60) * time.Second)
			go func() {
				select {
				case <-timer.C:
					connPool.Mux.Lock()
					delete(connPool.Dbs, connURL)
					connPool.Mux.Unlock()
				}
			}()
		}
	}

	conn.SetProp("connected", "true")

	return nil
}

// Close closes the connection
func (conn *BaseConn) Close() error {
	var err error
	if conn.db != nil {
		err = conn.db.Close()
		g.LogError(err)
	}
	if conn.sshClient != nil {
		conn.sshClient.Close()
	}
	return err
}

// AddLog logs a text for debugging
func (conn *BaseConn) AddLog(text string) {
	conn.Log = append(conn.Log, text)
	if len(conn.Log) > 300 {
		conn.Log = conn.Log[1:]
	}
}

// GetGormConn returns the gorm db connection
func (conn *BaseConn) GetGormConn(config *gorm.Config) (*gorm.DB, error) {
	return gorm.Open(getDialector(conn), config)
}

// GetTemplateValue returns the value of the path
func (conn *BaseConn) GetTemplateValue(path string) (value string) {

	prefixes := map[string]map[string]string{
		"core.":             conn.template.Core,
		"analysis.":         conn.template.Analysis,
		"function.":         conn.template.Function,
		"metadata.":         conn.template.Metadata,
		"general_type_map.": conn.template.GeneralTypeMap,
		"native_type_map.":  conn.template.NativeTypeMap,
		"variable.":         conn.template.Variable,
	}

	for prefix, dict := range prefixes {
		if strings.HasPrefix(path, prefix) {
			key := strings.Replace(path, prefix, "", 1)
			value = dict[key]
			break
		}
	}

	return value
}

// ToData convert is dataset
func (template Template) ToData() (data iop.Dataset) {
	columns := []string{"key", "value"}
	data = iop.NewDataset(iop.NewColumnsFromFields(columns...))
	data.Rows = append(data.Rows, []interface{}{"core", template.Core})
	data.Rows = append(data.Rows, []interface{}{"analysis", template.Analysis})
	data.Rows = append(data.Rows, []interface{}{"function", template.Function})
	data.Rows = append(data.Rows, []interface{}{"metadata", template.Metadata})
	data.Rows = append(data.Rows, []interface{}{"general_type_map", template.GeneralTypeMap})
	data.Rows = append(data.Rows, []interface{}{"native_type_map", template.NativeTypeMap})
	data.Rows = append(data.Rows, []interface{}{"variable", template.Variable})

	return
}

// LoadTemplates loads the appropriate yaml template
func (conn *BaseConn) LoadTemplates() error {
	conn.template = Template{
		Core:           map[string]string{},
		Metadata:       map[string]string{},
		Analysis:       map[string]string{},
		Function:       map[string]string{},
		GeneralTypeMap: map[string]string{},
		NativeTypeMap:  map[string]string{},
		NativeStatsMap: map[string]bool{},
		Variable:       map[string]string{},
	}

	baseTemplateBytes, err := templatesFolder.ReadFile("templates/base.yaml")
	if err != nil {
		return g.Error(err, "ioutil.ReadAll(baseTemplateFile)")
	}

	if err := yaml.Unmarshal([]byte(baseTemplateBytes), &conn.template); err != nil {
		return g.Error(err, "yaml.Unmarshal")
	}

	templateBytes, err := templatesFolder.ReadFile("templates/" + conn.Type.String() + ".yaml")
	if err != nil {
		return g.Error(err, "ioutil.ReadAll(templateFile) for "+conn.Type)
	}

	template := Template{}
	err = yaml.Unmarshal([]byte(templateBytes), &template)
	if err != nil {
		return g.Error(err, "yaml.Unmarshal")
	}

	for key, val := range template.Core {
		conn.template.Core[key] = val
	}

	for key, val := range template.Analysis {
		conn.template.Analysis[key] = val
	}

	for key, val := range template.Function {
		conn.template.Function[key] = val
	}

	for key, val := range template.Metadata {
		conn.template.Metadata[key] = val
	}

	for key, val := range template.Variable {
		conn.template.Variable[key] = val
	}

	TypesNativeFile, err := templatesFolder.Open("templates/types_native_to_general.tsv")
	if err != nil {
		return g.Error(err, `cannot open types_native_to_general`)
	}

	TypesNativeCSV := iop.CSV{Reader: bufio.NewReader(TypesNativeFile)}
	TypesNativeCSV.Delimiter = '\t'
	TypesNativeCSV.NoTrace = true

	data, err := TypesNativeCSV.Read()
	if err != nil {
		return g.Error(err, `TypesNativeCSV.Read()`)
	}

	for _, rec := range data.Records() {
		if rec["database"] == conn.Type.String() {
			nt := strings.TrimSpace(cast.ToString(rec["native_type"]))
			gt := strings.TrimSpace(cast.ToString(rec["general_type"]))
			s := strings.TrimSpace(cast.ToString(rec["stats_allowed"]))
			conn.template.NativeTypeMap[nt] = cast.ToString(gt)
			conn.template.NativeStatsMap[nt] = cast.ToBool(s)
		}
	}

	TypesGeneralFile, err := templatesFolder.Open("templates/types_general_to_native.tsv")
	if err != nil {
		return g.Error(err, `cannot open types_general_to_native`)
	}

	TypesGeneralCSV := iop.CSV{Reader: bufio.NewReader(TypesGeneralFile)}
	TypesGeneralCSV.Delimiter = '\t'
	TypesGeneralCSV.NoTrace = true

	data, err = TypesGeneralCSV.Read()
	if err != nil {
		return g.Error(err, `TypesGeneralCSV.Read()`)
	}

	for _, rec := range data.Records() {
		gt := strings.TrimSpace(cast.ToString(rec["general_type"]))
		conn.template.GeneralTypeMap[gt] = cast.ToString(rec[conn.Type.String()])
	}

	return nil
}

// StreamRecords the records of a sql query, returns `result`, `error`
func (conn *BaseConn) StreamRecords(sql string) (<-chan map[string]interface{}, error) {

	ds, err := conn.Self().StreamRows(sql)
	if err != nil {
		err = g.Error(err, "error in StreamRowsContext")
	}
	return ds.Records(), nil
}

// BulkExportStream streams the rows in bulk
func (conn *BaseConn) BulkExportStream(sql string) (ds *iop.Datastream, err error) {
	g.Trace("BulkExportStream not implemented for %s", conn.Type)
	return conn.Self().StreamRows(sql)
}

// BulkImportStream import the stream rows in bulk
func (conn *BaseConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	g.Trace("BulkImportStream not implemented for %s", conn.Type)
	return conn.Self().InsertBatchStream(tableFName, ds)
}

// StreamRows the rows of a sql query, returns `result`, `error`
func (conn *BaseConn) StreamRows(sql string, limit ...int) (ds *iop.Datastream, err error) {
	return conn.Self().StreamRowsContext(conn.Context().Ctx, sql, limit...)
}

// StreamRowsContext streams the rows of a sql query with context, returns `result`, `error`
func (conn *BaseConn) StreamRowsContext(ctx context.Context, sql string, limit ...int) (ds *iop.Datastream, err error) {
	Limit := uint64(0) // infinite
	if len(limit) > 0 && limit[0] != 0 {
		Limit = cast.ToUint64(limit[0])
	}

	start := time.Now()
	if strings.TrimSpace(sql) == "" {
		return ds, g.Error("Empty Query")
	}

	queryContext := g.NewContext(ctx)

	conn.AddLog(sql)
	var result *sqlx.Rows
	if conn.tx != nil {
		result, err = conn.tx.QueryContext(queryContext.Ctx, sql)
	} else {
		if !strings.Contains(sql, noTraceKey) {
			g.Debug(sql)
		}
		result, err = conn.db.QueryxContext(queryContext.Ctx, sql)
	}
	if err != nil {
		queryContext.Cancel()
		return ds, g.Error(err, "SQL Error for:\n"+sql)
	}

	colTypes, err := result.ColumnTypes()
	if err != nil {
		queryContext.Cancel()
		return ds, g.Error(err, "result.ColumnTypes()")
	}

	conn.Data.Result = result
	conn.Data.SQL = sql
	conn.Data.Duration = time.Since(start).Seconds()
	conn.Data.Rows = [][]interface{}{}
	conn.Data.Columns = SQLColumns(colTypes, conn.template.NativeTypeMap)
	conn.Data.NoTrace = !strings.Contains(sql, noTraceKey)

	g.Trace("query responded in %f secs", conn.Data.Duration)

	nextFunc := func(it *iop.Iterator) bool {
		if Limit > 0 && it.Counter >= Limit {
			queryContext.Cancel()
			result.Next()
			result.Close()
			return false
		}

		next := result.Next()
		if next {
			// add row
			it.Row, err = result.SliceScan()
			if err != nil {
				it.Context.CaptureErr(g.Error(err, "failed to scan"))
			} else {
				return true
			}
		}

		result.Close()

		// if any error occurs during iteration
		if result.Err() != nil {
			it.Context.CaptureErr(g.Error(result.Err(), "error during iteration"))
		}
		return false
	}

	ds = iop.NewDatastreamIt(queryContext.Ctx, conn.Data.Columns, nextFunc)
	ds.NoTrace = !strings.Contains(sql, noTraceKey)
	ds.Inferred = true

	err = ds.Start()
	if err != nil {
		queryContext.Cancel()
		return ds, g.Error(err, "could start datastream")
	}
	return
}

// NewTransaction creates a new transaction
func (conn *BaseConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (*Transaction, error) {
	context := g.NewContext(ctx)

	if len(options) == 0 {
		options = []*sql.TxOptions{&sql.TxOptions{}}
	}

	tx, err := conn.Db().BeginTxx(context.Ctx, options[0])
	if err != nil {
		return nil, g.Error(err, "could not begin Tx")
	}

	// a cloned connection object
	// this will enable to use transaction across all sub functions
	URL := conn.GetProp("orig_url")
	c, err := NewConnContext(context.Ctx, URL, conn.PropArr()...)
	if err != nil {
		return nil, g.Error(err, "could not clone conn object")
	}

	Tx := &Transaction{Tx: tx, Conn: c.Self(), Context: &context}
	c.SetTx(Tx)

	err = c.Connect()
	if err != nil {
		return nil, g.Error(err, "could not connect cloned conn object")
	}

	return Tx, nil
}

// SetTx sets the transaction
func (conn *BaseConn) SetTx(tx *Transaction) {
	conn.tx = tx
}

// Begin starts a connection wide transaction
func (conn *BaseConn) Begin(options ...*sql.TxOptions) (err error) {
	return conn.BeginContext(conn.Context().Ctx, options...)
}

// BeginContext starts a connection wide transaction
func (conn *BaseConn) BeginContext(ctx context.Context, options ...*sql.TxOptions) (err error) {
	if conn.Db() == nil {
		return
	}

	g.Trace("begin")
	tx, err := conn.NewTransaction(ctx, options...)
	if err != nil {
		return g.Error(err, "could not create transaction")
	}

	conn.tx = tx
	return
}

// Commit commits a connection wide transaction
func (conn *BaseConn) Commit() (err error) {
	if conn.tx == nil {
		return
	}

	conn.AddLog("COMMIT")
	select {
	case <-conn.tx.Context.Ctx.Done():
		err = conn.tx.Context.Err()
		conn.Rollback()
		return
	default:
		err = conn.tx.Commit()
		conn.tx = nil
		if err != nil {
			return g.Error(err, "Could not commit")
		}
	}

	return nil
}

// Rollback rolls back a connection wide transaction
func (conn *BaseConn) Rollback() (err error) {
	if conn.tx != nil {
		conn.AddLog("ROLLBACK")
		err = conn.tx.Rollback()
		conn.tx = nil
	}
	if err != nil {
		return g.Error(err, "Could not rollback")
	}
	return nil
}

// Prepare prepares the statement
func (conn *BaseConn) Prepare(query string) (stmt *sql.Stmt, err error) {
	if conn.tx != nil {
		stmt, err = conn.tx.Prepare(query)
	} else {
		stmt, err = conn.db.PrepareContext(conn.Context().Ctx, query)
	}
	if err != nil {
		err = g.Error(err, "could not prepare statement")
	}
	return
}

// Exec runs a sql query, returns `error`
func (conn *BaseConn) Exec(sql string, args ...interface{}) (result sql.Result, err error) {
	if conn.GetProp("connected") != "true" {
		err = conn.Self().Connect()
		if err != nil {
			err = g.Error(err, "Could not connect")
			return
		}
	}
	result, err = conn.Self().ExecContext(conn.Context().Ctx, sql, args...)
	if err != nil {
		err = g.Error(err, "Could not execute SQL")
	}
	return
}

// ExecMulti runs mutiple sql queries, returns `error`
func (conn *BaseConn) ExecMulti(sql string, args ...interface{}) (result sql.Result, err error) {
	if conn.GetProp("connected") != "true" {
		err = conn.Self().Connect()
		if err != nil {
			err = g.Error(err, "Could not connect")
			return
		}
	}
	result, err = conn.Self().ExecMultiContext(conn.Context().Ctx, sql, args...)
	if err != nil {
		err = g.Error(err, "Could not execute SQL")
	}
	return
}

// ExecContext runs a sql query with context, returns `error`
func (conn *BaseConn) ExecContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {

	if strings.TrimSpace(q) == "" {
		g.Warn("Empty Query")
		return
	}

	conn.AddLog(q)
	if conn.tx != nil {
		result, err = conn.tx.ExecContext(ctx, q, args...)
	} else {
		if !strings.Contains(q, noTraceKey) {
			g.Debug(CleanSQL(conn, q), args...)
		}
		result, err = conn.db.ExecContext(ctx, q, args...)
	}
	if err != nil {
		err = g.Error(err, "Error executing "+CleanSQL(conn, q))
	}
	return
}

// ExecMultiContext runs multiple sql queries with context, returns `error`
func (conn *BaseConn) ExecMultiContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {

	Res := Result{rowsAffected: 0}

	eG := g.ErrorGroup{}
	for _, sql := range ParseSQLMultiStatements(q) {
		conn.AddLog(sql)
		res, err := conn.ExecContext(ctx, sql, args...)
		if err != nil {
			eG.Capture(g.Error(err, "Error executing query"))
		} else {
			ra, _ := res.RowsAffected()
			Res.rowsAffected = Res.rowsAffected + ra
		}
	}

	err = eG.Err()
	result = Res

	return
}

// MustExec execs the query using e and panics if there was an error.
// Any placeholder parameters are replaced with supplied args.
func (conn *BaseConn) MustExec(sql string, args ...interface{}) (result sql.Result) {
	res, err := conn.Self().Exec(sql, args...)
	if err != nil {
		g.LogFatal(err, "fatal query error")
	}
	return res
}

// Query runs a sql query, returns `result`, `error`
func (conn *BaseConn) Query(sql string, limit ...int) (data iop.Dataset, err error) {
	if conn.GetProp("connected") != "true" {
		err = conn.Self().Connect()
		if err != nil {
			err = g.Error(err, "Could not connect")
			return
		}
	}

	ds, err := conn.Self().StreamRows(sql, limit...)
	if err != nil {
		err = g.Error(err, "Error with StreamRows")
		return iop.Dataset{SQL: sql}, err
	}

	data, err = ds.Collect(0)
	data.SQL = sql
	data.Duration = conn.Data.Duration // Collect does not time duration

	return data, err
}

// QueryContext runs a sql query with ctx, returns `result`, `error`
func (conn *BaseConn) QueryContext(ctx context.Context, sql string, limit ...int) (iop.Dataset, error) {

	ds, err := conn.Self().StreamRowsContext(ctx, sql, limit...)
	if err != nil {
		return iop.Dataset{SQL: sql}, err
	}

	data, err := ds.Collect(0)
	data.SQL = sql
	data.Duration = conn.Data.Duration // Collect does not time duration

	return data, err
}

// SplitTableFullName retrusn the schema / table name
func SplitTableFullName(tableName string) (string, string) {
	var (
		schema string
		table  string
	)

	a := strings.Split(tableName, ".")
	if len(a) == 2 {
		schema = a[0]
		table = a[1]
	} else if len(a) == 1 {
		schema = ""
		table = a[0]
	}
	return schema, table
}

func (conn *BaseConn) SumbitTemplate(level string, templateMap map[string]string, name string, values map[string]interface{}) (data iop.Dataset, err error) {
	template, ok := templateMap[name]
	if !ok {
		err = g.Error("Could not find template %s", name)
		return
	}

	template = template + noTraceKey
	sql, err := conn.ProcessTemplate(level, template, values)
	if err != nil {
		err = g.Error("error processing template")
		return
	}
	return conn.Self().Query(sql)
}

// GetCount returns count of records
func (conn *BaseConn) GetCount(tableFName string) (uint64, error) {
	sql := fmt.Sprintf(`select count(1) cnt from %s`, tableFName)
	data, err := conn.Self().Query(sql)
	if err != nil {
		return 0, err
	}
	return cast.ToUint64(data.Rows[0][0]), nil
}

// GetSchemas returns schemas
func (conn *BaseConn) GetSchemas() (iop.Dataset, error) {
	// fields: [schema_name]
	return conn.SumbitTemplate(
		"single", conn.template.Metadata, "schemas",
		g.M(),
	)
}

// GetObjects returns objects (tables or views) for given schema
// `objectType` can be either 'table', 'view' or 'all'
func (conn *BaseConn) GetObjects(schema string, objectType string) (iop.Dataset, error) {
	return conn.SumbitTemplate(
		"single", conn.template.Metadata, "objects",
		g.M("schema", schema, "object_type", objectType),
	)
}

// GetTables returns tables for given schema
func (conn *BaseConn) GetTables(schema string) (iop.Dataset, error) {
	// fields: [table_name]
	return conn.SumbitTemplate(
		"single", conn.template.Metadata, "tables",
		g.M("schema", schema),
	)
}

// GetViews returns views for given schema
func (conn *BaseConn) GetViews(schema string) (iop.Dataset, error) {
	// fields: [table_name]
	return conn.SumbitTemplate(
		"single", conn.template.Metadata, "views",
		g.M("schema", schema),
	)
}

// CommonColumns return common columns
func CommonColumns(colNames1 []string, colNames2 []string) (commCols []string) {
	commCols = []string{}
	cols2 := g.ArrMapString(colNames2, true)
	for _, col := range colNames1 {
		if _, ok := cols2[strings.ToLower(col)]; ok {
			commCols = append(commCols, col)
		}
	}
	return
}

// SQLColumns returns the columns from database ColumnType
func SQLColumns(colTypes []*sql.ColumnType, NativeTypeMap map[string]string) (columns iop.Columns) {
	columns = make(iop.Columns, len(colTypes))

	for i, colType := range colTypes {
		dbType := strings.ToLower(colType.DatabaseTypeName())
		dbType = strings.Split(dbType, "(")[0]

		Type := dbType
		if _, ok := NativeTypeMap[dbType]; ok {
			Type = NativeTypeMap[dbType]
		} else if Type != "" {
			g.Warn("type '%s' not mapped for col '%s': %#v", dbType, colType.Name(), colType)
			Type = "string" // default as string
		}

		col := iop.Column{
			Name:     colType.Name(),
			Position: i + 1,
			Type:     Type,
			DbType:   dbType,
			ColType:  colType,
		}

		length, scale, ok := int64(0), int64(0), false
		if col.IsString() {
			length, ok = colType.Length()
		} else if col.IsNumber() {
			length, scale, ok = colType.DecimalSize()
		}

		if length == math.MaxInt64 {
			length = math.MaxInt32
		}

		col.Stats.MaxLen = cast.ToInt(length)
		col.Stats.MaxDecLen = cast.ToInt(scale)
		col.Sourced = ok
		col.Sourced = false // TODO: cannot use sourced length/scale, unreliable.

		// g.Trace("col %s (%s -> %s) has %d length, %d scale, sourced: %t", colType.Name(), colType.DatabaseTypeName(), Type, length, scale, ok)

		columns[i] = col

		// g.Trace("%s -> %s (%s)", colType.Name(), Type, dbType)
	}
	return columns
}

// GetSQLColumns return columns from a sql query result
func (conn *BaseConn) GetSQLColumns(sqls ...string) (columns iop.Columns, err error) {
	sql := ""
	if len(sqls) > 0 {
		sql = sqls[0]
	} else {
		err = g.Error("no query provided")
		return
	}

	// add 1=0
	// sql = g.R(conn.GetTemplateValue("core.column_names"), "sql", sql)
	if !strings.Contains(sql, " ") {
		// is table
		sql = g.R(conn.GetTemplateValue("core.limit"), "table", sql, "fields", "*", "limit", "1")
	}

	// get column types
	g.Trace("GetSQLColumns: %s", sql)
	sql = sql + " /* GetSQLColumns */ " + noTraceKey
	ds, err := conn.Self().StreamRows(sql, 1)
	if err != nil {
		err = g.Error(err, "SQL Error for:\n"+sql)
		return columns, err
	}

	err = ds.WaitReady()
	if err != nil {
		err = g.Error(err, "Datastream Error ")
		return columns, err
	}

	ds.Collect(0) // advance the datastream so it can close
	return ds.Columns, nil
}

// TableExists returns true if the table exists
func (conn *BaseConn) TableExists(tableFName string) (exists bool, err error) {

	schema, table := SplitTableFullName(tableFName)
	colData, err := conn.SumbitTemplate(
		"single", conn.template.Metadata, "columns",
		g.M("schema", schema, "table", table),
	)
	if err != nil {
		return false, g.Error(err, "could not check table existence: "+tableFName)
	}

	if len(colData.Rows) > 0 {
		exists = true
	}
	return
}

// GetColumns returns columns for given table. `tableFName` should
// include schema and table, example: `schema1.table2`
// fields should be `column_name|data_type`
func (conn *BaseConn) GetColumns(tableFName string, fields ...string) (columns iop.Columns, err error) {
	columns = iop.Columns{}
	schema, table := SplitTableFullName(tableFName)
	colData, err := conn.SumbitTemplate(
		"single", conn.template.Metadata, "columns",
		g.M("schema", schema, "table", table),
	)
	if err != nil {
		return columns, g.Error(err, "could not get list of columns for table: "+tableFName)
	}

	// if fields provided, check if exists in table
	colMap := map[string]string{}
	fieldMap := map[string]string{}
	for _, rec := range colData.Records() {
		colName := cast.ToString(rec["column_name"])
		colMap[strings.ToLower(colName)] = colName
	}
	for _, field := range fields {
		_, ok := colMap[strings.ToLower(field)]
		if !ok {
			err = g.Error(
				"provided field '%s' not found in table %s",
				strings.ToLower(field), tableFName,
			)
			return
		}
		fieldMap[strings.ToLower(field)] = colMap[strings.ToLower(field)]
	}

	for i, rec := range colData.Records() {
		dType := cast.ToString(rec["data_type"])
		dType = strings.Split(strings.ToLower(dType), "(")[0]
		generalType, ok := conn.Template().NativeTypeMap[dType]
		if !ok {
			err = g.Error(
				"No general type mapping defined for col '%s', with type '%s' for '%s'",
				rec["column_name"],
				dType,
				conn.GetType(),
			)
			return
		}

		column := iop.Column{
			Position:    i + 1,
			Name:        cast.ToString(rec["column_name"]),
			Type:        generalType,
			DbType:      cast.ToString(rec["data_type"]),
			DbPrecision: cast.ToInt(rec["precision"]),
			DbScale:     cast.ToInt(rec["scale"]),
		}
		if len(fields) > 0 {
			_, ok := fieldMap[strings.ToLower(column.Name)]
			if !ok {
				continue
			}
		}
		columns = append(columns, column)
	}

	if len(columns) == 0 {
		err = g.Error("unable to obtain columns for " + tableFName)
	}

	return
}

// GetColumnsFull returns columns for given table. `tableName` should
// include schema and table, example: `schema1.table2`
// fields should be `schema_name|table_name|table_type|column_name|data_type|column_id`
func (conn *BaseConn) GetColumnsFull(tableFName string) (iop.Dataset, error) {
	schema, table := SplitTableFullName(tableFName)
	return conn.SumbitTemplate(
		"single", conn.template.Metadata, "columns_full",
		g.M("schema", schema, "table", table),
	)
}

// GetPrimaryKeys returns primark keys for given table.
func (conn *BaseConn) GetPrimaryKeys(tableFName string) (iop.Dataset, error) {
	schema, table := SplitTableFullName(tableFName)
	return conn.SumbitTemplate(
		"single", conn.template.Metadata, "primary_keys",
		g.M("schema", schema, "table", table),
	)
}

// GetIndexes returns indexes for given table.
func (conn *BaseConn) GetIndexes(tableFName string) (iop.Dataset, error) {
	schema, table := SplitTableFullName(tableFName)
	return conn.SumbitTemplate(
		"single", conn.template.Metadata, "indexes",
		g.M("schema", schema, "table", table),
	)
}

// GetDDL returns DDL for given table.
func (conn *BaseConn) GetDDL(tableFName string) (string, error) {
	schema, table := SplitTableFullName(tableFName)
	ddlCol := cast.ToInt(conn.template.Variable["ddl_col"])
	ddlArr := []string{}
	data, err := conn.SumbitTemplate(
		"single", conn.template.Metadata, "ddl_view",
		g.M("schema", schema, "table", table),
	)

	for _, row := range data.Rows {
		ddlArr = append(ddlArr, cast.ToString(row[ddlCol]))
	}

	ddl := strings.TrimSpace(strings.Join(ddlArr, "\n"))
	if err == nil && ddl != "" {
		return ddl, err
	}

	data, err = conn.SumbitTemplate(
		"single", conn.template.Metadata, "ddl_table",
		g.M("schema", schema, "table", table),
	)
	if err != nil {
		return "", err
	}

	for _, row := range data.Rows {
		ddlArr = append(ddlArr, cast.ToString(row[ddlCol]))
	}

	ddl = strings.TrimSpace(strings.Join(ddlArr, "\n"))
	return ddl, nil
}

// CreateTemporaryTable creates a temp table based on provided columns
func (conn *BaseConn) CreateTemporaryTable(tableName string, cols iop.Columns) (err error) {
	// generate ddl
	tableDDL, err := conn.GenerateDDL(tableName, cols.Dataset(), true)
	if err != nil {
		return g.Error(err, "Could not generate DDL for "+tableName)
	}

	// execute ddl
	_, err = conn.Exec(tableDDL)
	if err != nil {
		return g.Error(err, "Could not create table "+tableName)
	}

	return
}

// CreateTable creates a new table based on provided columns
// `tableName` should have 'schema.table' format
func (conn *BaseConn) CreateTable(tableName string, cols iop.Columns, tableDDL string) (err error) {

	// check table existence
	exists, err := conn.TableExists(tableName)
	if err != nil {
		return g.Error(err, "Error checking table "+tableName)
	} else if exists {
		return nil
	}

	// generate ddl
	if tableDDL == "" {
		tableDDL, err = conn.GenerateDDL(tableName, cols.Dataset(), false)
		if err != nil {
			return g.Error(err, "Could not generate DDL for "+tableName)
		}
	}

	// execute ddl
	_, err = conn.Exec(tableDDL)
	if err != nil {
		return g.Error(err, "Could not create table "+tableName)
	}

	return
}

// DropTable drops given table.
func (conn *BaseConn) DropTable(tableNames ...string) (err error) {

	for _, tableName := range tableNames {
		sql := g.R(conn.template.Core["drop_table"], "table", tableName)
		_, err = conn.Self().Exec(sql)
		if err != nil {
			errIgnoreWord := conn.template.Variable["error_ignore_drop_table"]
			if !(errIgnoreWord != "" && strings.Contains(cast.ToString(err), errIgnoreWord)) {
				return g.Error(err, "Error for "+sql)
			}
			g.Debug("table %s does not exist", tableName)
		} else {
			g.Debug("table %s dropped", tableName)
		}
	}
	return nil
}

// DropView drops given view.
func (conn *BaseConn) DropView(viewNames ...string) (err error) {

	for _, viewName := range viewNames {
		sql := g.R(conn.template.Core["drop_view"], "view", viewName)
		_, err = conn.Self().Exec(sql)
		if err != nil {
			errIgnoreWord := conn.template.Variable["error_ignore_drop_view"]
			if !(errIgnoreWord != "" && strings.Contains(cast.ToString(err), errIgnoreWord)) {
				return g.Error(err, "Error for "+sql)
			}
			g.Debug("view %s does not exist", viewName)
		} else {
			g.Debug("view %s dropped", viewName)
		}
	}
	return nil
}

// Import imports `data` into `tableName`
func (conn *BaseConn) Import(data iop.Dataset, tableName string) error {

	return nil
}

// GetSchemata obtain full schemata info for a schema
func (conn *BaseConn) GetSchemata(schemaName, tableName string) (Schemata, error) {

	schemata := Schemata{
		Schemas: map[string]Schema{},
		Tables:  map[string]*Table{},
	}

	values := g.M()
	if schemaName != "" {
		values["schema"] = schemaName
	}
	if tableName != "" {
		values["table"] = tableName
	}

	schemaData, err := conn.SumbitTemplate(
		"single", conn.template.Metadata, "schemata",
		values,
	)

	if err != nil {
		return schemata, g.Error(err, "Could not GetSchemata for "+schemaName)
	}

	for _, rec := range schemaData.Records() {
		schemaName = cast.ToString(rec["schema_name"])
		tableName := cast.ToString(rec["table_name"])

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
			Name:       tableName,
			Schema:     schemaName,
			FullName:   schemaName + "." + tableName,
			IsView:     cast.ToBool(rec["is_view"]),
			Columns:    iop.Columns{},
			ColumnsMap: map[string]*iop.Column{},
		}

		if _, ok := schemata.Schemas[schema.Name]; ok {
			schema = schemata.Schemas[schema.Name]
		}

		if _, ok := schemata.Schemas[schemaName].Tables[tableName]; ok {
			table = schemata.Schemas[schemaName].Tables[tableName]
		}

		column := iop.Column{
			Position: cast.ToInt(schemaData.Sp.ProcessVal(rec["position"])),
			Name:     cast.ToString(rec["column_name"]),
			Type:     cast.ToString(rec["data_type"]),
			DbType:   cast.ToString(rec["data_type"]),
		}

		table.Columns = append(table.Columns, column)
		table.ColumnsMap[column.Name] = &column

		schema.Tables[tableName] = table
		schemata.Schemas[schema.Name] = schema
		schemata.Tables[table.FullName] = &table

	}

	// conn.schemata.Schemas[schemaName] = schema

	return schemata, nil
}

// RunAnalysis runs an analysis
func (conn *BaseConn) RunAnalysis(analysisName string, values map[string]interface{}) (data iop.Dataset, err error) {
	sql, err := conn.Self().GetAnalysis(analysisName, values)
	if err != nil {
		err = g.Error(err, "could not run analysis %s", analysisName)
		return
	}
	return conn.Self().Query(sql)
}

// ProcessTemplate processes a template SQL text at a given level
func (conn *BaseConn) ProcessTemplate(level, text string, values map[string]interface{}) (sql string, err error) {

	sqls := []string{}

	getColumns := func(columns []string) (cols iop.Columns) {
		cols = iop.NewColumnsFromFields(columns...)
		if len(cols) == 0 {
			// get all columns then
			tableFName := g.F("%s.%s", values["schema"], values["table"])
			cols, err = conn.GetSQLColumns(tableFName)
			if err != nil {
				err = g.Error(err, "could not obtain table columns")
			}
		}
		return
	}

	text, err = g.ExecuteTemplate(text, values)
	if err != nil {
		err = g.Error(err, "error execute template")
		return
	}

	switch level {
	case "single":
		sql = g.Rm(text, values)
	case "schema-union":
		schemas, ok := values["schemas"]
		if !ok {
			err = g.Error("missing 'schemas' key")
		} else {
			for _, schema := range cast.ToSlice(schemas) {
				values["schema"] = schema
				sqls = append(sqls, g.Rm(text, values))
			}
			sql = strings.Join(sqls, "\nUNION ALL\n")
		}
	case "table-union":
		tableFNames, ok := values["tables"]
		if !ok {
			err = g.Error("missing 'tables' key")
		} else {
			for _, tableFName := range cast.ToStringSlice(tableFNames) {
				schema, table := SplitTableFullName(tableFName)
				values["schema"] = schema
				values["table"] = table
				sqls = append(sqls, g.Rm(text, values))
			}
			sql = strings.Join(sqls, "\nUNION ALL\n")
		}
	case "column-union":
		columns, ok := values["columns"]
		if !ok {
			err = g.Error("missing 'columns' key")
		} else {
			for _, col := range getColumns(cast.ToStringSlice(columns)) {
				values["column"] = col.Name
				values["field"] = col.Name
				values["type"] = col.Type
				sqls = append(sqls, g.Rm(text, values))
			}
			sql = strings.Join(sqls, "\nUNION ALL\n")
		}
	case "column-expres":
		columns, ok1 := values["columns"]
		expr, ok2 := values["expr"]
		if !ok1 || !ok2 {
			err = g.Error("missing 'columns' or 'expr' key")
		} else {
			if !strings.Contains(cast.ToString(expr), "{column}") {
				err = g.Error("\"expr\" value must contain '{column}'")
				return
			}
			exprs := []string{}
			for _, col := range getColumns(cast.ToStringSlice(columns)) {
				values["column"] = col.Name
				values["field"] = col.Name
				values["type"] = col.Type
				exprVal := g.Rm(cast.ToString(expr), values)
				exprs = append(exprs, exprVal)
			}
			values["columns_sql"] = strings.Join(exprs, ", ")
			sql = g.Rm(text, values)
		}
	}

	return
}

// GetAnalysis runs an analysis
func (conn *BaseConn) GetAnalysis(analysisName string, values map[string]interface{}) (sql string, err error) {
	template, ok := conn.template.Analysis[analysisName]
	if !ok {
		err = g.Error("did not find Analysis: " + analysisName)
		return
	}

	switch analysisName {
	case "table_count":
		sql, err = conn.ProcessTemplate("table-union", template, values)
	case "field_chars", "field_stat", "field_stat_group", "field_stat_deep":
		if _, ok := values["columns"]; !ok {
			values["columns"] = values["fields"]
		}
		sql, err = conn.ProcessTemplate("column-union", template, values)
	case "distro_field_date_wide", "fill_cnt_group_field":
		sql, err = conn.ProcessTemplate("column-expres", template, values)
	default:
		sql, err = conn.ProcessTemplate("single", template, values)
	}

	if err != nil {
		err = g.Error(err, "could not get analysis %s", analysisName)
		return
	}

	return
}

// CastColumnForSelect casts to the correct target column type
func (conn *BaseConn) CastColumnForSelect(srcCol iop.Column, tgtCol iop.Column) string {
	return conn.Self().Quote(srcCol.Name)
}

// CastColumnsForSelect cast the source columns into the target Column types
func (conn *BaseConn) CastColumnsForSelect(srcColumns iop.Columns, tgtColumns iop.Columns) []string {
	selectExprs := []string{}

	tgtFields := map[string]iop.Column{}
	// compare type, length/precision, scale
	for _, tgtCol := range tgtColumns {
		tgtFields[strings.ToLower(tgtCol.Name)] = tgtCol
	}

	for _, srcCol := range srcColumns {
		tgtCol, ok := tgtFields[strings.ToLower(srcCol.Name)]
		if !ok {
			continue
		}

		selectExpr := conn.Self().Quote(srcCol.Name)

		if srcCol.DbType != tgtCol.DbType {
			g.Debug(
				"inserting %s [%s] into %s [%s]",
				srcCol.Name, srcCol.DbType, tgtCol.Name, tgtCol.DbType,
			)
			selectExpr = conn.Self().CastColumnForSelect(srcCol, tgtCol)
		} else if srcCol.DbPrecision > tgtCol.DbPrecision {
			g.Debug(
				"target precision / length is smaller when inserting %s [%s(%d)] into %s [%s(%d)]",
				srcCol.Name, srcCol.DbType, srcCol.DbPrecision,
				tgtCol.Name, tgtCol.DbType, tgtCol.DbPrecision,
			)
			selectExpr = conn.Self().CastColumnForSelect(srcCol, tgtCol)
		}

		selectExprs = append(selectExprs, selectExpr)
	}

	return selectExprs
}

// ValidateColumnNames verifies that source fields are present in the target table
// It will return quoted field names as `newColNames`, the same length as `colNames`
func (conn *BaseConn) ValidateColumnNames(tgtColNames []string, colNames []string, quote bool) (newColNames []string, err error) {

	tgtFields := map[string]string{}
	for _, colName := range tgtColNames {
		colName = conn.Self().Unquote(colName)
		if quote {
			tgtFields[strings.ToLower(colName)] = conn.Self().Quote(colName)
		} else {
			tgtFields[strings.ToLower(colName)] = colName
		}
	}

	mismatches := []string{}
	for _, colName := range colNames {
		newColName, ok := tgtFields[strings.ToLower(colName)]
		if !ok {
			// src field is missing in tgt field
			mismatches = append(mismatches, g.F("source field '%s' is missing in target table", colName))
			continue
		}
		if quote {
			newColName = conn.Self().Quote(newColName)
		} else {
			newColName = conn.Self().Unquote(newColName)
		}
		newColNames = append(newColNames, newColName)
	}

	if len(mismatches) > 0 {
		err = g.Error("column names mismatch: %s", strings.Join(mismatches, "\n"))
	}

	g.Trace("insert target fields: " + strings.Join(newColNames, ", "))

	return
}

// InsertStream inserts a stream into a table
func (conn *BaseConn) InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	count, err = InsertStream(conn.Self(), nil, tableFName, ds)
	if err != nil {
		err = g.Error(err, "Could not insert into %s", tableFName)
	}
	return
}

// InsertBatchStream inserts a stream into a table in batch
func (conn *BaseConn) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	count, err = InsertBatchStream(conn.Self(), nil, tableFName, ds)
	if err != nil {
		err = g.Error(err, "Could not batch insert into %s", tableFName)
	}
	return
}

// bindVar return proper bind var according to https://jmoiron.github.io/sqlx/#bindvars
func (conn *BaseConn) bindVar(i int, field string, n int, c int) string {
	return g.R(
		conn.template.Variable["bind_string"],
		"i", cast.ToString(i),
		"field", field,
		"n", cast.ToString(n),
		"c", cast.ToString(c),
	)
}

// Unquote removes quotes to the field name
func (conn *BaseConn) Unquote(field string) string {
	q := conn.template.Variable["quote_char"]
	return strings.ReplaceAll(field, q, "")
}

// Quote adds quotes to the field name
func (conn *BaseConn) Quote(field string, normalize ...bool) string {
	if len(normalize) > 0 && normalize[0] {
		if g.In(conn.Type, dbio.TypeDbOracle, dbio.TypeDbSnowflake) {
			field = strings.ToUpper(field)
		} else {
			field = strings.ToLower(field)
		}
	}
	q := conn.template.Variable["quote_char"]
	field = conn.Self().Unquote(field)
	return q + field + q
}

// GenerateInsertStatement returns the proper INSERT statement
func (conn *BaseConn) GenerateInsertStatement(tableName string, fields []string, numRows int) string {

	values := make([]string, len(fields))
	qFields := make([]string, len(fields)) // quoted fields

	valuesStr := ""
	c := 0
	for n := 0; n < numRows; n++ {
		for i, field := range fields {
			c++
			values[i] = conn.bindVar(i+1, field, n, c)
			qFields[i] = conn.Self().Quote(field)
		}
		valuesStr += fmt.Sprintf("(%s),", strings.Join(values, ", "))
	}

	statement := g.R(
		"INSERT INTO {table} ({fields}) VALUES {values}",
		"table", tableName,
		"fields", strings.Join(qFields, ", "),
		"values", strings.TrimSuffix(valuesStr, ","),
	)
	g.Trace("insert statement: " + strings.Split(statement, ") VALUES ")[0] + ")")
	return statement
}

// Upsert inserts / updates from a srcTable into a target table.
// Assuming the srcTable has some or all of the tgtTable fields with matching types
func (conn *BaseConn) Upsert(srcTable string, tgtTable string, primKeys []string) (rowAffCnt int64, err error) {
	var cnt int64
	if conn.tx != nil {
		cnt, err = Upsert(conn.Self(), conn.tx, srcTable, tgtTable, primKeys)
	} else {
		cnt, err = Upsert(conn.Self(), nil, srcTable, tgtTable, primKeys)
	}
	if err != nil {
		err = g.Error(err, "could not upsert")
	}
	return cast.ToInt64(cnt), err
}

// SwapTable swaps two table
func (conn *BaseConn) SwapTable(srcTable string, tgtTable string) (err error) {

	tgtTableTemp := tgtTable + "_tmp" + g.RandString(g.AlphaRunesLower, 2)
	conn.DropTable(tgtTableTemp)

	sql := g.R(
		conn.GetTemplateValue("core.rename_table"),
		"table", tgtTable,
		"new_table", tgtTableTemp,
	)
	_, err = conn.Exec(sql)
	if err != nil {
		return g.Error(err, "could not rename table "+tgtTable)
	}

	sql = g.R(
		conn.GetTemplateValue("core.rename_table"),
		"table", srcTable,
		"new_table", tgtTable,
	)
	_, err = conn.Exec(sql)
	if err != nil {
		return g.Error(err, "could not rename table "+srcTable)
	}

	sql = g.R(
		conn.GetTemplateValue("core.rename_table"),
		"table", tgtTableTemp,
		"new_table", srcTable,
	)
	_, err = conn.Exec(sql)
	if err != nil {
		return g.Error(err, "could not rename table "+tgtTableTemp)
	}

	return
}

// GetNativeType returns the native column type from generic
func (conn *BaseConn) GetNativeType(col iop.Column) (nativeType string, err error) {

	nativeType, ok := conn.template.GeneralTypeMap[col.Type]
	if !ok {
		err = g.Error(
			"No native type mapping defined for col '%s', with type '%s' ('%s') for '%s'",
			col.Name,
			col.Type,
			col.DbType,
			conn.Type,
		)
		// return "", g.Error(err)
		g.Warn(err.Error() + ". Using 'string'")
		err = nil
		nativeType = conn.template.GeneralTypeMap["string"]
	}

	// Add precision as needed
	if strings.HasSuffix(nativeType, "()") {
		length := col.Stats.MaxLen
		if col.IsString() {
			if !col.Sourced {
				length = col.Stats.MaxLen * 2
				if length < 255 {
					length = 255
				}
			}
			nativeType = strings.ReplaceAll(
				nativeType,
				"()",
				fmt.Sprintf("(%d)", length),
			)
		} else if col.IsInteger() {
			if !col.Sourced && length < ddlDefDecLength {
				length = ddlDefDecLength
			}
			nativeType = strings.ReplaceAll(
				nativeType,
				"()",
				fmt.Sprintf("(%d)", length),
			)
		}
	} else if strings.HasSuffix(nativeType, "(,)") {
		length := col.Stats.MaxLen
		scale := col.Stats.MaxDecLen

		if !col.Sourced {
			length = ddlMaxDecLength // max out
			scale = ddlMaxDecScale   // max out

			if col.IsNumber() {
				if length < ddlMaxDecScale {
					length = ddlMaxDecScale
				} else if length > ddlMaxDecLength {
					length = ddlMaxDecLength
				}

				if scale < ddlMinDecScale {
					scale = ddlMinDecScale
				} else if scale > ddlMaxDecScale {
					scale = ddlMaxDecScale
				}
			}
		}

		nativeType = strings.ReplaceAll(
			nativeType,
			"(,)",
			fmt.Sprintf("(%d,%d)", length, scale),
		)
	}

	return
}

func (conn *BaseConn) generateColumnDDL(col iop.Column, nativeType string) (columnDDL string) {

	columnDDL = g.R(
		conn.GetTemplateValue("core.modify_column"),
		"column", conn.Self().Quote(col.Name),
		"type", nativeType,
	)

	return
}

// GenerateDDL genrate a DDL based on a dataset
func (conn *BaseConn) GenerateDDL(tableFName string, data iop.Dataset, temporary bool) (string, error) {

	if !data.Inferred || data.SafeInference {
		if len(data.Columns) > 0 && data.Columns[0].Stats.TotalCnt == 0 && data.Columns[0].Type == "" {
			g.Warn("Generating DDL from 0 rows. Will use string for unknown types.")
		}
		data.InferColumnTypes()
	}
	columnsDDL := []string{}

	for _, col := range data.Columns {
		// convert from general type to native type
		nativeType, err := conn.GetNativeType(col)
		if err != nil {
			return "", g.Error(err, "no native mapping")
		}

		if !data.NoTrace {
			g.Trace(
				"%s - %s (maxLen: %d, nullCnt: %d, totCnt: %d, strCnt: %d, dtCnt: %d, intCnt: %d, decCnt: %d)",
				col.Name, col.Type,
				col.Stats.MaxLen, col.Stats.NullCnt,
				col.Stats.TotalCnt, col.Stats.StringCnt,
				col.Stats.DateCnt, col.Stats.IntCnt,
				col.Stats.DecCnt,
			)
		}

		// normalize column name uppercase/lowercase
		columnDDL := conn.Self().Quote(col.Name, true) + " " + nativeType
		columnsDDL = append(columnsDDL, columnDDL)
	}

	createTemplate := conn.template.Core["create_table"]
	if temporary {
		createTemplate = conn.template.Core["create_temporary_table"]
	}

	ddl := g.R(
		createTemplate,
		"table", tableFName,
		"col_types", strings.Join(columnsDDL, ",\n"),
	)

	return ddl, nil
}

// BulkImportFlow imports the streams rows in bulk concurrently using channels
func (conn *BaseConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {

	// g.Trace("BulkImportFlow not implemented for %s", conn.GetType())
	df.Context.SetConcurencyLimit(conn.Context().Wg.Limit)
	// df.Context.SetConcurencyLimit(1) // safer for now, fails with too many files

	doImport := func(tableFName string, ds *iop.Datastream) {
		defer df.Context.Wg.Write.Done()

		var cnt uint64
		if conn.GetProp("use_bulk") == "false" {
			cnt, err = conn.Self().InsertBatchStream(tableFName, ds)
		} else {
			cnt, err = conn.Self().BulkImportStream(tableFName, ds)
		}
		count += cnt
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "could not bulk import"))
		}
	}

	// concurrent imports does not work very well
	// for ds := range df.StreamCh {
	// 	df.Context.Wg.Write.Add()
	// 	go doImport(tableFName, ds)
	// }

	// safer for now, fails with too many files
	df.Context.Wg.Write.Add()
	doImport(tableFName, iop.MergeDataflow(df))

	df.Context.Wg.Write.Wait()

	return count, df.Context.Err()
}

// BulkExportFlow creates a dataflow from a sql query
func (conn *BaseConn) BulkExportFlow(sqls ...string) (df *iop.Dataflow, err error) {

	g.Trace("BulkExportFlow not implemented for %s", conn.GetType())
	if UseBulkExportFlowCSV {
		return conn.BulkExportFlowCSV(sqls...)
	}
	columns, err := conn.Self().GetSQLColumns(sqls...)
	if err != nil {
		err = g.Error(err, "Could not get columns.")
		return
	}

	df = iop.NewDataflow()
	df.Context = g.NewContext(conn.Context().Ctx)

	go func() {
		defer df.Close()
		dss := []*iop.Datastream{}

		for _, sql := range sqls {
			ds, err := conn.Self().BulkExportStream(sql)
			if err != nil {
				df.Context.CaptureErr(g.Error(err, "Error running query"))
				return
			}
			dss = append(dss, ds)
		}

		df.PushStreams(dss...)

	}()

	// wait for first ds to start streaming.
	// columns need to be populated
	err = df.WaitReady()
	if err != nil {
		return df, err
	}

	df.SetColumns(columns)
	df.Inferred = true

	return df, nil
}

// BulkExportFlowCSV creates a dataflow from a sql query, using CSVs
func (conn *BaseConn) BulkExportFlowCSV(sqls ...string) (df *iop.Dataflow, err error) {

	columns, err := conn.Self().GetSQLColumns(sqls...)
	if err != nil {
		err = g.Error(err, "Could not get columns.")
		return
	}

	df = iop.NewDataflow()

	unload := func(sql string, pathPart string) {
		defer df.Context.Wg.Read.Done()
		fileReadyChn := make(chan string, 10000)
		ds, err := conn.Self().BulkExportStream(sql)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Error running query"))
			conn.Context().Cancel()
			return
		}

		fs, err := filesys.NewFileSysClient(dbio.TypeFileLocal, conn.PropArr()...)
		if err != nil {
			conn.Context().CaptureErr(g.Error(err, "Unable to create Local file sys Client"))
			ds.Context.Cancel()
			return
		}

		sqlDf, err := iop.MakeDataFlow(ds)
		if err != nil {
			conn.Context().CaptureErr(g.Error(err, "Unable to create data flow"))
			ds.Context.Cancel()
			return
		}

		go func() {
			bw, err := fs.Self().WriteDataflowReady(sqlDf, pathPart, fileReadyChn)
			if err != nil {
				conn.Context().CaptureErr(g.Error(err, "Unable to write to file: "+pathPart))
				ds.Context.Cancel()
				return
			}
			sqlDf.AddBytes(bw)
		}()

		for filePath := range fileReadyChn {
			// when the file is ready, push to dataflow
			nDs, err := iop.ReadCsvStream(filePath)
			if err != nil {
				conn.Context().CaptureErr(g.Error(err, "Unable to read stream: "+filePath))
				ds.Context.Cancel()
				df.Context.Cancel()
				return
			}
			nDs.Defer(func() { os.RemoveAll(filePath) })
			df.PushStreams(nDs)
		}
	}

	path := fmt.Sprintf("/tmp/%s/stream/%s/%s.csv", filePathStorageSlug, conn.GetType(), cast.ToString(g.Now()))

	go func() {
		defer df.Close()
		for i, sql := range sqls {
			pathPart := fmt.Sprintf("%s/sql%02d", path, i+1)
			df.Context.Wg.Read.Add()
			go unload(sql, pathPart)
		}

		// wait until all nDs are pushed to close
		df.Context.Wg.Read.Wait()
	}()

	// wait for first ds to start streaming.
	err = df.WaitReady()
	if err != nil {
		return df, g.Error(err)
	}

	g.Debug("Unloading to %s", path)
	df.SetColumns(columns)
	df.Inferred = true

	return
}

// GenerateUpsertSQL returns a sql for upsert
func (conn *BaseConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {
	return
}

// GenerateUpsertExpressions returns a map with needed expressions
func (conn *BaseConn) GenerateUpsertExpressions(srcTable string, tgtTable string, pkFields []string) (exprs map[string]string, err error) {

	srcColumns, err := conn.GetSQLColumns(srcTable)
	if err != nil {
		err = g.Error(err, "could not get columns for "+srcTable)
		return
	}
	tgtColumns, err := conn.GetSQLColumns(tgtTable)
	if err != nil {
		err = g.Error(err, "could not get column list")
		return
	}

	pkFields, err = conn.ValidateColumnNames(tgtColumns.Names(), pkFields, true)
	if err != nil {
		err = g.Error(err, "PK columns mismatch")
		return
	}
	pkFieldMap := map[string]string{}
	pkEqualFields := []string{}
	for _, pkField := range pkFields {
		pkEqualField := g.F("src.%s = tgt.%s", pkField, pkField)
		pkEqualFields = append(pkEqualFields, pkEqualField)
		pkFieldMap[pkField] = ""
	}

	srcFields, err := conn.ValidateColumnNames(tgtColumns.Names(), srcColumns.Names(), true)
	if err != nil {
		err = g.Error(err, "columns mismatch")
		return
	}

	tgtFields := srcFields
	setFields := []string{}
	insertFields := []string{}
	placeholdFields := []string{}
	for _, colName := range srcFields {
		insertFields = append(insertFields, colName)
		placeholdFields = append(placeholdFields, g.F("ph.%s", colName))
		if _, ok := pkFieldMap[colName]; !ok {
			// is not a pk field
			setField := g.F("%s = src.%s", colName, colName)
			setFields = append(setFields, setField)
		}
	}

	// cast into the correct type
	srcFields = conn.Self().CastColumnsForSelect(srcColumns, tgtColumns)

	exprs = map[string]string{
		"src_tgt_pk_equal": strings.Join(pkEqualFields, " and "),
		"src_upd_pk_equal": strings.ReplaceAll(strings.Join(pkEqualFields, ", "), "tgt.", "upd."),
		"src_fields":       strings.Join(srcFields, ", "),
		"tgt_fields":       strings.Join(tgtFields, ", "),
		"insert_fields":    strings.Join(insertFields, ", "),
		"pk_fields":        strings.Join(pkFields, ", "),
		"set_fields":       strings.Join(setFields, ", "),
		"placehold_fields": strings.Join(placeholdFields, ", "),
	}

	return
}

// GetColumnStats analyzes the table and returns the column statistics
func (conn *BaseConn) GetColumnStats(tableName string, fields ...string) (columns iop.Columns, err error) {

	tableColumns, err := conn.Self().GetSQLColumns(tableName)
	if err != nil {
		err = g.Error(err, "could not obtain columns data")
		return
	}

	generalTypes := map[string]string{}
	statFields := []string{}
	for _, col := range tableColumns {
		generalType := col.Type
		colName := col.Name
		generalTypes[strings.ToLower(colName)] = generalType
		statFields = append(statFields, colName)
	}

	if len(fields) == 0 {
		fields = statFields
	}

	// run analysis field_stat_len
	m := g.M("table", tableName, "fields", fields)
	data, err := conn.Self().RunAnalysis("field_stat_len", m)
	if err != nil {
		err = g.Error(err, "could not analyze table")
		return
	}

	columns = iop.Columns{}
	for _, rec := range data.Records() {
		colName := cast.ToString(rec["field"])
		column := iop.Column{
			Name: colName,
			Type: generalTypes[strings.ToLower(colName)],
			Stats: iop.ColumnStats{
				Min:       cast.ToInt64(rec["f_min"]),
				Max:       cast.ToInt64(rec["f_max"]),
				MaxDecLen: cast.ToInt(rec["f_max_dec_len"]),
				MinLen:    cast.ToInt(rec["f_min_len"]),
				MaxLen:    cast.ToInt(rec["f_max_len"]),
				NullCnt:   cast.ToInt64(rec["f_null_cnt"]),
				TotalCnt:  cast.ToInt64(rec["tot_cnt"]),
			},
		}

		if column.Stats.MaxDecLen == 0 {
			column.Stats.MaxDecLen = ddlDefDecScale
		}
		columns = append(columns, column)
	}

	return

}

// OptimizeTable analyzes the table and alters the table with
// the columns data type based on its analysis result
// if table is missing, it is created with a new DDl
// Hole in this: will truncate data points, since it is based
// only on new data being inserted... would need a complete
// stats of the target table to properly optimize.
func (conn *BaseConn) OptimizeTable(tableName string, newColStats iop.Columns) (err error) {

	// stats are already provided with columns
	tgtColStats, err := conn.Self().GetColumnStats(tableName)
	if err != nil {
		return g.Error(err, "unable to run column stats")
	}

	colStats, err := iop.SyncColumns(newColStats, tgtColStats)
	if err != nil {
		return g.Error(err, "unable to sync columns data")
	}

	columns := iop.InferFromStats(colStats, false, false)

	// compare, and alter or recreate as needed
	colDDLs := []string{}
	for _, col := range columns {
		nativeType, err := conn.GetNativeType(col)
		if err != nil {
			return g.Error(err, "no native mapping")
		}

		switch col.Type {
		case "string", "text", "json", "bytes":
			// alter field to resize column
			colDDL := g.R(
				conn.GetTemplateValue("core.modify_column"),
				"column", conn.Self().Quote(col.Name),
				"type", nativeType,
			)
			colDDLs = append(colDDLs, colDDL)
		default:
			// alter field to resize column
			colDDL := g.R(
				conn.GetTemplateValue("core.modify_column"),
				"column", conn.Self().Quote(col.Name),
				"type", nativeType,
			)
			colDDLs = append(colDDLs, colDDL)
		}

	}

	ddl := g.R(
		conn.GetTemplateValue("core.alter_columns"),
		"table", tableName,
		"col_ddls", strings.Join(colDDLs, ", "),
	)
	_, err = conn.Exec(ddl)
	if err != nil {
		err = g.Error(err, "could not alter columns on table "+tableName)
	}

	return
}

// CompareChecksums compares the checksum values from the database side
// to the checkum values from the StreamProcessor
func (conn *BaseConn) CompareChecksums(tableName string, columns iop.Columns) (err error) {
	tColumns, err := conn.GetSQLColumns(tableName)
	if err != nil {
		err = g.Error(err, "could not get column list")
		return
	}

	// make sure columns exist in table, get common columns into fields
	fields, err := conn.ValidateColumnNames(tColumns.Names(), columns.Names(), false)
	if err != nil {
		err = g.Error(err, "columns mismatch")
		return
	}
	fieldsMap := g.ArrMapString(fields, true)
	g.Debug("comparing checksums %#v vs %#v: %#v", tColumns.Names(), columns.Names(), fields)

	exprs := []string{}
	colChecksum := map[string]uint64{}
	for _, col := range columns {
		colChecksum[strings.ToLower(col.Name)] = col.Stats.Checksum
		if _, ok := fieldsMap[strings.ToLower(col.Name)]; !ok {
			continue // making sure it is a common column
		}

		expr := ""
		switch {
		case col.IsString():
			expr = conn.GetTemplateValue("function.checksum_string")
		case col.IsInteger():
			expr = conn.GetTemplateValue("function.checksum_integer")
		case col.IsDecimal():
			expr = conn.GetTemplateValue("function.checksum_decimal")
		case col.IsDatetime():
			expr = conn.GetTemplateValue("function.checksum_datetime")
		case col.IsBool():
			expr = conn.GetTemplateValue("function.checksum_boolean")
		default:
			expr = "0"
		}
		colName := fieldsMap[strings.ToLower(col.Name)]
		expr = g.R(expr, "field", conn.Self().Quote(cast.ToString(colName)))
		exprs = append(exprs, g.F("sum(%s) as %s", expr, col.Name))
	}

	sql := g.F(
		"select %s from %s "+noTraceKey,
		strings.Join(exprs, ", "),
		tableName,
	)

	data, err := conn.Self().Query(sql)
	if err != nil {
		return g.Error(err, "error running CompareChecksums query")
	}
	// g.P(data.Rows[0])

	eg := g.ErrorGroup{}
	for i, col := range data.Columns {
		checksum1 := colChecksum[strings.ToLower(col.Name)]
		checksum2 := cast.ToUint64(data.Rows[0][i])
		if checksum1 != checksum2 {
			eg.Add(g.Error("checksum failure for %s: %d != %d", col.Name, checksum1, checksum2))
		}
	}

	return eg.Err()
}

func (conn *BaseConn) credsProvided(provider string) bool {
	if provider == "AWS" {
		if conn.GetProp("AWS_SECRET_ACCESS_KEY") != "" || conn.GetProp("AWS_ACCESS_KEY_ID") != "" {
			return true
		}
	}
	if provider == "AZURE" {
		if conn.GetProp("AZURE_ACCOUNT") != "" || conn.GetProp("AZURE_CONTAINER") != "" || (conn.GetProp("AZURE_SAS_SVC_URL") == "" && conn.GetProp("AZURE_CONN_STR") == "") {
			return true
		}
	}
	return false
}

// settingMppBulkImportFlow sets settings for MPP databases type
// for BulkImportFlow
func settingMppBulkImportFlow(conn Connection) {
	if cast.ToInt(conn.GetProp("FILE_MAX_ROWS")) == 0 {
		conn.SetProp("FILE_MAX_ROWS", "500000")
	}
	if cast.ToInt(conn.GetProp("FILE_BYTES_LIMIT")) == 0 {
		conn.SetProp("FILE_BYTES_LIMIT", "16000000")
	}

	conn.SetProp("COMPRESSION", "GZIP")

	conn.SetProp("DBIO_PARALLEL", "true")
}

// ToData converts schema objects to tabular format
func (schema *Schema) ToData() (data iop.Dataset) {
	columns := []string{"schema_name", "table_name", "is_view", "column_id", "column_name", "column_type"}
	data = iop.NewDataset(iop.NewColumnsFromFields(columns...))

	for _, table := range schema.Tables {
		for _, col := range table.Columns {
			row := []interface{}{schema.Name, table.Name, table.IsView, col.Position, col.Name, col.DbType}
			data.Rows = append(data.Rows, row)
		}
	}
	return
}

// TestPermissions tests the needed permissions in a given connection
func TestPermissions(conn Connection, tableName string) (err error) {

	type testObj struct {
		Title string
		SQL   string
		Skip  []dbio.Type
	}

	col := iop.Column{Name: "col1", Type: "integer"}
	nativeType := conn.Template().GeneralTypeMap[col.Type]

	// drop table if exists
	err = conn.DropTable(tableName)
	if err != nil {
		return g.Error(err, "failed testing permissions: Drop table")
	}

	tests := []testObj{

		// Create table
		testObj{
			Title: "Create a test table",
			SQL: g.R(
				conn.GetTemplateValue("core.create_table"),
				"table", tableName,
				"col_types", col.Name+" "+nativeType,
			),
		},

		// Insert
		testObj{
			Title: "Insert into a test table",
			SQL: g.R(
				conn.GetTemplateValue("core.insert"),
				"table", tableName,
				"fields", col.Name,
				"values", "1",
			),
		},

		// Update
		testObj{
			Title: "Update the test table",
			SQL: g.R(
				conn.GetTemplateValue("core.update"),
				"table", tableName,
				"set_fields", g.F("%s = 2", col.Name),
				"pk_fields_equal", "1=1",
			),
		},

		// Delete
		testObj{
			Title: "Delete from the test table",
			SQL: g.R(
				"delete from {table} where 1=0",
				"table", tableName,
			),
		},

		// Truncate
		testObj{
			Title: "Truncate the test table",
			SQL: g.R(
				conn.GetTemplateValue("core.truncate_table"),
				"table", tableName,
			),
		},

		// Drop table
		testObj{
			Title: "Drop the test table",
			SQL: g.R(
				conn.GetTemplateValue("core.drop_table"),
				"table", tableName,
			),
		},
	}

	for _, test := range tests {
		for _, skipType := range test.Skip {
			if conn.GetType() == skipType {
				continue
			}
		}

		_, err = conn.Exec(test.SQL)
		if err != nil {
			return g.Error(err, "failed testing permissions: %s", test.Title)
		}
	}

	return
}

// CleanSQL removes creds from the query
func CleanSQL(conn Connection, sql string) string {
	for _, v := range conn.Props() {
		if strings.TrimSpace(v) == "" {
			continue
		}
		sql = strings.ReplaceAll(sql, v, "***")
	}
	return sql
}

func CopyFromS3(conn Connection, tableFName, s3Path string) (err error) {
	AwsID := conn.GetProp("AWS_ACCESS_KEY_ID")
	AwsAccessKey := conn.GetProp("AWS_SECRET_ACCESS_KEY")
	if AwsID == "" || AwsAccessKey == "" {
		err = g.Error("Need to set 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' to copy to snowflake from S3")
		return
	}

	sql := g.R(
		conn.Template().Core["copy_from_s3"],
		"table", tableFName,
		"s3_path", s3Path,
		"aws_access_key_id", AwsID,
		"aws_secret_access_key", AwsAccessKey,
	)

	g.Info("copying into %s from s3", conn.GetType())
	g.Debug("url: " + s3Path)
	_, err = conn.Exec(sql)
	if err != nil {
		return g.Error(err, "SQL Error:\n"+CleanSQL(conn, sql))
	}

	return nil
}

func getAzureToken(conn Connection) (azToken string, err error) {
	azSasURL := conn.GetProp("AZURE_SAS_SVC_URL")
	if azSasURL == "" {
		err = g.Error("Need to set 'AZURE_SAS_SVC_URL' to copy to snowflake from azure")
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
	azToken = azSasURLArr[1]
	return
}

// CopyFromAzure uses the Snowflake COPY INTO Table command from Azure
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
func CopyFromAzure(conn Connection, tableFName, azPath string) (err error) {
	azToken, err := getAzureToken(conn)
	if err != nil {
		return g.Error(err)
	}

	sql := g.R(
		conn.Template().Core["copy_from_azure"],
		"table", tableFName,
		"azure_path", azPath,
		"azure_sas_token", azToken,
	)

	g.Info("copying into %s from azure", conn.GetType())
	g.Debug("url: " + azPath)
	conn.SetProp("azure_sas_token", azToken)
	_, err = conn.Exec(sql)
	if err != nil {
		return g.Error(err, "SQL Error:\n"+CleanSQL(conn, sql))
	}

	return nil
}

// ParseSQLMultiStatements splits a sql text into statements
// typically by a ';'
func ParseSQLMultiStatements(sql string) (sqls g.Strings) {
	inQuote := false
	inCommentLine := false
	inCommentMulti := false
	char := ""
	pChar := ""
	nChar := ""
	currState := ""

	inComment := func() bool {
		return inCommentLine || inCommentMulti
	}

	for i := range sql {
		char = string(sql[i])

		// previous
		if i > 0 {
			pChar = string(sql[i-1])
		}

		// next
		nChar = ""
		if i+1 < len(sql) {
			nChar = string(sql[i+1])
		}

		switch {
		case !inQuote && !inComment() && char == "'":
			inQuote = true
		case inQuote && char == "'" && nChar != "'":
			inQuote = false
		case !inQuote && !inComment() && pChar == "-" && char == "-":
			inCommentLine = true
		case inCommentLine && char == "\n":
			inCommentLine = false
		case !inQuote && !inComment() && pChar == "/" && char == "*":
			inCommentMulti = true
		case inCommentMulti && pChar == "*" && char == "/":
			inCommentMulti = false
		}

		currState = currState + char

		// detect end
		if char == ";" && !inQuote && !inComment() {
			if strings.TrimSpace(currState) != "" {
				sqls = append(sqls, currState)
			}
			currState = ""
		}
	}

	if len(currState) > 0 {
		if strings.TrimSpace(currState) != "" {
			sqls = append(sqls, currState)
		}
	}

	return
}
