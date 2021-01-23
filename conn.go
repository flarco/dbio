package dbio

import (
	"os"
	"strings"

	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/spf13/cast"
)

type ConnType int

const (
	ConnTypeNone ConnType = iota
	connTypeFileStart
	ConnTypeFileLocal
	ConnTypeFileHDFS
	ConnTypeFileS3
	ConnTypeFileAzure
	ConnTypeFileGoogle
	ConnTypeFileSftp
	ConnTypeFileHTTP
	connTypeFileEnd

	connTypeDbStart
	ConnTypeDbPostgres
	ConnTypeDbRedshift
	ConnTypeDbMySQL
	ConnTypeDbOracle
	ConnTypeDbBigQuery
	ConnTypeDbSnowflake
	ConnTypeDbSQLite
	ConnTypeDbSQLServer
	ConnTypeDbAzure
	connTypeDbEnd

	connTypeAPIStart
	ConnTypeAPIGit
	ConnTypeAPIGithub
	connTypeAPIEnd
)

// DataConn represents a data connection with properties
type DataConn struct {
	ID   string
	Type string
	URL  string
	Data map[string]interface{}
}

// NewDataConnFromMap loads a DataConn from a Map
func NewDataConnFromMap(m map[string]interface{}) (dc *DataConn) {
	dc = &DataConn{
		ID:   cast.ToString(m["id"]),
		Type: cast.ToString(m["type"]),
		Data: map[string]interface{}{},
	}
	data, ok := m["data"].(map[string]interface{})
	if ok {
		dc.Data = data
	}

	dc.SetFromEnv()
	dc.SetURL()
	return dc
}

// SetFromEnv set values from environment
func (dc *DataConn) SetFromEnv() {
	if newURL := os.Getenv(strings.TrimLeft(dc.URL, "$")); newURL != "" {
		dc.URL = newURL
	}

	for k, v := range dc.Data {
		val := cast.ToString(v)
		if strings.HasPrefix(val, "$") {
			varKey := strings.TrimLeft(val, "$")
			if newVal := os.Getenv(varKey); newVal != "" {
				dc.Data[k] = newVal
			} else {
				g.Warn("No env var value found for %s", val)
			}
		}
	}
}

// DataS returns data as map[string]string
func (dc *DataConn) DataS(lowerCase ...bool) map[string]string {
	lc := false
	if len(lowerCase) > 0 {
		lc = lowerCase[0]
	}
	data := map[string]string{}
	for k, v := range dc.Data {
		if lc {
			data[strings.ToLower(k)] = cast.ToString(v)
		} else {
			data[k] = cast.ToString(v)
		}
	}
	return data
}

// ToMap transforms DataConn to a Map
func (dc *DataConn) ToMap() map[string]interface{} {
	return g.M("id", dc.ID, "type", dc.Type, "data", dc.Data)
}

// GetType returns the connection type
func (dc *DataConn) GetType() ConnType {

	switch {
	case strings.HasPrefix(dc.URL, "postgres"):
		if strings.Contains(dc.URL, "redshift.amazonaws.com") {
			return ConnTypeDbRedshift
		}
		return ConnTypeDbPostgres
	case strings.HasPrefix(dc.URL, "redshift"):
		return ConnTypeDbRedshift
	case strings.HasPrefix(dc.URL, "sqlserver:"):
		if strings.Contains(dc.URL, "database.windows.net") {
			return ConnTypeDbAzure
		}
		return ConnTypeDbSQLServer
	case strings.HasPrefix(dc.URL, "mysql:"):
		return ConnTypeDbMySQL
	case strings.HasPrefix(dc.URL, "oracle:"):
		return ConnTypeDbOracle
	case strings.HasPrefix(dc.URL, "bigquery:"):
		return ConnTypeDbBigQuery
	case strings.HasPrefix(dc.URL, "snowflake"):
		return ConnTypeDbSnowflake
	case strings.HasPrefix(dc.URL, "fileSys :"):
		return ConnTypeDbSQLite
	case strings.HasPrefix(dc.URL, "s3:"):
		return ConnTypeFileS3
	case strings.HasPrefix(dc.URL, "hdfs:"):
		return ConnTypeFileHDFS
	case strings.Contains(dc.URL, ".core.windows.net"), strings.HasPrefix(dc.URL, "azure://"):
		if strings.Contains(dc.URL, "&sig=") && strings.Contains(dc.URL, "&spr=") {
			return ConnTypeNone // is SAS URL
		}
		return ConnTypeFileAzure
	case strings.HasPrefix(dc.URL, "gs://"):
		return ConnTypeFileGoogle
	case strings.HasPrefix(dc.URL, "sftp:/"):
		return ConnTypeFileSftp
	case strings.HasPrefix(dc.URL, "http:/"), strings.HasPrefix(dc.URL, "https:/"):
		if strings.HasSuffix(dc.URL, ".git") {
			return ConnTypeAPIGit
		}
		if strings.Contains(dc.URL, "github.com/") && len(strings.Split(dc.URL, "/")) == 5 {
			return ConnTypeAPIGithub
		}
		return ConnTypeFileHTTP
	case strings.Contains(dc.URL, `:\`):
		return ConnTypeFileLocal // Windows
	case strings.HasPrefix(dc.URL, `/`) && !strings.Contains(dc.URL, `:`):
		return ConnTypeFileLocal // Linux
	case strings.Contains(dc.URL, "://"):
		return ConnTypeNone
	default:
		return ConnTypeFileLocal
	}
}

// SetURL sets the corresponding URL string
// dc.Type should be set prior
func (dc *DataConn) SetURL() error {
	cType, ok := ConnTypeMapping()[dc.Type]
	if !ok {
		return g.Error("could not map type '%s'", dc.Type)
	}

	if dc.URL != "" {
		dc.Type = dc.GetType().GetKey()
		return nil
	}

	if u, ok := dc.Data["url"]; ok {
		dc.URL = cast.ToString(u)
		return nil
	}

	template := ""

	switch cType {
	case ConnTypeDbOracle:
		if tns, ok := dc.Data["tns"]; ok {
			if !strings.HasPrefix(cast.ToString(tns), "(") {
				dc.Data["tns"] = "(" + cast.ToString(tns) + ")"
			}
			template = "oracle://{username}:{password}@{tns}"
		} else {
			template = "oracle://{username}:{password}@{host}:{port}/{sid}"
		}
	case ConnTypeDbPostgres:
		template = "postgresql://{username}:{password}@{host}:{port}/{database}?sslmode={sslmode}"
	case ConnTypeDbRedshift:
		template = "redshift://{username}:{password}@{host}:{port}/{database}?sslmode={sslmode}"
	case ConnTypeDbMySQL:
		template = "mysql://{username}:{password}@{host}:{port}/{database}"
	case ConnTypeDbBigQuery:
		template = "bigquery://{project_id}/{location}/{dataset_id}"
	case ConnTypeDbSnowflake:
		template = "snowflake://{username}:{password}@{host}.snowflakecomputing.com:443/{database}?schema={schema}&warehouse={warehouse}"
	case ConnTypeDbSQLite:
		template = "sqlite:///{database}"
	case ConnTypeDbSQLServer, ConnTypeDbAzure:
		template = "sqlserver://{username}:{password}@{host}:{port}/{database}"
	}

	if template != "" {
		dc.URL = g.Rm(template, dc.Data)
	}

	return nil
}

// ConnTypesDefPort are all the default ports
var ConnTypesDefPort = map[ConnType]int{
	ConnTypeDbPostgres:  5432,
	ConnTypeDbRedshift:  5439,
	ConnTypeDbMySQL:     3306,
	ConnTypeDbOracle:    1521,
	ConnTypeDbSQLServer: 1433,
	ConnTypeDbAzure:     1433,
}

// GetCredProps returns the credential properties
func (dc *DataConn) GetCredProps() (m map[string]interface{}, err error) {
	u, err := net.NewURL(dc.URL)
	if err != nil {
		err = g.Error(err, "could not parse URL for "+dc.GetTypeKey())
		return
	}

	schema := u.PopParam("schema")
	if schema == "" {
		schema = dc.DataS(true)["schema"]
	}

	m = g.M(
		"type", dc.GetTypeKey(),
		"host", u.Hostname(),
		"user", u.Username(),
		"password", u.Password(),
		"port", u.Port(ConnTypesDefPort[dc.GetType()]),
		"database", strings.ReplaceAll(u.Path(), "/", ""),
		"url", u.URL(),
	)
	return
}

// GetTypeNameLong return the type long name
func (dc *DataConn) GetTypeNameLong() string {
	mapping := map[ConnType]string{
		ConnTypeFileLocal:   "FileSys - Local",
		ConnTypeFileHDFS:    "FileSys - HDFS",
		ConnTypeFileS3:      "FileSys - S3",
		ConnTypeFileAzure:   "FileSys - Azure",
		ConnTypeFileGoogle:  "FileSys - Google",
		ConnTypeFileSftp:    "FileSys - Sftp",
		ConnTypeFileHTTP:    "FileSys - HTTP",
		ConnTypeDbPostgres:  "DB - PostgreSQL",
		ConnTypeDbRedshift:  "DB - Redshift",
		ConnTypeDbMySQL:     "DB - MySQL",
		ConnTypeDbOracle:    "DB - Oracle",
		ConnTypeDbBigQuery:  "DB - BigQuery",
		ConnTypeDbSnowflake: "DB - Snowflake",
		ConnTypeDbSQLite:    "DB - SQLite",
		ConnTypeDbSQLServer: "DB - SQLServer",
		ConnTypeDbAzure:     "DB - Azure",
		ConnTypeAPIGit:      "API - Git",
		ConnTypeAPIGithub:   "API - Github",
	}
	return mapping[dc.GetType()]
}

// GetKind return the Kind name
func (dc *DataConn) GetKind() string {
	mapping := map[ConnType]string{
		ConnTypeFileLocal:   "file",
		ConnTypeFileHDFS:    "file",
		ConnTypeFileS3:      "file",
		ConnTypeFileAzure:   "file",
		ConnTypeFileGoogle:  "file",
		ConnTypeFileSftp:    "file",
		ConnTypeFileHTTP:    "file",
		ConnTypeDbPostgres:  "database",
		ConnTypeDbRedshift:  "database",
		ConnTypeDbMySQL:     "database",
		ConnTypeDbOracle:    "database",
		ConnTypeDbBigQuery:  "database",
		ConnTypeDbSnowflake: "database",
		ConnTypeDbSQLite:    "database",
		ConnTypeDbSQLServer: "database",
		ConnTypeDbAzure:     "database",
		ConnTypeAPIGit:      "api",
		ConnTypeAPIGithub:   "api",
	}
	return mapping[dc.GetType()]
}

// ConnTypesKeyMapping are all the connection types with their key
var ConnTypesKeyMapping = map[ConnType]string{
	ConnTypeFileLocal:   "local",
	ConnTypeFileHDFS:    "hdfs",
	ConnTypeFileS3:      "s3",
	ConnTypeFileAzure:   "azure",
	ConnTypeFileGoogle:  "gs",
	ConnTypeFileSftp:    "sftp",
	ConnTypeFileHTTP:    "http",
	ConnTypeDbPostgres:  "postgres",
	ConnTypeDbRedshift:  "redshift",
	ConnTypeDbMySQL:     "mysql",
	ConnTypeDbOracle:    "oracle",
	ConnTypeDbBigQuery:  "bigquery",
	ConnTypeDbSnowflake: "snowflake",
	ConnTypeDbSQLite:    "sqlite",
	ConnTypeDbSQLServer: "sqlserver",
	ConnTypeDbAzure:     "azuresql",
	ConnTypeAPIGit:      "git",
	ConnTypeAPIGithub:   "github",
}

// GetKey return the corresponding key
func (ct ConnType) GetKey() string {
	return ConnTypesKeyMapping[ct]
}

// ConnTypeMapping returns the mapping of key to type
func ConnTypeMapping() map[string]ConnType {
	m := map[string]ConnType{}
	for k, v := range ConnTypesKeyMapping {
		m[v] = k
	}
	return m
}

// GetTypeKey return the type name
func (dc *DataConn) GetTypeKey() string {
	return dc.GetType().GetKey()
}

// ConnTypesNameMapping are all the connection types with their key
var ConnTypesNameMapping = map[ConnType]string{
	ConnTypeFileLocal:   "Local Storage",
	ConnTypeFileHDFS:    "HDFS",
	ConnTypeFileS3:      "AWS S3",
	ConnTypeFileAzure:   "Azure Storage",
	ConnTypeFileGoogle:  "Google Storage",
	ConnTypeFileSftp:    "SFTP",
	ConnTypeFileHTTP:    "HTTP",
	ConnTypeDbPostgres:  "PostgreSQL",
	ConnTypeDbRedshift:  "Redshift",
	ConnTypeDbMySQL:     "MySQL",
	ConnTypeDbOracle:    "Oracle",
	ConnTypeDbBigQuery:  "BigQuery",
	ConnTypeDbSnowflake: "Snowflake",
	ConnTypeDbSQLite:    "SQLite",
	ConnTypeDbSQLServer: "SQL Server",
	ConnTypeDbAzure:     "Azure SQL",
	ConnTypeAPIGit:      "Git",
	ConnTypeAPIGithub:   "Github",
}

// GetName return the corresponding Name
func (ct ConnType) GetName() string {
	return ConnTypesNameMapping[ct]
}

// GetTypeName return the type name
func (dc *DataConn) GetTypeName() string {
	return dc.GetType().GetName()
}

// GetConnKeyNameMapping returns a key to name mapping of connection types
func GetConnKeyNameMapping() map[string]string {
	m := map[string]string{}
	for t, key := range ConnTypesKeyMapping {
		m[key] = ConnTypesNameMapping[t]
	}
	return m
}

// IsDbType returns true for database connections
func (dc *DataConn) IsDbType() bool {
	connType := dc.GetType()
	if connType > connTypeDbStart && connType < connTypeDbEnd {
		return true
	}
	return false
}

// IsFileType returns true for file connections
func (dc *DataConn) IsFileType() bool {
	connType := dc.GetType()
	if connType > connTypeFileStart && connType < connTypeFileEnd {
		return true
	}
	return false
}

// IsAPIType returns true for api connections
func (dc *DataConn) IsAPIType() bool {
	connType := dc.GetType()
	if connType > connTypeAPIStart && connType < connTypeAPIEnd {
		return true
	}
	return false
}
