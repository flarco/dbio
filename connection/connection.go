package connection

import (
	"context"
	"os"
	"strings"

	"github.com/flarco/dbio"

	"github.com/flarco/dbio/database"
	"github.com/flarco/dbio/filesys"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/spf13/cast"
)

// Info is the connection type
type Info struct {
	Name string
	Type dbio.Type
	Data map[string]interface{}
}

// Connection is a connection
type Connection interface {
	// Self() Connection
	Close() error
	Context() g.Context
	Info() Info
	URL() string
	DataS(lowerCase ...bool) map[string]string
	ToMap() map[string]interface{}
	AsDatabase() (database.Connection, error)
	AsFile() (filesys.FileSysClient, error)
	// AsAPI() interface{}
	Set(map[string]interface{})
}

// connBase is the base connection struct
type connBase struct {
	Name    string                 `json:"name"`
	Type    dbio.Type              `json:"type"`
	Data    map[string]interface{} `json:"data"`
	context g.Context              `json:"-"`
}

// NewConnection creates a new connection
func NewConnection(Name string, t dbio.Type, Data map[string]interface{}) (conn Connection, err error) {
	c := g.NewContext(context.Background())

	b := connBase{
		Name: Name, Type: t, Data: g.AsMap(Data, true), context: c,
	}
	conn = &b

	err = b.setURL()
	if err != nil {
		return conn, g.Error(err, "could not set URL for %s: %s", b.Type, Name)
	}

	if b.Type == dbio.TypeUnknown {
		return conn, g.Error("must specify connection type")
	}

	switch b.Type.Kind() {
	case dbio.KindDatabase, dbio.KindFile:
	default:
		err = g.Error("unsupported connection type (%s)", b.Type)
	}
	return conn, err
}

// NewConnectionFromURL creates a new connection from a url
func NewConnectionFromURL(Name, URL string) (conn Connection, err error) {
	return NewConnection(Name, "", g.M("url", URL))
}

// NewConnectionFromMap loads a Connection from a Map
func NewConnectionFromMap(m map[string]interface{}) (c Connection, err error) {
	return NewConnection(
		cast.ToString(m["name"]),
		dbio.Type(cast.ToString(m["type"])),
		g.AsMap(m["data"]),
	)
}

// Info returns connection information
func (c *connBase) Info() Info {
	return Info{
		Name: c.Name,
		Type: c.Type,
		Data: c.Data,
	}
}

// ToMap transforms DataConn to a Map
func (c *connBase) ToMap() map[string]interface{} {
	return g.M("name", c.Name, "type", c.Type, "data", c.Data)
}

// Set sets key/values from a map
func (c *connBase) Set(m map[string]interface{}) {
	for k, v := range m {
		c.Data[k] = v
	}
	c.setURL()
}

// DataS returns data as map[string]string
func (c *connBase) DataS(lowerCase ...bool) map[string]string {
	lc := false
	if len(lowerCase) > 0 {
		lc = lowerCase[0]
	}
	data := map[string]string{}
	for k, v := range c.Data {
		if lc {
			data[strings.ToLower(k)] = cast.ToString(v)
		} else {
			data[k] = cast.ToString(v)
		}
	}
	return data
}

// Context returns the context
func (c *connBase) Context() g.Context {
	return c.context
}

// URL returns the url string
func (c *connBase) URL() string {
	return cast.ToString(c.Data["url"])
}

func (c *connBase) AsDatabase() (database.Connection, error) {
	return database.NewConnContext(
		c.Context().Ctx, c.URL(), g.MapToKVArr(c.DataS())...,
	)
}

func (c *connBase) AsFile() (filesys.FileSysClient, error) {
	return filesys.NewFileSysClientFromURLContext(
		c.Context().Ctx, c.URL(), g.MapToKVArr(c.DataS())...,
	)
}

// SetFromEnv set values from environment
func (c *connBase) setFromEnv() {
	if c.Name == "" && strings.HasPrefix(c.URL(), "$") {
		c.Name = strings.TrimLeft(c.URL(), "$")
	}

	if newURL := os.Getenv(strings.TrimLeft(c.URL(), "$")); newURL != "" {
		c.Data["url"] = newURL
	}

	for k, v := range c.Data {
		val := cast.ToString(v)
		if strings.HasPrefix(val, "$") {
			varKey := strings.TrimLeft(val, "$")
			if newVal := os.Getenv(varKey); newVal != "" {
				c.Data[k] = newVal
			} else {
				g.Warn("No env var value found for %s", val)
			}
		}
	}
}

// Close closes the connection
func (c *connBase) Close() error {
	return nil
}

func (c *connBase) setURL() (err error) {
	c.setFromEnv()

	// setIfMissing sets a default value if key is not present
	setIfMissing := func(key string, val interface{}) {
		if _, ok := c.Data[key]; !ok {
			c.Data[key] = val
		}
	}

	// if URL is provided, extract properties from it
	if c.URL() != "" {
		U, err := net.NewURL(c.URL())
		if err != nil {
			return g.Error("could not parse provided url")
		}

		scheme := U.U.Scheme
		if scheme == "" {
			// if scheme is blank, than is local file
			scheme = string(dbio.TypeFileLocal)
		}

		t, ok := dbio.ValidateType(scheme)
		if !ok {
			return g.Error("unsupported type: %s", U.U.Scheme)
		}
		c.Type = t

		if c.Type.IsDb() {
			// set props from URL
			setIfMissing("schema", U.PopParam("schema"))
			setIfMissing("host", U.Hostname())
			setIfMissing("username", U.Username())
			setIfMissing("password", U.Password())
			setIfMissing("port", U.Port(c.Info().Type.DefPort()))
			setIfMissing("database", strings.ReplaceAll(U.Path(), "/", ""))
		}
	}

	template := ""

	// checkData checks c.Data for any missing keys
	checkData := func(keys ...string) error {
		eG := g.ErrorGroup{}
		for _, k := range keys {
			if _, ok := c.Data[k]; !ok {
				eG.Add(g.Error("Prop value not provided: %s", k))
			}
		}
		return eG.Err()
	}

	switch c.Type {
	case dbio.TypeDbOracle:
		setIfMissing("password", "")
		setIfMissing("sid", c.Data["database"])
		setIfMissing("port", c.Type.DefPort())
		if tns, ok := c.Data["tns"]; ok {
			if !strings.HasPrefix(cast.ToString(tns), "(") {
				c.Data["tns"] = "(" + cast.ToString(tns) + ")"
			}
			template = "oracle://{username}:{password}@{tns}"
		} else {
			template = "oracle://{username}:{password}@{host}:{port}/{sid}"
		}
	case dbio.TypeDbPostgres:
		setIfMissing("password", "")
		setIfMissing("sslmode", "disable")
		setIfMissing("port", c.Type.DefPort())
		template = "postgresql://{username}:{password}@{host}:{port}/{database}?sslmode={sslmode}"
	case dbio.TypeDbRedshift:
		setIfMissing("password", "")
		setIfMissing("sslmode", "disable")
		setIfMissing("port", c.Type.DefPort())
		template = "redshift://{username}:{password}@{host}:{port}/{database}?sslmode={sslmode}"
	case dbio.TypeDbMySQL:
		setIfMissing("password", "")
		setIfMissing("port", c.Type.DefPort())
		template = "mysql://{username}:{password}@{host}:{port}/{database}"
	case dbio.TypeDbBigQuery:
		template = "bigquery://{project_id}/{location}/{dataset_id}"
	case dbio.TypeDbSnowflake:
		setIfMissing("schema", "public")
		template = "snowflake://{username}:{password}@{host}.snowflakecomputing.com:443/{database}?schema={schema}&warehouse={warehouse}"
	case dbio.TypeDbSQLite:
		template = "sqlite:///{database}"
	case dbio.TypeDbSQLServer, dbio.TypeDbAzure, dbio.TypeDbAzureDWH:
		setIfMissing("password", "")
		setIfMissing("port", c.Type.DefPort())
		template = "sqlserver://{username}:{password}@{host}:{port}/{database}"
	case dbio.TypeFileS3:
		template = "s3://{aws_bucket}"
	case dbio.TypeFileGoogle:
		template = "gs://{gc_bucket}"
	case dbio.TypeFileAzure:
		template = "azure://{azure_account}"
	case dbio.TypeFileSftp:
		template = "sftp://{username}:{password}@{host}:{port}"
	default:
		switch c.Type.Kind() {
		case dbio.KindFile, dbio.KindAPI:
			return nil
		default:
			return g.Error("unrecognized type: %s", c.Type)
		}
	}

	keys := g.MatchesGroup(template, "{([a-zA-Z]+)}", 0)
	if err := checkData(keys...); err != nil {
		return g.Error(err, "required keys not provided")
	}

	// set URL is missing
	setIfMissing("url", g.Rm(template, c.Data))

	return nil
}

// CopyDirect copies directly from cloud files
// (without passing through dbio)
func CopyDirect(conn database.Connection, tableFName string, srcFile Connection) (cnt uint64, ok bool, err error) {
	if srcFile == nil || !srcFile.Info().Type.IsFile() {
		return 0, false, nil
	}

	fs, err := srcFile.AsFile()
	if err != nil {
		err = g.Error(err, "Could not obtain client for: "+srcFile.URL())
		return
	}

	switch fs.FsType() {
	case dbio.TypeFileS3:
		ok = true
		err = database.CopyFromS3(conn, tableFName, srcFile.URL())
		if err != nil {
			err = g.Error(err, "could not load into database from S3")
		}
	case dbio.TypeFileAzure:
		ok = true
		err = database.CopyFromAzure(conn, tableFName, srcFile.URL())
		if err != nil {
			err = g.Error(err, "could not load into database from Azure")
		}
	case dbio.TypeFileGoogle:
	}

	if err != nil {
		// ok = false // try through dbio?
	}
	return
}

// GetTypeNameLong return the type long name
func GetTypeNameLong(c Connection) string {
	mapping := map[dbio.Type]string{
		dbio.TypeFileLocal:   "FileSys - Local",
		dbio.TypeFileHDFS:    "FileSys - HDFS",
		dbio.TypeFileS3:      "FileSys - S3",
		dbio.TypeFileAzure:   "FileSys - Azure",
		dbio.TypeFileGoogle:  "FileSys - Google",
		dbio.TypeFileSftp:    "FileSys - Sftp",
		dbio.TypeFileHTTP:    "FileSys - HTTP",
		dbio.TypeDbPostgres:  "DB - PostgreSQL",
		dbio.TypeDbRedshift:  "DB - Redshift",
		dbio.TypeDbMySQL:     "DB - MySQL",
		dbio.TypeDbOracle:    "DB - Oracle",
		dbio.TypeDbBigQuery:  "DB - BigQuery",
		dbio.TypeDbSnowflake: "DB - Snowflake",
		dbio.TypeDbSQLite:    "DB - SQLite",
		dbio.TypeDbSQLServer: "DB - SQLServer",
		dbio.TypeDbAzure:     "DB - Azure",
		dbio.TypeAPIGit:      "API - Git",
		dbio.TypeAPIGithub:   "API - Github",
	}
	return mapping[c.Info().Type]
}

// GetTypeName return the type name
func GetTypeName(c Connection) string {
	mapping := map[dbio.Type]string{
		dbio.TypeFileLocal:   "Local",
		dbio.TypeFileHDFS:    "HDFS",
		dbio.TypeFileS3:      "S3",
		dbio.TypeFileAzure:   "Azure",
		dbio.TypeFileGoogle:  "Google",
		dbio.TypeFileSftp:    "Sftp",
		dbio.TypeFileHTTP:    "HTTP",
		dbio.TypeDbPostgres:  "PostgreSQL",
		dbio.TypeDbRedshift:  "Redshift",
		dbio.TypeDbMySQL:     "MySQL",
		dbio.TypeDbOracle:    "Oracle",
		dbio.TypeDbBigQuery:  "BigQuery",
		dbio.TypeDbSnowflake: "Snowflake",
		dbio.TypeDbSQLite:    "SQLite",
		dbio.TypeDbSQLServer: "SQLServer",
		dbio.TypeDbAzure:     "Azure",
		dbio.TypeAPIGit:      "Git",
		dbio.TypeAPIGithub:   "Github",
	}
	return mapping[c.Info().Type]
}
