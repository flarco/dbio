package connection

import (
	"context"
	"strings"

	"github.com/flarco/dbio/database"
	"github.com/flarco/dbio/filesys"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/spf13/cast"
)

// Kind is the connection kind
type Kind string

const (
	// KindDatabase for databases
	KindDatabase Kind = "database"
	// KindFile for files (cloud, sftp)
	KindFile Kind = "file"
	// KindAPI for APIs
	KindAPI Kind = "api"
	// KindUnknown for unknown
	KindUnknown Kind = ""
)

// Type is the connection type
type Type string

const (
	TypeUnknown = ""

	TypeFileLocal  = "local"
	TypeFileHDFS   = "hdfs"
	TypeFileS3     = "s3"
	TypeFileAzure  = "azure"
	TypeFileGoogle = "gs"
	TypeFileSftp   = "sftp"
	TypeFileHTTP   = "http"

	TypeDbPostgres  = "postgres"
	TypeDbRedshift  = "redshift"
	TypeDbMySQL     = "mysql"
	TypeDbOracle    = "oracle"
	TypeDbBigQuery  = "bigquery"
	TypeDbSnowflake = "snowflake"
	TypeDbSQLite    = "sqlite"
	TypeDbSQLServer = "sqlserver"
	TypeDbAzure     = "azuresql"

	TypeAPIGit    = "git"
	TypeAPIGithub = "github"
)

// ValidateType returns true is type is valid
func ValidateType(tStr string) (Type, bool) {
	t := Type(tStr)

	tMap := map[string]Type{
		"postgresql": TypeDbPostgres,
	}

	if tMatched, ok := tMap[tStr]; ok {
		t = tMatched
	}

	switch t {
	case
		TypeFileLocal, TypeFileS3, TypeFileAzure, TypeFileGoogle, TypeFileSftp,

		TypeDbPostgres, TypeDbRedshift, TypeDbMySQL, TypeDbOracle, TypeDbBigQuery, TypeDbSnowflake, TypeDbSQLite, TypeDbSQLServer, TypeDbAzure:
		return t, true
	}

	return t, false
}

// String returns string instance
func (t Type) String() string {
	return string(t)
}

// Kind returns the kind of connection
func (t Type) Kind() Kind {
	switch t {
	case TypeDbPostgres, TypeDbRedshift, TypeDbMySQL, TypeDbOracle, TypeDbBigQuery,
		TypeDbSnowflake, TypeDbSQLite, TypeDbSQLServer, TypeDbAzure:
		return KindDatabase
	case TypeFileLocal, TypeFileHDFS, TypeFileS3, TypeFileAzure, TypeFileGoogle, TypeFileSftp, TypeFileHTTP:
		return KindFile
	case TypeAPIGit, TypeAPIGithub:
		return KindAPI
	}
	return KindUnknown
}

// Info is the connection type
type Info struct {
	Name string
	Type Type
}

// Connection is a connection
type Connection interface {
	// Self() Connection
	Close() error
	Context() g.Context
	Info() Info
	URL() string
	// ToMap() map[string]interface{}
	AsDatabase() database.Connection
	AsFile() filesys.FileSysClient
	// AsAPI() interface{}
}

// connBase is the base connection struct
type connBase struct {
	ID      string                 `json:"id"`
	Type    Type                   `json:"type"`
	Data    map[string]interface{} `json:"data"`
	context g.Context              `json:"-"`
}

// NewConnection creates a new connection
func NewConnection(ID string, t Type, Data map[string]interface{}) (conn Connection, err error) {
	c := g.NewContext(context.Background())

	if t == TypeUnknown {
		return conn, g.Error("must specify connection type")
	}

	b := connBase{ID: ID, Type: t, Data: Data, context: c}
	err = b.setURL()
	if err != nil {
		return conn, g.Error(err, "could not set URL for %s: %s", t, ID)
	}

	_, ok := ValidateType(t.String())
	if !ok {
		return conn, g.Error("unsupported type: %s", t.String())
	}

	switch t.Kind() {
	case KindDatabase:
		dbConn, err := database.NewConnContext(
			b.Context().Ctx, b.URL(), g.MapToKVArr(b.DataS())...,
		)
		if err != nil {
			return conn, g.Error(err, "could not connect to %s: %s", t, ID)
		}

		dbConnPtr := &connDatabase{
			connBase: b,
			conn:     dbConn,
		}
		conn = dbConnPtr

	case KindFile:
		fileConn, err := filesys.NewFileSysClientFromURLContext(
			b.Context().Ctx, b.URL(), g.MapToKVArr(b.DataS())...,
		)
		if err != nil {
			return conn, g.Error(err, "could not connect to %s: %s", t, ID)
		}
		fileConnPtr := &connFile{
			connBase: b,
			conn:     fileConn,
		}
		conn = fileConnPtr

	default:
		err = g.Error("unsupported connection type (%s)", t)
	}
	return conn, err
}

// NewConnectionFromURL creates a new connection from a url
func NewConnectionFromURL(ID, URL string) (conn Connection, err error) {
	U, err := net.NewURL(URL)
	if err != nil {
		return conn, g.Error("could not parse provided url")
	}

	t, ok := ValidateType(U.U.Scheme)
	if !ok {
		return conn, g.Error("unsupported type: %s", U.U.Scheme)
	}

	return NewConnection(ID, t, g.M("url", URL))
}

// Info returns connection information
func (c *connBase) Info() Info {
	return Info{
		Name: c.ID,
		Type: c.Type,
	}
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

func (c *connBase) AsDatabase() database.Connection {
	return nil
}

func (c *connBase) AsFile() filesys.FileSysClient {
	return nil
}

// setURL sets the url
func (c *connBase) setURL() (err error) {

	if c.URL() != "" {
		return
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

	// setDefault sets a default value if key is not present
	setDefault := func(key string, val interface{}) {
		if _, ok := c.Data[key]; !ok {
			c.Data[key] = val
		}
	}

	switch c.Type {
	case TypeDbOracle:
		setDefault("password", "")
		if tns, ok := c.Data["tns"]; ok {
			if !strings.HasPrefix(cast.ToString(tns), "(") {
				c.Data["tns"] = "(" + cast.ToString(tns) + ")"
			}
			template = "oracle://{username}:{password}@{tns}"
		} else {
			template = "oracle://{username}:{password}@{host}:{port}/{sid}"
		}
	case TypeDbPostgres:
		setDefault("password", "")
		setDefault("sslmode", "prefer")
		template = "postgresql://{username}:{password}@{host}:{port}/{database}?sslmode={sslmode}"
	case TypeDbRedshift:
		setDefault("password", "")
		setDefault("sslmode", "prefer")
		template = "redshift://{username}:{password}@{host}:{port}/{database}?sslmode={sslmode}"
	case TypeDbMySQL:
		setDefault("password", "")
		template = "mysql://{username}:{password}@{host}:{port}/{database}"
	case TypeDbBigQuery:
		template = "bigquery://{project_id}/{location}/{dataset_id}"
	case TypeDbSnowflake:
		setDefault("schema", "public")
		template = "snowflake://{username}:{password}@{host}.snowflakecomputing.com:443/{database}?schema={schema}&warehouse={warehouse}"
	case TypeDbSQLite:
		template = "sqlite:///{database}"
	case TypeDbSQLServer, TypeDbAzure:
		setDefault("password", "")
		template = "sqlserver://{username}:{password}@{host}:{port}/{database}"
	default:
		return g.Error("unrecognized type: %s", c.Type)
	}

	keys := g.MatchesGroup(template, "{([a-zA-Z]+)}", 0)
	if err := checkData(keys...); err != nil {
		return g.Error(err, "keys missing")
	}
	c.Data["url"] = g.Rm(template, c.Data)

	return nil
}

type connDatabase struct {
	connBase
	conn database.Connection
}

// Close closes the connection
func (c *connDatabase) Close() error {
	return c.conn.Close()
}

func (c *connDatabase) AsDatabase() database.Connection {
	return c.conn
}

type connFile struct {
	connBase
	conn filesys.FileSysClient
}

// Close closes the connection
func (c *connFile) Close() error {
	return nil
}

func (c *connFile) AsFile() filesys.FileSysClient {
	return c.conn
}
