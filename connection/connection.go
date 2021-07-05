package connection

import (
	"context"
	"io/ioutil"
	"net/url"
	"os"
	"strings"

	"github.com/flarco/dbio"
	"gopkg.in/yaml.v2"

	"github.com/flarco/dbio/database"
	"github.com/flarco/dbio/filesys"
	"github.com/flarco/dbio/saas/airbyte"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/spf13/cast"
)

// Info is the connection type
type Info struct {
	Name     string
	Type     dbio.Type
	Database string
	Data     map[string]interface{}
}

// ConnectionInt is a connection
type ConnectionInt interface {
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

// Connection is the base connection struct
type Connection struct {
	Name    string                 `json:"name"`
	Type    dbio.Type              `json:"type"`
	Data    map[string]interface{} `json:"data"`
	context *g.Context             `json:"-"`
}

// NewConnection creates a new connection
func NewConnection(Name string, t dbio.Type, Data map[string]interface{}) (conn Connection, err error) {
	c := g.NewContext(context.Background())
	conn = Connection{
		Name:    strings.TrimLeft(Name, "$"),
		Type:    t,
		Data:    g.AsMap(Data, true),
		context: &c,
	}

	err = conn.setURL()
	if err != nil {
		return conn, g.Error(err, "could not set URL for %s: %s", conn.Type, Name)
	}

	return conn, err
}

// NewConnectionFromURL creates a new connection from a url
func NewConnectionFromURL(Name, URL string) (conn Connection, err error) {
	return NewConnection(Name, "", g.M("url", URL))
}

// NewConnectionFromMap loads a Connection from a Map
func NewConnectionFromMap(m map[string]interface{}) (c Connection, err error) {
	name := cast.ToString(m["name"])
	Type := cast.ToString(m["type"])

	c, err = NewConnection(
		name,
		dbio.Type(Type),
		g.AsMap(m["data"]),
	)

	if Type == "" {
		c2, err1 := NewConnectionFromDbt(name)
		if c2.Type != "" {
			c = c2
			return
		}

		c2, err1 = NewConnectionFromProfiles(name)
		if c2.Type != "" {
			c = c2
			return
		}
		err = err1
	}

	return
}

// NewConnectionFromDbt loads a Connection from a DBT Profile
func NewConnectionFromDbt(name string) (c Connection, err error) {
	conns, err := ReadDbtConnections()
	if err != nil {
		err = g.Error(err)
		return
	}

	if conn, ok := conns[name]; ok {
		return conn, nil
	}
	return
}

// NewConnectionFromProfiles loads a Connection from YAML Profiles
func NewConnectionFromProfiles(name string) (c Connection, err error) {
	profileConns := map[string]Connection{}
	for _, path := range strings.Split(os.Getenv("DBIO_PROFILE_PATHS"), ",") {
		if strings.TrimSpace(path) == "" {
			continue
		}
		conns, err := ReadConnections(path)
		if err != nil {
			err = g.Error(err)
			return c, err
		}

		for k, v := range conns {
			profileConns[k] = v
		}
	}

	if conn, ok := profileConns[name]; ok {
		return conn, nil
	}
	return
}

// Info returns connection information
func (c *Connection) Info() Info {
	return Info{
		Name:     c.Name,
		Type:     c.Type,
		Database: c.DataS(true)["database"],
		Data:     c.Data,
	}
}

// ToMap transforms DataConn to a Map
func (c *Connection) ToMap() map[string]interface{} {
	return g.M("name", c.Name, "type", c.Type, "data", c.Data)
}

// Set sets key/values from a map
func (c *Connection) Set(m map[string]interface{}) {
	for k, v := range m {
		c.Data[k] = v
	}
	c.setURL()
}

// DataS returns data as map[string]string
func (c *Connection) DataS(lowerCase ...bool) map[string]string {
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
func (c *Connection) Context() *g.Context {
	return c.context
}

// URL returns the url string
func (c *Connection) URL() string {
	return cast.ToString(c.Data["url"])
}

func (c *Connection) AsDatabase() (database.Connection, error) {
	return database.NewConnContext(
		c.Context().Ctx, c.URL(), g.MapToKVArr(c.DataS())...,
	)
}

func (c *Connection) AsFile() (filesys.FileSysClient, error) {
	return filesys.NewFileSysClientFromURLContext(
		c.Context().Ctx, c.URL(), g.MapToKVArr(c.DataS())...,
	)
}

func (c *Connection) AsAirbyte() (*airbyte.Airbyte, error) {
	return airbyte.NewAirbyteConnection(c.Type.String(), c.Data)
}

func (c *Connection) setFromEnv() {
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
func (c *Connection) Close() error {
	return nil
}

func (c *Connection) setURL() (err error) {
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

		c.Type, _ = dbio.ValidateType(U.U.Scheme)

		if c.Type.IsDb() {
			// set props from URL
			setIfMissing("schema", U.PopParam("schema"))
			setIfMissing("host", U.Hostname())
			setIfMissing("username", U.Username())
			setIfMissing("password", U.Password())
			setIfMissing("port", U.Port(c.Info().Type.DefPort()))
			setIfMissing("database", strings.ReplaceAll(U.Path(), "/", ""))
			if c.Type == dbio.TypeDbSnowflake {
				setIfMissing("warehouse", U.PopParam("warehouse"))
			} else if c.Type == dbio.TypeDbBigQuery {
				setIfMissing("schema", c.Data["dataset_id"])
			}
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

	setIfMissing("username", c.Data["user"])
	switch c.Type {
	case dbio.TypeDbOracle:
		setIfMissing("password", "")
		setIfMissing("sid", c.Data["database"])
		setIfMissing("port", c.Type.DefPort())
		if tns, ok := c.Data["tns"]; ok && cast.ToString(tns) != "" {
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
		setIfMissing("database", c.Data["dbname"])
		template = "postgresql://{username}:{password}@{host}:{port}/{database}?sslmode={sslmode}"
	case dbio.TypeDbRedshift:
		setIfMissing("password", "")
		setIfMissing("sslmode", "disable")
		setIfMissing("port", c.Type.DefPort())
		setIfMissing("database", c.Data["dbname"])
		template = "redshift://{username}:{password}@{host}:{port}/{database}?sslmode={sslmode}"
	case dbio.TypeDbMySQL:
		setIfMissing("password", "")
		setIfMissing("port", c.Type.DefPort())
		template = "mysql://{username}:{password}@{host}:{port}/{database}"
	case dbio.TypeDbBigQuery:
		template = "bigquery://{project_id}/{location}/{dataset_id}"
	case dbio.TypeDbSnowflake:
		// setIfMissing("schema", "public")
		// template = "snowflake://{username}:{password}@{host}.snowflakecomputing.com:443/{database}?schema={schema}&warehouse={warehouse}"
		setIfMissing("host", c.Data["account"])
		template = "snowflake://{username}:{password}@{host}.snowflakecomputing.com:443/{database}?warehouse={warehouse}"
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
	case dbio.TypeFileLocal:
		return nil
	default:
		if c.Type.IsUnknown() {
			g.Trace("no type detected")
		}
		return nil
	}

	keys := g.MatchesGroup(template, "{([a-zA-Z]+)}", 0)
	if err := checkData(keys...); err != nil {
		return g.Error(err, "required keys not provided")
	}

	// set URL is missing
	urlData := g.M()
	for k, v := range c.Data {
		urlData[k] = v
	}
	urlData["password"] = url.QueryEscape(cast.ToString(urlData["password"]))
	setIfMissing("url", g.Rm(template, urlData))

	return nil
}

// CopyDirect copies directly from cloud files
// (without passing through dbio)
func CopyDirect(conn database.Connection, tableFName string, srcFile Connection) (cnt uint64, ok bool, err error) {
	if !srcFile.Info().Type.IsFile() {
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
		// dbio.TypeAPIGithub:   "API - Github",
	}

	AirbyteSpecs, _ := airbyte.GetAirbyteSpecs()
	for k, spec := range AirbyteSpecs {
		t := dbio.Type(k)
		if _, ok := mapping[t]; !ok {
			mapping[t] = "API - " + spec.Title
		}
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
		// dbio.TypeAPIGithub:   "Github",
	}

	AirbyteSpecs, _ := airbyte.GetAirbyteSpecs()
	for k, spec := range AirbyteSpecs {
		t := dbio.Type(k)
		if _, ok := mapping[t]; !ok {
			mapping[t] = spec.Title
		}
	}
	return mapping[c.Info().Type]
}

func ReadDbtConnections() (conns map[string]Connection, err error) {
	conns = map[string]Connection{}

	profileDir := strings.TrimSuffix(os.Getenv("DBT_PROFILES_DIR"), "/")
	if profileDir == "" {
		profileDir = g.UserHomeDir() + "/.dbt"
	}
	path := profileDir + "/profiles.yml"
	if !g.PathExists(path) {
		return
	}

	file, err := os.Open(path)
	if err != nil {
		err = g.Error(err, "error reading from yaml: %s", path)
		return
	}

	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		err = g.Error(err, "error reading bytes from yaml: %s", path)
		return
	}

	type ProfileConn struct {
		Target  string           `json:"target" yaml:"target"`
		Outputs map[string]g.Map `json:"outputs" yaml:"outputs"`
	}

	dbtProfile := map[string]ProfileConn{}
	err = yaml.Unmarshal(bytes, &dbtProfile)
	if err != nil {
		err = g.Error(err, "error parsing yaml string")
		return
	}

	for pName, pc := range dbtProfile {
		for target, data := range pc.Outputs {
			connName := strings.ToUpper(pName + "/" + target)
			data["dbt"] = true

			conn, err := NewConnectionFromMap(
				g.M("name", connName, "data", data, "type", data["type"]),
			)
			if err != nil {
				g.Warn("could not load dbt connection %s", connName)
				g.LogError(err)
				continue
			}

			conns[connName] = conn
			// g.Trace("found connection from dbt profiles YAML: " + connName)
		}
	}

	return
}

// ReadConnections loads the connections
func ReadConnections(path string) (conns map[string]Connection, err error) {
	conns = map[string]Connection{}

	if !g.PathExists(path) {
		return
	}

	profile := map[string]map[string]interface{}{}
	file, err := os.Open(path)
	if err != nil {
		err = g.Error(err, "error reading from yaml")
		return
	}

	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		err = g.Error(err, "error reading bytes from yaml")
		return
	}

	err = yaml.Unmarshal(bytes, profile)
	if err != nil {
		err = g.Error(err, "error parsing yaml string")
		return
	}

	if connections, ok := profile["connections"]; ok {
		for name, v := range connections {
			switch v.(type) {
			case map[string]interface{}, map[interface{}]interface{}:
				data := cast.ToStringMap(v)
				if n := cast.ToString(data["name"]); n != "" {
					data["name"] = name
				}

				conn, err := NewConnectionFromMap(g.M("name", name, "data", data, "type", data["type"]))
				if err != nil {
					g.Warn("could not load connection %s", name)
					g.LogError(err)
					continue
				}

				conns[name] = conn
				g.Trace("found connection from YAML: " + name)
			default:
				g.Warn("did not handle %s", name)
			}
		}
	}
	return
}
