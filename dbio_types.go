package dbio

import (
	"github.com/flarco/dbio/saas/airbyte"
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
	// KindAPI for Airbyte
	KindAirbyte Kind = "airbyte"
	// KindUnknown for unknown
	KindUnknown Kind = ""
)

// Type is the connection type
type Type string

const (
	TypeUnknown Type = ""

	TypeFileLocal  Type = "local"
	TypeFileHDFS   Type = "hdfs"
	TypeFileS3     Type = "s3"
	TypeFileAzure  Type = "azure"
	TypeFileGoogle Type = "gs"
	TypeFileSftp   Type = "sftp"
	TypeFileHTTP   Type = "http"

	TypeDbPostgres   Type = "postgres"
	TypeDbRedshift   Type = "redshift"
	TypeDbMySQL      Type = "mysql"
	TypeDbOracle     Type = "oracle"
	TypeDbBigQuery   Type = "bigquery"
	TypeDbSnowflake  Type = "snowflake"
	TypeDbSQLite     Type = "sqlite"
	TypeDbSQLServer  Type = "sqlserver"
	TypeDbAzure      Type = "azuresql"
	TypeDbAzureDWH   Type = "azuredwh"
	TypeDbClickhouse Type = "clickhouse"

	TypeAPIGit Type = "git"
	// TypeAPIGithub Type = "github"
)

var AirbyteSpecs map[string]airbyte.ConnectionSpecification

func init() {
	AirbyteSpecs, _ = airbyte.GetAirbyteSpecs()
	// g.PP(AirbyteSpecs)
}

// ValidateType returns true is type is valid
func ValidateType(tStr string) (Type, bool) {
	t := Type(tStr)

	tMap := map[string]Type{
		"postgresql": TypeDbPostgres,
		"file":       TypeFileLocal,
	}

	if tMatched, ok := tMap[tStr]; ok {
		t = tMatched
	}

	switch t {
	case
		TypeFileLocal, TypeFileS3, TypeFileAzure, TypeFileGoogle, TypeFileSftp,

		TypeDbPostgres, TypeDbRedshift, TypeDbMySQL, TypeDbOracle, TypeDbBigQuery, TypeDbSnowflake, TypeDbSQLite, TypeDbSQLServer, TypeDbAzure, TypeDbAzureDWH:
		return t, true
	}

	if _, ok := AirbyteSpecs[t.String()]; ok {
		return t, true
	}

	return t, false
}

// String returns string instance
func (t Type) String() string {
	return string(t)
}

// DefPort returns the default port
func (t Type) DefPort() int {
	connTypesDefPort := map[Type]int{
		TypeDbPostgres:   5432,
		TypeDbRedshift:   5439,
		TypeDbMySQL:      3306,
		TypeDbOracle:     1521,
		TypeDbSQLServer:  1433,
		TypeDbAzure:      1433,
		TypeDbClickhouse: 9000,
	}
	return connTypesDefPort[t]
}

// Kind returns the kind of connection
func (t Type) Kind() Kind {
	switch t {
	case TypeDbPostgres, TypeDbRedshift, TypeDbMySQL, TypeDbOracle, TypeDbBigQuery,
		TypeDbSnowflake, TypeDbSQLite, TypeDbSQLServer, TypeDbAzure, TypeDbClickhouse:
		return KindDatabase
	case TypeFileLocal, TypeFileHDFS, TypeFileS3, TypeFileAzure, TypeFileGoogle, TypeFileSftp, TypeFileHTTP:
		return KindFile
	case TypeAPIGit:
		return KindAPI
	}
	if _, ok := AirbyteSpecs[t.String()]; ok {
		return KindAirbyte
	}
	return KindUnknown
}

// IsDb returns true if database connection
func (t Type) IsDb() bool {
	return t.Kind() == KindDatabase
}

// IsFile returns true if file connection
func (t Type) IsFile() bool {
	return t.Kind() == KindFile
}

// IsAPI returns true if API connection
func (t Type) IsAPI() bool {
	return t.Kind() == KindAPI
}

// IsAirbyte returns true if Airbyte connection
func (t Type) IsAirbyte() bool {
	return t.Kind() == KindAirbyte
}

// IsUnknown returns true if unknown
func (t Type) IsUnknown() bool {
	return t.Kind() == KindUnknown
}
