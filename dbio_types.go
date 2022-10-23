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

	TypeFileLocal  Type = "file"
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
	TypeDbBigTable   Type = "bigtable"
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
	case TypeDbPostgres, TypeDbRedshift, TypeDbMySQL, TypeDbOracle, TypeDbBigQuery, TypeDbBigTable,
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

// IsDb returns true if database connection
func (t Type) IsNoSQL() bool {
	return t == TypeDbBigTable
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

// NameLong return the type long name
func (t Type) NameLong() string {
	mapping := map[Type]string{
		TypeFileLocal:    "FileSys - Local",
		TypeFileHDFS:     "FileSys - HDFS",
		TypeFileS3:       "FileSys - S3",
		TypeFileAzure:    "FileSys - Azure",
		TypeFileGoogle:   "FileSys - Google",
		TypeFileSftp:     "FileSys - Sftp",
		TypeFileHTTP:     "FileSys - HTTP",
		TypeDbPostgres:   "DB - PostgreSQL",
		TypeDbRedshift:   "DB - Redshift",
		TypeDbMySQL:      "DB - MySQL",
		TypeDbOracle:     "DB - Oracle",
		TypeDbBigQuery:   "DB - BigQuery",
		TypeDbSnowflake:  "DB - Snowflake",
		TypeDbSQLite:     "DB - SQLite",
		TypeDbSQLServer:  "DB - SQLServer",
		TypeDbAzure:      "DB - Azure",
		TypeDbClickhouse: "DB - Clickhouse",
		TypeAPIGit:       "API - Git",
		// TypeAPIGithub:   "API - Github",
	}

	for k, spec := range AirbyteSpecs {
		t := Type(k)
		if _, ok := mapping[t]; !ok {
			mapping[t] = "API - " + spec.Title
		}
	}
	return mapping[t]
}

// Name return the type name
func (t Type) Name() string {
	mapping := map[Type]string{
		TypeFileLocal:    "Local",
		TypeFileHDFS:     "HDFS",
		TypeFileS3:       "S3",
		TypeFileAzure:    "Azure",
		TypeFileGoogle:   "Google",
		TypeFileSftp:     "Sftp",
		TypeFileHTTP:     "HTTP",
		TypeDbPostgres:   "PostgreSQL",
		TypeDbRedshift:   "Redshift",
		TypeDbMySQL:      "MySQL",
		TypeDbOracle:     "Oracle",
		TypeDbBigQuery:   "BigQuery",
		TypeDbSnowflake:  "Snowflake",
		TypeDbSQLite:     "SQLite",
		TypeDbSQLServer:  "SQLServer",
		TypeDbClickhouse: "Clickhouse",
		TypeDbAzure:      "Azure",
		TypeAPIGit:       "Git",
		// TypeAPIGithub:   "Github",
	}

	for k, spec := range AirbyteSpecs {
		t := Type(k)
		if _, ok := mapping[t]; !ok {
			mapping[t] = spec.Title
		}
	}
	return mapping[t]
}
