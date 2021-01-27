package dbio

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
	TypeDbAzureDWH  = "azuredwh"

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

		TypeDbPostgres, TypeDbRedshift, TypeDbMySQL, TypeDbOracle, TypeDbBigQuery, TypeDbSnowflake, TypeDbSQLite, TypeDbSQLServer, TypeDbAzure, TypeDbAzureDWH:
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
		TypeDbPostgres:  5432,
		TypeDbRedshift:  5439,
		TypeDbMySQL:     3306,
		TypeDbOracle:    1521,
		TypeDbSQLServer: 1433,
		TypeDbAzure:     1433,
	}
	return connTypesDefPort[t]
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
