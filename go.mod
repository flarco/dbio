module github.com/flarco/dbio

go 1.16

require (
	cloud.google.com/go v0.66.0
	cloud.google.com/go/bigquery v1.8.0
	cloud.google.com/go/storage v1.12.0
	github.com/360EntSecGroup-Skylar/excelize v1.4.1
	github.com/Azure/azure-sdk-for-go v48.0.0+incompatible
	github.com/Azure/azure-storage-blob-go v0.13.0
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/ClickHouse/clickhouse-go v1.4.5
	github.com/PuerkitoBio/goquery v1.6.0
	github.com/apache/arrow/go/arrow v0.0.0-20210531090448-9b68458bbf0a // indirect
	github.com/aws/aws-sdk-go v1.35.22
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.2.2 // indirect
	github.com/denisenkom/go-mssqldb v0.9.0
	github.com/digitalocean/godo v1.59.0
	github.com/dnaeon/go-vcr v1.1.0 // indirect
	github.com/dustin/go-humanize v1.0.0
	github.com/flarco/g v0.0.5
	github.com/form3tech-oss/jwt-go v3.2.3+incompatible // indirect
	github.com/go-ini/ini v1.62.0 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/godror/godror v0.25.1
	github.com/google/flatbuffers v2.0.0+incompatible // indirect
	github.com/google/uuid v1.2.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0
	github.com/jmoiron/sqlx v1.2.0
	github.com/json-iterator/go v1.1.10
	github.com/klauspost/compress v1.12.3 // indirect
	github.com/lib/pq v1.3.0
	github.com/mattn/go-sqlite3 v1.14.5
	github.com/minio/minio-go v6.0.14+incompatible
	github.com/pierrec/lz4/v4 v4.1.7 // indirect
	github.com/pkg/browser v0.0.0-20210115035449-ce105d075bb4 // indirect
	github.com/pkg/sftp v1.12.0
	github.com/shirou/gopsutil v2.20.8+incompatible
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/smartystreets/goconvey v1.6.4 // indirect
	github.com/snowflakedb/gosnowflake v1.5.1
	github.com/solcates/go-sql-bigquery v0.2.4
	github.com/spf13/cast v1.3.1
	github.com/stretchr/testify v1.7.0
	github.com/xitongsys/parquet-go v1.5.4
	github.com/xitongsys/parquet-go-source v0.0.0-20201014235637-c24a23d9ef1e
	github.com/xo/dburl v0.3.0
	golang.org/x/crypto v0.0.0-20210513164829-c07d793c2f9a
	golang.org/x/net v0.0.0-20210525063256-abc453219eb5 // indirect
	golang.org/x/oauth2 v0.0.0-20200902213428-5d25da1a8d43
	golang.org/x/sys v0.0.0-20210531080801-fdfd190a6549 // indirect
	google.golang.org/api v0.34.0
	gopkg.in/VividCortex/ewma.v1 v1.1.1 // indirect
	gopkg.in/cheggaaa/pb.v2 v2.0.7
	gopkg.in/fatih/color.v1 v1.7.0 // indirect
	gopkg.in/ini.v1 v1.62.0 // indirect
	gopkg.in/mattn/go-colorable.v0 v0.1.0 // indirect
	gopkg.in/mattn/go-isatty.v0 v0.0.4 // indirect
	gopkg.in/mattn/go-runewidth.v0 v0.0.4 // indirect
	gopkg.in/yaml.v2 v2.3.0
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c
	gorm.io/driver/postgres v1.0.5
	gorm.io/driver/sqlite v1.1.4
	gorm.io/gorm v1.20.7
	syreclabs.com/go/faker v1.2.2
)

// replace github.com/flarco/g => ../g
