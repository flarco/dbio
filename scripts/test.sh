set -e  # exit on error

cd connection
go test
cd -

cd iop
go test -run 'TestParseDate'
cd -

cd database
go test -run 'TestPostgres|TestMySQL|TestOracle|TestSnowflake|TestSqlServer|TestBigQuery|TestSQLite'
go test -run TestLargeDataset
cd -

cd filesys
go test -run 'TestFileSysLocal|TestFileSysGoogle|TestFileSysS3|TestFileSysAzure|TestFileSysSftp|TestExcel'
cd -