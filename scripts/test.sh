set -e  # exit on error

export _DEBUG=''

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

cd saas
go test -run 'TestAirbyteGithub|TestAirbyteNotion'
cd -