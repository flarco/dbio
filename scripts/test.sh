set -e  # exit on error

go mod tidy

cd connection
# go test .
cd -

# cd database
# go test .
# cd -

cd filesys
go test -run TestFileSysLocal
cd -

