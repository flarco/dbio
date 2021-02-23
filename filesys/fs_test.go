package filesys

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/flarco/dbio"

	"github.com/flarco/dbio/iop"

	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
)

func TestFileSysLocal(t *testing.T) {
	t.Parallel()
	fs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	// Test List
	paths, err := fs.List(".")
	assert.NoError(t, err)
	assert.Contains(t, paths, "./fs_test.go")

	paths, err = fs.ListRecursive(".")
	assert.NoError(t, err)
	assert.Contains(t, paths, "test/test1/test1.csv")

	// Test Delete, Write, Read
	testPath := "test/fs.test"
	testString := "abcde"
	fs.Delete(testPath)
	reader := strings.NewReader(testString)
	_, err = fs.Write(testPath, reader)
	assert.NoError(t, err)

	readers, err := fs.GetReaders(testPath)
	if !assert.NoError(t, err) {
		return
	}

	testBytes, err := ioutil.ReadAll(readers[0])
	assert.NoError(t, err)
	assert.Equal(t, testString, string(testBytes))

	err = fs.Delete(testPath)
	assert.NoError(t, err)

	paths, err = fs.ListRecursive(".")
	assert.NoError(t, err)
	assert.NotContains(t, paths, "./"+testPath)

	// Test datastream
	df, err := fs.ReadDataflow("test/test1")
	assert.NoError(t, err)

	data, err := iop.Collect(df.Streams...)
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, len(data.Rows))

	fs.SetProp("header", "FALSE")
	df1, err := fs.ReadDataflow("test/test2/test2.1.noheader.csv")
	assert.NoError(t, err)

	data1, err := iop.Collect(df1.Streams...)
	assert.NoError(t, err)
	assert.EqualValues(t, 18, len(data1.Rows))

}

func TestFileSysDOSpaces(t *testing.T) {
	fs, err := NewFileSysClient(
		dbio.TypeFileS3,
		// "AWS_ENDPOINT=https://nyc3.digitaloceanspaces.com",
		"AWS_ENDPOINT=nyc3.digitaloceanspaces.com",
		"AWS_ACCESS_KEY_ID="+os.Getenv("DOS_ACCESS_KEY_ID"),
		"AWS_SECRET_ACCESS_KEY="+os.Getenv("DOS_SECRET_ACCESS_KEY"),
	)
	assert.NoError(t, err)

	// Test List
	paths, err := fs.List("s3://ocral/")
	assert.NoError(t, err)

	// Test Delete, Write, Read
	testPath := "s3://ocral/test/fs.test"
	testString := "abcde"
	err = fs.Delete(testPath)
	assert.NoError(t, err)

	reader := strings.NewReader(testString)
	bw, err := fs.Write(testPath, reader)
	assert.EqualValues(t, 5, bw)

	// writer, err := fs.GetWriter(testPath)
	// bw, err := Write(reader, writer)
	assert.NoError(t, err)

	reader2, err := fs.GetReader(testPath)
	assert.NoError(t, err)

	testBytes, err := ioutil.ReadAll(reader2)
	assert.NoError(t, err)

	assert.Equal(t, testString, string(testBytes))
	err = fs.Delete(testPath)
	assert.NoError(t, err)

	paths, err = fs.ListRecursive("s3://ocral/")
	assert.NoError(t, err)
	assert.NotContains(t, paths, testPath)

	paths, err = fs.ListRecursive("s3://ocral/test/")

	// Test datastream
	df, err := fs.ReadDataflow("s3://ocral/test/")
	assert.NoError(t, err)

	for ds := range df.StreamCh {
		data, err := ds.Collect(0)
		assert.NoError(t, err)
		assert.EqualValues(t, 18, len(data.Rows))
	}

	// Test concurrent wrinting from datastream

	localFs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	df2, err := localFs.ReadDataflow("test/test1")
	assert.NoError(t, err)
	// assert.EqualValues(t, 3, len(df2.Streams))

	writeFolderPath := "s3://ocral/test.fs.write"
	_, err = fs.WriteDataflow(df2, writeFolderPath)
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, df2.Count())

	// eventual consistency
	time.Sleep(2 * time.Second) // wait to s3 files to write on AWS side
	df3, err := fs.ReadDataflow(writeFolderPath)
	assert.NoError(t, err)

	data2, err := iop.Collect(df3.Streams...)
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, len(data2.Rows))
}

func TestFileSysLarge(t *testing.T) {
	path := ""
	// path = "s3://ocral-data-1/LargeDataset.csv"
	// path = "s3://ocral-data-1"
	path = "s3://ocral-data-1/test.fs.write/part.01.0001.csv"
	path = "s3://ocral-data-1/test.fs.write/part"
	// path = "gs://flarco_us_bucket/test"
	// path = "gs://flarco_us_bucket/test/part"
	// path = "https://flarcostorage.blob.core.windows.net/testcont/test2"
	// path = "https://flarcostorage.blob.core.windows.net/testcont/test2/part"
	fs, err := NewFileSysClientFromURL(path)
	assert.NoError(t, err)

	paths, err := fs.List(path)
	assert.NoError(t, err)
	g.P(paths)

	return

	df, err := fs.ReadDataflow(path, 10000)
	assert.NoError(t, err)

	for ds := range df.StreamCh {
		data, err := ds.Collect(0)
		assert.NoError(t, err)
		println(len(data.Rows))
	}
	assert.NoError(t, df.Context.Err())
}

func TestFileSysS3(t *testing.T) {
	t.Parallel()
	fs, err := NewFileSysClient(dbio.TypeFileS3)
	// fs, err := NewFileSysClient(S3cFileSys, "AWS_ENDPOINT=s3.amazonaws.com")
	assert.NoError(t, err)

	// Test List
	paths, err := fs.List("s3://ocral-data-1/")
	assert.NoError(t, err)
	assert.Contains(t, paths, "s3://ocral-data-1/test/")

	paths, err = fs.ListRecursive("s3://ocral-data-1/")
	assert.NoError(t, err)
	// helpers.P(paths)
	assert.Contains(t, paths, "s3://ocral-data-1/test/test1.1.csv")

	// Test Delete, Write, Read
	testPath := "s3://ocral-data-1/test/fs.test"
	testString := "abcde"
	err = fs.Delete(testPath)
	assert.NoError(t, err)

	reader := strings.NewReader(testString)
	bw, err := fs.Write(testPath, reader)
	assert.EqualValues(t, 5, bw)

	// writer, err := fs.GetWriter(testPath)
	// bw, err := Write(reader, writer)
	assert.NoError(t, err)

	reader2, err := fs.GetReader(testPath)
	assert.NoError(t, err)

	testBytes, err := ioutil.ReadAll(reader2)
	assert.NoError(t, err)

	assert.Equal(t, testString, string(testBytes))
	err = fs.Delete(testPath)
	assert.NoError(t, err)

	paths, err = fs.ListRecursive("s3://ocral-data-1/")
	assert.NoError(t, err)
	assert.NotContains(t, paths, testPath)

	// Test datastream
	df, err := fs.ReadDataflow("s3://ocral-data-1/test/")
	assert.NoError(t, err)

	for ds := range df.StreamCh {
		data, err := ds.Collect(0)
		assert.NoError(t, err)
		assert.EqualValues(t, 18, len(data.Rows))
	}

	// Test concurrent wrinting from datastream

	localFs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	df2, err := localFs.ReadDataflow("test/test1")
	assert.NoError(t, err)
	// assert.EqualValues(t, 3, len(df2.Streams))

	writeFolderPath := "s3://ocral-data-1/test.fs.write"
	err = fs.Delete(writeFolderPath)
	assert.NoError(t, err)

	fs.SetProp("FILE_BYTES_LIMIT", "20000")
	_, err = fs.WriteDataflow(df2, writeFolderPath)
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, df2.Count())

	// eventual consistency
	time.Sleep(2 * time.Second) // wait to s3 files to write on AWS side
	df3, err := fs.ReadDataflow(writeFolderPath, 1)
	assert.NoError(t, err)

	data2, err := iop.Collect(df3.Streams...)
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, len(data2.Rows))

	// if fs.Context().Err == nil {
	// 	data, err := Collect(dss...)
	// 	assert.NoError(t, err)
	// 	assert.EqualValues(t, 1036, len(data.Rows))
	// }

}

func TestFileSysAzure(t *testing.T) {
	t.Parallel()

	fs, err := NewFileSysClient(dbio.TypeFileAzure)
	assert.NoError(t, err)

	testString := "abcde"
	testPath := "https://flarcostorage.blob.core.windows.net/testcont/test1"
	reader := strings.NewReader(testString)
	bw, err := fs.Write(testPath, reader)
	assert.EqualValues(t, 5, bw)
	assert.NoError(t, err)

	reader2, err := fs.GetReader(testPath)
	assert.NoError(t, err)

	testBytes, err := ioutil.ReadAll(reader2)
	assert.NoError(t, err)

	assert.Equal(t, testString, string(testBytes))
	err = fs.Delete(testPath)
	assert.NoError(t, err)

	paths, err := fs.ListRecursive("https://flarcostorage.blob.core.windows.net")
	assert.NoError(t, err)
	assert.NotContains(t, paths, testPath)

	localFs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	df2, err := localFs.ReadDataflow("test/test1")
	assert.NoError(t, err)
	// assert.EqualValues(t, 3, len(df2.Streams))

	writeFolderPath := "https://flarcostorage.blob.core.windows.net/testcont/test2"
	_, err = fs.WriteDataflow(df2, writeFolderPath)
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, df2.Count())

	df3, err := fs.ReadDataflow(writeFolderPath)
	if assert.NoError(t, err) {
		data2, err := iop.Collect(df3.Streams...)
		assert.NoError(t, err)
		assert.EqualValues(t, 1036, len(data2.Rows))
	}

	df3, err = fs.ReadDataflow("https://flarcostorage.blob.core.windows.net/testcont/test2/part.01.0001.csv")
	if assert.NoError(t, err) {
		data2, err := iop.Collect(df3.Streams...)
		assert.NoError(t, err)
		assert.Greater(t, len(data2.Rows), 0)
	}
	// fs.Delete(writeFolderPath)
}

func TestFileSysGoogle(t *testing.T) {
	t.Parallel()

	fs, err := NewFileSysClient(dbio.TypeFileGoogle, "GC_BUCKET=flarco_us_bucket")
	assert.NoError(t, err)

	testString := "abcde"
	testPath := "gs://flarco_us_bucket/test/test1"
	reader := strings.NewReader(testString)
	bw, err := fs.Write(testPath, reader)
	assert.EqualValues(t, 5, bw)
	assert.NoError(t, err)

	reader2, err := fs.GetReader(testPath)
	assert.NoError(t, err)

	testBytes, err := ioutil.ReadAll(reader2)
	assert.NoError(t, err)

	assert.Equal(t, testString, string(testBytes))

	err = fs.Delete(testPath)
	assert.NoError(t, err)

	paths, err := fs.ListRecursive("gs://flarco_us_bucket/test")
	assert.NoError(t, err)
	assert.NotContains(t, paths, testPath)

	localFs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	df2, err := localFs.ReadDataflow("test/test1")
	assert.NoError(t, err)
	// assert.EqualValues(t, 3, len(df2.Streams))

	writeFolderPath := "gs://flarco_us_bucket/test"
	_, err = fs.WriteDataflow(df2, writeFolderPath)
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, df2.Count())

	df3, err := fs.ReadDataflow(writeFolderPath)
	assert.NoError(t, err)

	data2, err := iop.Collect(df3.Streams...)
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, len(data2.Rows))
}

func TestFileSysSftp(t *testing.T) {
	t.Parallel()

	fs, err := NewFileSysClient(
		dbio.TypeFileSftp,
		// "SSH_PRIVATE_KEY=/root/.ssh/id_rsa",
		"SFTP_URL="+os.Getenv("SSH_TEST_PASSWD_URL"),
	)
	assert.NoError(t, err)

	root := os.Getenv("SSH_TEST_PASSWD_URL")

	testString := "abcde"
	testPath := root + "/tmp/test/test1"
	reader := strings.NewReader(testString)
	bw, err := fs.Write(testPath, reader)
	assert.NoError(t, err)
	assert.EqualValues(t, 5, bw)

	reader2, err := fs.GetReader(testPath)
	assert.NoError(t, err)

	testBytes, err := ioutil.ReadAll(reader2)
	assert.NoError(t, err)

	assert.Equal(t, testString, string(testBytes))

	err = fs.Delete(testPath)
	assert.NoError(t, err)

	paths, err := fs.ListRecursive(root + "/tmp/test")
	assert.NoError(t, err)
	assert.NotContains(t, paths, testPath)

	localFs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	df2, err := localFs.ReadDataflow("test/test1")
	assert.NoError(t, err)
	// assert.EqualValues(t, 3, len(df2.Streams))

	writeFolderPath := root + "/tmp/test"
	_, err = fs.WriteDataflow(df2, writeFolderPath)
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, df2.Count())

	df3, err := fs.ReadDataflow(writeFolderPath)
	assert.NoError(t, err)

	data2, err := iop.Collect(df3.Streams...)
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, len(data2.Rows))

	err = fs.Delete(writeFolderPath)
	assert.NoError(t, err)
}

func TestFileSysHTTP(t *testing.T) {
	fs, err := NewFileSysClient(
		dbio.TypeFileHTTP, //"HTTP_USER=user", "HTTP_PASSWORD=password",
	)
	paths, err := fs.List("https://repo.anaconda.com/miniconda/")
	assert.NoError(t, err)
	assert.Greater(t, len(paths), 100)

	// paths, err = fs.List("https://privatefiles.ocral.org/")
	// assert.NoError(t, err)
	// g.P(paths)

	sampleCsv := "http://samplecsvs.s3.amazonaws.com/Sacramentorealestatetransactions.csv"
	// sampleCsv = "https://people.sc.fsu.edu/~jburkardt/data/csv/hw_25000.csv"
	sampleCsv = "https://www.stats.govt.nz/assets/Uploads/Business-price-indexes/Business-price-indexes-December-2019-quarter/Download-data/business-price-indexes-december-2019-quarter-csv.csv"
	// sampleCsv = "http://hci.stanford.edu/courses/cs448b/data/ipasn/cs448b_ipasn.csv"
	// sampleCsv = "https://perso.telecom-paristech.fr/eagan/class/igr204/data/BabyData.zip"
	sampleCsv = "https://people.sc.fsu.edu/~jburkardt/data/csv/freshman_kgs.csv"
	df, err := fs.ReadDataflow(sampleCsv)
	if !assert.NoError(t, err) {
		return
	}

	for _, ds := range df.Streams {
		data, err := iop.Collect(ds)
		assert.NoError(t, err)
		assert.Greater(t, len(data.Rows), 0)
		// g.P(len(data.Rows))
		for _, row := range data.Rows {
			g.P(row)
		}
	}
}

func testManyCSV(t *testing.T) {
	fs, err := NewFileSysClient(dbio.TypeFileHTTP, "concurencyLimit=5")
	paths, err := fs.List("https://people.sc.fsu.edu/~jburkardt/data/csv/csv.html")
	// paths, err := fs.List("https://www.stats.govt.nz/large-datasets/csv-files-for-download/")
	assert.NoError(t, err)

	// println(strings.Join(paths, "\n"))

	csvPaths := []string{}
	dss := []*iop.Datastream{}
	for _, path := range paths {
		if strings.HasSuffix(path, ".csv") {
			csvPaths = append(csvPaths, path)
			g.Debug("added csvPath %s", path)
			ds, err := fs.Self().GetDatastream(path)
			g.LogError(err, "could not parse "+path)
			if err == nil {
				dss = append(dss, ds)
			}
			// data, err := ds.Collect(0)
			// assert.NoError(t, err)
			// g.Debug("%d rows collected from %s", len(data.Rows), path)
		}
	}

	g.Debug("%d csvPaths", len(csvPaths))

	for i, ds := range dss {
		data, err := iop.Collect(ds)
		g.Debug("%d rows collected from %s", len(data.Rows), csvPaths[i])
		if assert.NoError(t, err, "for "+csvPaths[i]) {
			assert.Greater(t, len(data.Rows), 0)
		}
	}
}
