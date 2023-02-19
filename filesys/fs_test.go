package filesys

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/flarco/dbio"
	"github.com/flarco/g/net"
	"github.com/spf13/cast"

	"github.com/flarco/dbio/iop"

	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
)

func TestFileSysLocalCsv(t *testing.T) {
	t.Parallel()
	fs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	// Test List
	paths, err := fs.List(".")
	assert.NoError(t, err)
	assert.Contains(t, paths, "./fs_test.go")

	paths, err = fs.ListRecursive(".")
	assert.NoError(t, err)
	assert.Contains(t, paths, "test/test1/csv/test1.csv")

	// Test Delete, Write, Read
	testPath := "test/fs.test"
	testString := "abcde"
	Delete(fs, testPath)
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

	err = Delete(fs, testPath)
	assert.NoError(t, err)

	paths, err = fs.ListRecursive(".")
	assert.NoError(t, err)
	assert.NotContains(t, paths, "./"+testPath)

	// Test datastream
	fs.SetProp("datetime_format", "02-01-2006 15:04:05.000")
	df, err := fs.ReadDataflow("test/test1/csv")
	assert.NoError(t, err)

	if t.Failed() {
		return
	}

	data, err := df.Collect()
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, len(data.Rows))
	assert.NoError(t, df.Err())

	fs.SetProp("header", "false")
	df1, err := fs.ReadDataflow("test/test2/test2.1.noheader.csv")
	assert.NoError(t, err)

	data1, err := df1.Collect()
	assert.NoError(t, err)
	assert.EqualValues(t, 18, len(data1.Rows))

}

func TestFileSysLocalFormat(t *testing.T) {
	t.Parallel()
	iop.SampleSize = 4
	fs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	// clean up existing
	os.RemoveAll("test/test_write")

	for _, format := range []FileType{FileTypeJson, FileTypeJsonLines, FileTypeCsv} {
		if t.Failed() {
			break
		}

		formatS := string(format)

		// file
		fs2, err := NewFileSysClient(dbio.TypeFileLocal, "FORMAT="+formatS)
		assert.NoError(t, err, formatS)
		df2, _ := fs.ReadDataflow("test/test2/test2.1.noheader.csv")
		_, err = fs2.WriteDataflow(df2, g.F("test/test_write/%s.test", formatS))
		assert.NoError(t, err, formatS)
		df3, err := fs2.ReadDataflow(g.F("test/test_write/%s.test", formatS))
		assert.NoError(t, err, formatS)
		_, err = df3.Collect()
		assert.NoError(t, err, formatS)
		assert.Equal(t, cast.ToInt(df2.Count()), cast.ToInt(df3.Count()))

		// folder
		fs2, err = NewFileSysClient(dbio.TypeFileLocal, "FORMAT="+formatS, "FILE_MAX_ROWS=5")
		assert.NoError(t, err, formatS)
		df2, _ = fs.ReadDataflow("test/test2/test2.1.noheader.csv")
		_, err = fs2.WriteDataflow(df2, g.F("test/test_write/%s.folder", formatS))
		assert.NoError(t, err, formatS)
		df3, err = fs2.ReadDataflow(g.F("test/test_write/%s.folder", formatS))
		assert.NoError(t, err, formatS)
		_, err = df3.Collect()
		assert.NoError(t, err, formatS)
		assert.Equal(t, cast.ToInt(df2.Count()), cast.ToInt(df3.Count()))

	}

	if !t.Failed() {
		os.RemoveAll("test/test_write")
	}
}

func TestFileSysLocalJson(t *testing.T) {
	t.Parallel()
	iop.SampleSize = 4
	fs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	df1, err := fs.ReadDataflow("test/test1/json")
	assert.NoError(t, err)

	data1, err := df1.Collect()
	assert.NoError(t, err)
	assert.EqualValues(t, 1019, len(data1.Rows))

	fs.SetProp("flatten", "true")
	df1, err = fs.ReadDataflow("test/test1/json")
	assert.NoError(t, err)

	data1, err = df1.Collect()
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, len(data1.Rows))

	fs.SetProp("flatten", "false")
	df2, err := fs.ReadDataflow("test/test2/json")
	assert.NoError(t, err)

	data2, err := df2.Collect()
	assert.NoError(t, err)
	assert.EqualValues(t, 20, len(data2.Rows))
	assert.EqualValues(t, 1, len(data2.Columns))

	fs.SetProp("flatten", "true")
	df2, err = fs.ReadDataflow("test/test2/json")
	assert.NoError(t, err)

	data2, err = df2.Collect()
	assert.NoError(t, err)
	assert.EqualValues(t, 20, len(data2.Rows))
	assert.EqualValues(t, 9, len(data2.Columns))

}

func TestFileSysDOSpaces(t *testing.T) {
	fs, err := NewFileSysClient(
		dbio.TypeFileS3,
		// "ENDPOINT=https://nyc3.digitaloceanspaces.com",
		"ENDPOINT=nyc3.digitaloceanspaces.com",
		"ACCESS_KEY_ID="+os.Getenv("DOS_ACCESS_KEY_ID"),
		"SECRET_ACCESS_KEY="+os.Getenv("DOS_SECRET_ACCESS_KEY"),
		"METADATA="+g.Marshal(iop.Metadata{LoadedAt: iop.KeyValue{"loaded_at", time.Now().Unix()}, StreamURL: iop.KeyValue{"url", ""}}),
	)
	assert.NoError(t, err)

	// Test List
	paths, err := fs.List("s3://ocral/")
	assert.NoError(t, err)

	// Test Delete, Write, Read
	testPath := "s3://ocral/test/fs.test"
	testString := "abcde"
	err = Delete(fs, testPath)
	assert.NoError(t, err)

	reader := strings.NewReader(testString)
	bw, err := fs.Write(testPath, reader)
	assert.EqualValues(t, 5, bw)

	// writer, err := fs.GetWriter(testPath)
	// bw, err := Write(reader, writer)
	assert.NoError(t, err)

	reader2, err := fs.GetReader(testPath)
	if !assert.NoError(t, err) {
		return
	}

	testBytes, err := ioutil.ReadAll(reader2)
	assert.NoError(t, err)

	assert.Equal(t, testString, string(testBytes))
	err = Delete(fs, testPath)
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

	localFs.SetProp("datetime_format", "02-01-2006 15:04:05.000")
	df2, err := localFs.ReadDataflow("test/test1/csv")
	assert.NoError(t, err)
	// assert.EqualValues(t, 3, len(df2.Streams))

	writeFolderPath := "s3://ocral/test.fs.write"
	_, err = fs.WriteDataflow(df2, writeFolderPath)
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, df2.Count())

	localFs.SetProp("datetime_format", "02-01-2006 15:04:05.000")
	df2, err = localFs.ReadDataflow("test/test1/csv")
	assert.NoError(t, err)
	writeFolderPath = "s3://ocral/test.fs.write.json"
	_, err = fs.WriteDataflow(df2, writeFolderPath)
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, df2.Count())

	// eventual consistency
	time.Sleep(2 * time.Second) // wait to s3 files to write on AWS side
	df3, err := fs.ReadDataflow(writeFolderPath)
	assert.NoError(t, err)

	data2, err := df3.Collect()
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, len(data2.Rows))
	assert.Contains(t, data2.Columns.Names(), "loaded_at")
	assert.Contains(t, data2.Columns.Names(), "url")
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

	df, err := fs.ReadDataflow(path, FileStreamConfig{Limit: 10000})
	assert.NoError(t, err)

	for ds := range df.StreamCh {
		_, err := ds.Collect(0)
		assert.NoError(t, err)
	}
	assert.NoError(t, df.Err())
}

func TestFileSysS3(t *testing.T) {
	t.Parallel()
	fs, err := NewFileSysClient(dbio.TypeFileS3)
	// fs, err := NewFileSysClient(S3cFileSys, "ENDPOINT=s3.amazonaws.com")
	assert.NoError(t, err)

	buckets, err := fs.Buckets()
	assert.NoError(t, err)
	assert.NotEmpty(t, buckets)

	// Test Delete, Write, Read
	testPath := "s3://ocral-data-1/test/fs.test"
	testString := "abcde"
	err = Delete(fs, testPath)
	assert.NoError(t, err)

	reader := strings.NewReader(testString)
	bw, err := fs.Write(testPath, reader)
	assert.EqualValues(t, 5, bw)
	assert.NoError(t, err)

	paths, err := fs.List("s3://ocral-data-1/")
	assert.NoError(t, err)
	assert.Contains(t, paths, "s3://ocral-data-1/test/")

	paths, err = fs.ListRecursive("s3://ocral-data-1/")
	assert.NoError(t, err)
	assert.Contains(t, paths, testPath)

	reader2, err := fs.GetReader(testPath)
	if !assert.NoError(t, err) {
		return
	}

	testBytes, err := ioutil.ReadAll(reader2)
	assert.NoError(t, err)

	assert.Equal(t, testString, string(testBytes))
	err = Delete(fs, testPath)
	assert.NoError(t, err)

	paths, err = fs.ListRecursive("s3://ocral-data-1/")
	assert.NoError(t, err)
	assert.NotContains(t, paths, testPath)

	// Test concurrent wrinting from datastream

	localFs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	localFs.SetProp("datetime_format", "02-01-2006 15:04:05.000")
	df2, err := localFs.ReadDataflow("test/test1/csv")
	assert.NoError(t, err)

	writeFolderPath := "s3://ocral-data-1/test.fs.write"
	err = Delete(fs, writeFolderPath)
	assert.NoError(t, err)

	fs.SetProp("FILE_MAX_BYTES", "20000")
	_, err = fs.WriteDataflow(df2, writeFolderPath)
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, df2.Count())
	assert.EqualValues(t, 3, len(df2.Streams))

	// eventual consistency
	time.Sleep(2 * time.Second) // wait to s3 files to write on AWS side
	df3, err := fs.ReadDataflow(writeFolderPath, FileStreamConfig{Limit: 1})
	assert.NoError(t, err)

	data2, err := iop.MergeDataflow(df3).Collect(int(df3.Limit))
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(data2.Rows))
}

func TestFileSysAzure(t *testing.T) {
	t.Parallel()

	fs, err := NewFileSysClient(dbio.TypeFileAzure)
	if !assert.NoError(t, err) {
		return
	}

	buckets, err := fs.Buckets()
	if !assert.NoError(t, err) {
		return
	}
	assert.NotEmpty(t, buckets)

	testString := "abcde"
	testPath := "https://flarcostorage.blob.core.windows.net/testcont/test1"
	reader := strings.NewReader(testString)
	bw, err := fs.Write(testPath, reader)
	assert.EqualValues(t, 5, bw)
	assert.NoError(t, err)

	reader2, err := fs.GetReader(testPath)
	if !assert.NoError(t, err) {
		return
	}

	testBytes, err := ioutil.ReadAll(reader2)
	assert.NoError(t, err)

	assert.Equal(t, testString, string(testBytes))
	err = Delete(fs, testPath)
	assert.NoError(t, err)

	paths, err := fs.ListRecursive("https://flarcostorage.blob.core.windows.net")
	assert.NoError(t, err)
	assert.NotContains(t, paths, testPath)

	localFs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	localFs.SetProp("datetime_format", "02-01-2006 15:04:05.000")
	df2, err := localFs.ReadDataflow("test/test1/csv")
	assert.NoError(t, err)
	// assert.EqualValues(t, 3, len(df2.Streams))

	writeFolderPath := "https://flarcostorage.blob.core.windows.net/testcont/test2"
	_, err = fs.WriteDataflow(df2, writeFolderPath)
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, df2.Count())

	df3, err := fs.ReadDataflow(writeFolderPath)
	if assert.NoError(t, err) {
		data2, err := df3.Collect()
		assert.NoError(t, err)
		assert.EqualValues(t, 1036, len(data2.Rows))
	}

	df3, err = fs.ReadDataflow("https://flarcostorage.blob.core.windows.net/testcont/test2/part.01.0001.csv")
	if assert.NoError(t, err) {
		data2, err := df3.Collect()
		assert.NoError(t, err)
		assert.Greater(t, len(data2.Rows), 0)
	}
	// Delete(fs, writeFolderPath)
}

func TestFileSysGoogle(t *testing.T) {
	t.Parallel()

	fs, err := NewFileSysClient(dbio.TypeFileGoogle, "BUCKET=flarco_us_bucket")
	if !assert.NoError(t, err) {
		return
	}

	buckets, err := fs.Buckets()
	assert.NoError(t, err)
	assert.NotEmpty(t, buckets)

	testString := "abcde"
	testPath := "gs://flarco_us_bucket/test/test1"
	reader := strings.NewReader(testString)
	bw, err := fs.Write(testPath, reader)
	assert.EqualValues(t, 5, bw)
	assert.NoError(t, err)

	reader2, err := fs.GetReader(testPath)
	if !assert.NoError(t, err) {
		return
	}

	testBytes, err := ioutil.ReadAll(reader2)
	assert.NoError(t, err)

	assert.Equal(t, testString, string(testBytes))

	err = Delete(fs, testPath)
	assert.NoError(t, err)

	paths, err := fs.ListRecursive("gs://flarco_us_bucket/test")
	assert.NoError(t, err)
	assert.NotContains(t, paths, testPath)

	localFs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	localFs.SetProp("datetime_format", "02-01-2006 15:04:05.000")
	df2, err := localFs.ReadDataflow("test/test1/csv")
	assert.NoError(t, err)
	// assert.EqualValues(t, 3, len(df2.Streams))

	writeFolderPath := "gs://flarco_us_bucket/test"
	_, err = fs.WriteDataflow(df2, writeFolderPath)
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, df2.Count())

	df3, err := fs.ReadDataflow(writeFolderPath)
	assert.NoError(t, err)

	data2, err := df3.Collect()
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, len(data2.Rows))
}

func TestFileSysSftp(t *testing.T) {
	t.Parallel()

	fs, err := NewFileSysClient(
		dbio.TypeFileSftp,
		// "SSH_PRIVATE_KEY=/root/.ssh/id_rsa",
		"URL="+os.Getenv("SSH_TEST_PASSWD_URL"),
	)
	assert.NoError(t, err)
	if t.Failed() {
		return
	}

	root := os.Getenv("SSH_TEST_PASSWD_URL")
	rootU, err := net.NewURL(root)
	assert.NoError(t, err)
	root = "sftp://" + rootU.Hostname()

	testString := "abcde"
	testPath := root + "/tmp/test/test1"
	reader := strings.NewReader(testString)
	bw, err := fs.Write(testPath, reader)
	assert.NoError(t, err)
	assert.EqualValues(t, 5, bw)

	reader2, err := fs.GetReader(testPath)
	if !assert.NoError(t, err) {
		return
	}

	testBytes, err := ioutil.ReadAll(reader2)
	assert.NoError(t, err)

	assert.Equal(t, testString, string(testBytes))

	err = Delete(fs, testPath)
	assert.NoError(t, err)

	paths, err := fs.ListRecursive(root + "/tmp/test")
	assert.NoError(t, err)
	assert.NotContains(t, paths, testPath)

	localFs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	localFs.SetProp("datetime_format", "02-01-2006 15:04:05.000")
	df2, err := localFs.ReadDataflow("test/test1/csv")
	assert.NoError(t, err)
	// assert.EqualValues(t, 3, len(df2.Streams))

	writeFolderPath := root + "/tmp/test"
	_, err = fs.WriteDataflow(df2, writeFolderPath)
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, df2.Count())

	df3, err := fs.ReadDataflow(writeFolderPath)
	assert.NoError(t, err)

	data2, err := df3.Collect()
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, len(data2.Rows))

	err = Delete(fs, writeFolderPath)
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
		data, err := ds.Collect(0)
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
		data, err := ds.Collect(0)
		g.Debug("%d rows collected from %s", len(data.Rows), csvPaths[i])
		if assert.NoError(t, err, "for "+csvPaths[i]) {
			assert.Greater(t, len(data.Rows), 0)
		}
	}
}
