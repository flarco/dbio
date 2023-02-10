package filesys

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/flarco/dbio"
	"github.com/flarco/g/net"
	"github.com/samber/lo"

	"github.com/flarco/dbio/iop"

	"github.com/dustin/go-humanize"
	"github.com/flarco/dbio/env"

	"github.com/flarco/g"
	"github.com/spf13/cast"
)

// FileSysClient is a client to a file systems
// such as local, s3, hdfs, azure storage, google cloud storage
type FileSysClient interface {
	Self() FileSysClient
	Init(ctx context.Context) (err error)
	Client() *BaseFileSysClient
	Context() (context *g.Context)
	FsType() dbio.Type
	GetReader(path string) (reader io.Reader, err error)
	GetReaders(paths ...string) (readers []io.Reader, err error)
	GetDatastream(path string) (ds *iop.Datastream, err error)
	GetWriter(path string) (writer io.Writer, err error)
	Buckets() (paths []string, err error)
	List(path string) (paths []string, err error)
	ListRecursive(path string) (paths []string, err error)
	Write(path string, reader io.Reader) (bw int64, err error)
	delete(path string) (err error)
	setDf(df *iop.Dataflow)

	ReadDataflow(url string, cfg ...FileStreamConfig) (df *iop.Dataflow, err error)
	WriteDataflow(df *iop.Dataflow, url string) (bw int64, err error)
	WriteDataflowReady(df *iop.Dataflow, url string, fileReadyChn chan FileReady) (bw int64, err error)
	GetProp(key string) (val string)
	SetProp(key string, val string)
	MkdirAll(path string) (err error)
}

// NewFileSysClient create a file system client
// such as local, s3, azure storage, google cloud storage
// props are provided as `"Prop1=Value1", "Prop2=Value2", ...`
func NewFileSysClient(fst dbio.Type, props ...string) (fsClient FileSysClient, err error) {
	return NewFileSysClientContext(context.Background(), fst, props...)
}

// NewFileSysClientContext create a file system client with context
// such as local, s3, azure storage, google cloud storage
// props are provided as `"Prop1=Value1", "Prop2=Value2", ...`
func NewFileSysClientContext(ctx context.Context, fst dbio.Type, props ...string) (fsClient FileSysClient, err error) {
	concurencyLimit := runtime.NumCPU()
	if os.Getenv("CONCURENCY_LIMIT") != "" {
		concurencyLimit = cast.ToInt(os.Getenv("CONCURENCY_LIMIT"))
	}

	switch fst {
	case dbio.TypeFileLocal:
		fsClient = &LocalFileSysClient{}
		concurencyLimit = 20
	case dbio.TypeFileS3:
		fsClient = &S3FileSysClient{}
		//fsClient = &S3cFileSysClient{}
		//fsClient.Client().fsType = S3cFileSys
	case dbio.TypeFileSftp:
		fsClient = &SftpFileSysClient{}
	// case HDFSFileSys:
	// 	fsClient = fsClient
	case dbio.TypeFileAzure:
		fsClient = &AzureFileSysClient{}
	case dbio.TypeFileGoogle:
		fsClient = &GoogleFileSysClient{}
	case dbio.TypeFileHTTP:
		fsClient = &HTTPFileSysClient{}
	default:
		err = g.Error("Unrecognized File System")
		return
	}

	fsClient.Client().fsType = fst
	fsClient.Client().context = g.NewContext(ctx)

	// set default properties
	fsClient.SetProp("header", "true")
	fsClient.SetProp("delimiter", ",")

	// set properties
	for k, v := range g.KVArrToMap(props...) {
		fsClient.SetProp(k, v)
	}

	if fsClient.GetProp("CONCURENCY_LIMIT") != "" {
		concurencyLimit = cast.ToInt(fsClient.GetProp("CONCURENCY_LIMIT"))
	}

	for k, v := range env.Vars() {
		if fsClient.GetProp(k) == "" {
			fsClient.SetProp(k, v)
		}
	}

	// Init Limit
	err = fsClient.Init(ctx)
	if err != nil {
		err = g.Error(err, "Error initiating File Sys Client")
	}
	fsClient.Context().SetConcurencyLimit(concurencyLimit)

	return
}

// NewFileSysClientFromURL returns the proper fs client for the given path
// props are provided as `"Prop1=Value1", "Prop2=Value2", ...`
func NewFileSysClientFromURL(url string, props ...string) (fsClient FileSysClient, err error) {
	return NewFileSysClientFromURLContext(context.Background(), url, props...)
}

// NewFileSysClientFromURLContext returns the proper fs client for the given path with context
// props are provided as `"Prop1=Value1", "Prop2=Value2", ...`
func NewFileSysClientFromURLContext(ctx context.Context, url string, props ...string) (fsClient FileSysClient, err error) {
	switch {
	case strings.HasPrefix(url, "s3://"):
		if v, ok := g.KVArrToMap(props...)["ENDPOINT"]; ok && v != "" {
			return NewFileSysClientContext(ctx, dbio.TypeFileS3, props...)
		}
		return NewFileSysClientContext(ctx, dbio.TypeFileS3, props...)
	case strings.HasPrefix(url, "sftp://"):
		props = append(props, "URL="+url)
		return NewFileSysClientContext(ctx, dbio.TypeFileSftp, props...)
	case strings.HasPrefix(url, "gs://"):
		return NewFileSysClientContext(ctx, dbio.TypeFileGoogle, props...)
	case strings.Contains(url, ".core.windows.net") || strings.HasPrefix(url, "azure://"):
		return NewFileSysClientContext(ctx, dbio.TypeFileAzure, props...)
	case strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://"):
		return NewFileSysClientContext(ctx, dbio.TypeFileHTTP, props...)
	case strings.HasPrefix(url, "file://"):
		props = append(props, g.F("concurencyLimit=%d", 20))
		return NewFileSysClientContext(ctx, dbio.TypeFileLocal, props...)
	case strings.Contains(url, "://"):
		err = g.Error(
			g.Error("Unable to determine FileSysClient for "+url),
			"",
		)
		return
	default:
		props = append(props, g.F("concurencyLimit=%d", 20))
		return NewFileSysClientContext(ctx, dbio.TypeFileLocal, props...)
	}
}

// PathNode represents a file node
type PathNode struct {
	Name         string    `json:"name"`
	IsDir        bool      `json:"is_dir"`
	Size         int64     `json:"size,omitempty"`
	LastModified time.Time `json:"last_modified,omitempty"`
	Children     PathNodes `json:"children,omitempty"`
}

// PathNodes represent file nodes
type PathNodes []PathNode

// Add adds a new node to list
func (pn *PathNodes) Add(p PathNode) {
	nodes := *pn
	nodes = append(nodes, p)
	pn = &nodes
}

// List give a list of recursive paths
func (pn PathNodes) List() (paths []string) {
	for _, p := range pn {
		paths = append(paths, p.Name)
		if p.IsDir {
			paths = append(paths, p.Children.List()...)
		}
	}
	return paths
}

type FileType string

const FileTypeCsv FileType = "csv"
const FileTypeXml FileType = "xml"
const FileTypeJson FileType = "json"

// PeekFileType peeks into the file to try determine the file type
// CSV is the default
func PeekFileType(reader io.Reader) (ft FileType, reader2 io.Reader, err error) {

	data, reader2, err := g.Peek(reader, 0)
	if err != nil {
		err = g.Error(err, "could not peek file")
		return
	}

	peekStr := strings.TrimSpace(string(data))
	if strings.HasPrefix(peekStr, "[") || strings.HasPrefix(peekStr, "{") {
		ft = FileTypeJson
	} else if strings.HasPrefix(peekStr, "<") {
		ft = FileTypeXml
	} else {
		ft = FileTypeCsv
	}

	return
}

// ParseURL parses a URL
func ParseURL(urlStr string) (host string, path string, err error) {

	u, err := url.Parse(urlStr)
	if err != nil {
		err = g.Error(err, "Unable to parse URL "+urlStr)
		return
	}

	scheme := u.Scheme
	host = u.Hostname()
	path = u.Path

	if scheme == "" || host == "" {
		err = g.Error("Invalid URL: " + urlStr)
	}

	return
}

func getExcelStream(fs FileSysClient, reader io.Reader) (ds *iop.Datastream, err error) {
	xls, err := NewExcelFromReader(reader)
	if err != nil {
		err = g.Error(err, "Unable to open Excel File from reader")
		return nil, err
	}
	xls.Props = fs.Client().properties

	sheetName := fs.GetProp("sheet")
	if sheetName == "" {
		sheetName = xls.Sheets[0]
	}

	sheetRange := fs.GetProp("range")
	if sheetRange != "" {
		data, err := xls.GetDatasetFromRange(sheetName, sheetRange)
		if err != nil {
			err = g.Error(err, "Unable to get range data for %s!%s", sheetName, sheetRange)
			return nil, err
		}
		ds = data.Stream()
	} else {
		data := xls.GetDataset(sheetName)
		ds = data.Stream()
	}
	return ds, nil
}

////////////////////// BASE

// BaseFileSysClient is the base file system type.
type BaseFileSysClient struct {
	FileSysClient
	properties map[string]string
	instance   *FileSysClient
	context    g.Context
	fsType     dbio.Type
	df         *iop.Dataflow
}

// Context provides a pointer to context
func (fs *BaseFileSysClient) Context() (context *g.Context) {
	return &fs.context
}

// Client provides a pointer to itself
func (fs *BaseFileSysClient) Client() *BaseFileSysClient {
	return fs
}

// setDf sets the dataflow
func (fs *BaseFileSysClient) setDf(df *iop.Dataflow) {
	fs.df = df
}

// Instance returns the respective connection Instance
// This is useful to refer back to a subclass method
// from the superclass level. (Aka overloading)
func (fs *BaseFileSysClient) Self() FileSysClient {
	return *fs.instance
}

// FsType return the type of the client
func (fs *BaseFileSysClient) FsType() dbio.Type {
	return fs.fsType
}

// Buckets returns the buckets found in the account
func (fs *BaseFileSysClient) Buckets() (paths []string, err error) {
	return
}

// GetProp returns the value of a property
func (fs *BaseFileSysClient) GetProp(key string) string {
	fs.context.Mux.Lock()
	val := fs.properties[strings.ToLower(key)]
	fs.context.Mux.Unlock()
	return val
}

// SetProp sets the value of a property
func (fs *BaseFileSysClient) SetProp(key string, val string) {
	fs.context.Mux.Lock()
	if fs.properties == nil {
		fs.properties = map[string]string{}
	}
	fs.properties[strings.ToLower(key)] = val
	fs.context.Mux.Unlock()
}

// Props returns a copy of the properties map
func (fs *BaseFileSysClient) Props() map[string]string {
	m := map[string]string{}
	fs.context.Mux.Lock()
	for k, v := range fs.properties {
		m[k] = v
	}
	fs.context.Mux.Unlock()
	return m
}

func (fs *BaseFileSysClient) GetRefTs() time.Time {
	var ts time.Time
	if val := cast.ToInt64(fs.GetProp("SLING_FS_TIMESTAMP")); val != 0 {
		ts = time.Unix(val, 0)
		if gte := os.Getenv("SLING_GREATER_THAN_EQUAL"); gte != "" && cast.ToBool(gte) {
			ts = time.Unix(val, 0).Add(-1 * time.Millisecond)
		}
	}
	return ts
}

// GetDatastream return a datastream for the given path
func (fs *BaseFileSysClient) GetDatastream(urlStr string) (ds *iop.Datastream, err error) {

	ds = iop.NewDatastreamContext(fs.Context().Ctx, nil)
	ds.SafeInference = true
	ds.SetMetadata(fs.GetProp("METADATA"))
	ds.Metadata.StreamURL.Value = urlStr
	ds.SetConfig(fs.Props())
	g.Debug("%s, reading datastream from %s", ds.ID, urlStr)

	if strings.Contains(strings.ToLower(urlStr), ".xlsx") {
		reader, err := fs.Self().GetReader(urlStr)
		if err != nil {
			err = g.Error(err, "Error getting Excel reader")
			return ds, err
		}

		// Wait for reader to start reading or err
		for {
			// Try peeking
			if b := bufio.NewReader(reader).Size(); b > 0 {
				// g.P(b)
				break
			}

			if fs.Context().Err() != nil {
				// has errorred
				return ds, fs.Context().Err()
			}
			time.Sleep(50 * time.Millisecond)
		}

		eDs, err := getExcelStream(fs.Self(), reader)
		if err != nil {
			err = g.Error(err, "Error consuming Excel reader")
			return ds, err
		}
		return eDs, nil
	}

	// CSV, JSON or XML files
	go func() {
		// manage concurrency
		defer fs.Context().Wg.Read.Done()
		fs.Context().Wg.Read.Add()

		reader, err := fs.Self().GetReader(urlStr)
		if err != nil {
			fs.Context().CaptureErr(g.Error(err, "Error getting reader"))
			g.LogError(fs.Context().Err())
			fs.Context().Cancel()
			return
		}

		// Wait for reader to start reading or err
		for {
			// Try peeking
			if b := bufio.NewReader(reader).Size(); b > 0 {
				break
			}

			if fs.Context().Err() != nil {
				// has errorred
				return
			}
			time.Sleep(50 * time.Millisecond)
		}

		if strings.Contains(urlStr, ".json") {
			err = ds.ConsumeJsonReader(reader)
		} else if strings.HasSuffix(urlStr, ".xml") {
			err = ds.ConsumeXmlReader(reader)
		} else {
			err = ds.ConsumeCsvReader(reader)
		}
		if err != nil {
			ds.Context.CaptureErr(g.Error(err, "Error consuming reader for %s", urlStr))
			ds.Context.Cancel()
			fs.Context().CaptureErr(g.Error(err, "Error consuming reader"))
			// fs.Context().Cancel()
			// g.LogError(fs.Context().Err())
		}

	}()

	return ds, err
}

// ReadDataflow read
func (fs *BaseFileSysClient) ReadDataflow(url string, cfg ...FileStreamConfig) (df *iop.Dataflow, err error) {
	Cfg := FileStreamConfig{} // infinite
	if len(cfg) > 0 {
		Cfg = cfg[0]
	}

	if strings.HasSuffix(strings.ToLower(url), ".zip") {
		localFs, err := NewFileSysClient(dbio.TypeFileLocal)
		if err != nil {
			return df, g.Error(err, "could not initialize localFs")
		}

		reader, err := fs.Self().GetReader(url)
		if err != nil {
			return df, g.Error(err, "could not get zip reader")
		}

		folderPath := path.Join(os.TempDir(), "dbio_temp_")

		zipPath := folderPath + ".zip"
		_, err = localFs.Write(zipPath, reader)
		if err != nil {
			return df, g.Error(err, "could not write to "+zipPath)
		}

		paths, err := iop.Unzip(zipPath, folderPath)
		if err != nil {
			return df, g.Error(err, "Error unzipping")
		}
		// delete zip file
		Delete(localFs, zipPath)

		// TODO: handle multiple files, yielding multiple schemas
		df, err = GetDataflow(localFs.Self(), paths, Cfg)
		if err != nil {
			return df, g.Error(err, "Error making dataflow")
		}

		// delete unzipped folder when done
		df.Defer(func() { Delete(localFs, folderPath) })

		return df, nil
	}

	paths, err := fs.Self().ListRecursive(url)
	if err != nil {
		err = g.Error(err, "Error getting paths")
		return
	}
	df, err = GetDataflow(fs.Self(), paths, Cfg)
	if err != nil {
		err = g.Error(err, "Error getting dataflow")
		return
	}

	df.FsURL = url
	return
}

// WriteDataflow writes a dataflow to a file sys.
func (fs *BaseFileSysClient) WriteDataflow(df *iop.Dataflow, url string) (bw int64, err error) {
	// handle excel file here, generate reader
	if strings.Contains(strings.ToLower(url), ".xlsx") {
		xls := NewExcel()

		sheetName := fs.GetProp("sheet")
		if sheetName == "" {
			sheetName = "Sheet1"
		}

		err = xls.WriteSheet(sheetName, iop.MergeDataflow(df), "overwrite")
		if err != nil {
			err = g.Error(err, "error writing to excel file")
			return
		}

		pr, pw := io.Pipe()
		go func() {
			defer pw.Close()
			err = xls.WriteToWriter(pw)
			if err != nil {
				g.LogError(err, "error writing to excel file")
			}
		}()

		bw, err = fs.Self().Write(url, pr)
		return
	}

	fileReadyChn := make(chan FileReady, 10000)

	g.Trace("writing dataflow to %s", url)
	go func() {
		for range fileReadyChn {
			// do nothing, wait for completion
		}
	}()

	return fs.Self().WriteDataflowReady(df, url, fileReadyChn)
}

// GetReaders returns one or more readers from specified paths in specified FileSysClient
func (fs *BaseFileSysClient) GetReaders(paths ...string) (readers []io.Reader, err error) {
	if len(paths) == 0 {
		err = g.Error("Provided 0 files for: %#v", paths)
		return
	}

	for _, path := range paths {
		reader, err := fs.Self().GetReader(path)
		if err != nil {
			return nil, g.Error(err, "Unable to process "+path)
		}
		readers = append(readers, reader)
	}

	return readers, nil
}

type FileReady struct {
	Columns iop.Columns
	URI     string
	BytesW  int64
	BatchID string
}

// WriteDataflowReady writes to a file sys and notifies the fileReady chan.
func (fs *BaseFileSysClient) WriteDataflowReady(df *iop.Dataflow, url string, fileReadyChn chan FileReady) (bw int64, err error) {
	fsClient := fs.Self()
	defer close(fileReadyChn)
	useBufferedStream := cast.ToBool(fs.GetProp("USE_BUFFERED_STREAM"))
	concurrency := cast.ToInt(fs.GetProp("CONCURRENCY"))
	compression := iop.CompressorType(strings.ToUpper(fs.GetProp("COMPRESSION")))
	fileExt := cast.ToString(fs.GetProp("FILE_EXT"))
	fileRowLimit := cast.ToInt(fs.GetProp("FILE_MAX_ROWS"))
	fileBytesLimit := cast.ToInt64(fs.GetProp("FILE_BYTES_LIMIT")) // uncompressed file size
	if g.In(compression, iop.GzipCompressorType, iop.ZStandardCompressorType, iop.SnappyCompressorType) {
		fileBytesLimit = fileBytesLimit * 6 // compressed, multiply
	}
	if concurrency == 0 || concurrency > runtime.NumCPU() {
		concurrency = runtime.NumCPU()
	}
	if concurrency > 7 {
		concurrency = 7
	}

	if fileExt == "" {
		switch {
		case strings.Contains(strings.ToLower(url), ".jsonl"):
			fileExt = "jsonlines"
		case strings.HasSuffix(strings.ToLower(url), ".json"):
			fileExt = "json"
		default:
			fileExt = "csv"
		}
	}

	url = strings.TrimSuffix(url, "/")

	singleFile := fileRowLimit == 0 && fileBytesLimit == 0 && len(df.Streams) == 1

	processStream := func(ds *iop.Datastream, partURL string) {
		defer df.Context.Wg.Read.Done()
		localCtx := g.NewContext(ds.Context.Ctx, concurrency)

		writePart := func(reader io.Reader, batchR *iop.BatchReader, partURL string) {
			defer localCtx.Wg.Read.Done()

			bw0, err := fsClient.Write(partURL, reader)
			if batchR.Counter != 0 {
				bID := lo.Ternary(batchR.Batch != nil, batchR.Batch.ID(), "")
				fileReadyChn <- FileReady{batchR.Columns, partURL, bw0, bID}
			}
			if err != nil {
				df.Context.CaptureErr(g.Error(err))
				ds.Context.Cancel()
				df.Context.Cancel()
			}
			g.Trace("wrote %s [%d rows] to %s", humanize.Bytes(cast.ToUint64(bw0)), batchR.Counter, partURL)
			bw += bw0
			df.AddOutBytes(uint64(bw0))
		}

		// pre-add to WG to not hold next reader in memory while waiting
		localCtx.Wg.Read.Add()
		fileCount := 0

		processReader := func(batchR *iop.BatchReader) error {
			fileCount++
			subPartURL := fmt.Sprintf("%s.%04d.%s", partURL, fileCount, fileExt)
			if singleFile {
				subPartURL = partURL
				for _, comp := range []iop.CompressorType{
					iop.GzipCompressorType,
					iop.SnappyCompressorType,
					iop.ZStandardCompressorType,
				} {
					compressor := iop.NewCompressor(comp)
					if strings.HasSuffix(subPartURL, compressor.Suffix()) {
						compression = comp
						subPartURL = strings.TrimSuffix(subPartURL, compressor.Suffix())
						break
					}
				}
			}

			compressor := iop.NewCompressor(compression)
			subPartURL = subPartURL + compressor.Suffix()
			g.Trace("writing stream to " + subPartURL)
			go writePart(compressor.Compress(batchR.Reader), batchR, subPartURL)
			localCtx.Wg.Read.Add()
			// localCtx.MemBasedLimit(98) // wait until memory is lower than 90%

			return df.Err()
		}

		switch fileExt {
		case "json":
			for reader := range ds.NewJsonReaderChnl(fileRowLimit, fileBytesLimit) {
				err := processReader(&iop.BatchReader{Columns: ds.Columns, Reader: reader, Counter: -1})
				if err != nil {
					break
				}
			}
		case "jsonlines":
			for reader := range ds.NewJsonLinesReaderChnl(fileRowLimit, fileBytesLimit) {
				err := processReader(&iop.BatchReader{Columns: ds.Columns, Reader: reader, Counter: -1})
				if err != nil {
					break
				}
			}
		case "csv":
			if useBufferedStream {
				// faster, but dangerous. Holds data in memory
				for reader := range ds.NewCsvBufferReaderChnl(fileRowLimit, fileBytesLimit) {
					err := processReader(&iop.BatchReader{Columns: ds.Columns, Reader: reader, Counter: -1})
					if err != nil {
						break
					}
				}
			} else {
				// slower! but safer, waits for compression but does not hold data in memory
				for batchR := range ds.NewCsvReaderChnl(fileRowLimit, fileBytesLimit) {
					err := processReader(batchR)
					if err != nil {
						break
					}
				}
			}
		}

		ds.Buffer = nil // clear buffer
		if ds.Err() != nil {
			df.Context.CaptureErr(g.Error(ds.Err()))
		}
		localCtx.Wg.Read.Done() // clear that pre-added WG
		localCtx.Wg.Read.Wait()

		df.AddInBytes(ds.Bytes) // add in bytes
	}

	g.Debug("writing to %s", url)
	err = Delete(fsClient, url)
	if err != nil {
		err = g.Error(err, "Could not delete url")
		return
	}

	if !singleFile && (fsClient.FsType() == dbio.TypeFileLocal || fsClient.FsType() == dbio.TypeFileSftp) {
		err = fsClient.MkdirAll(url)
		if err != nil {
			err = g.Error(err, "could not create directory")
			return
		}
	}

	partCnt := 1
	// for ds := range df.MakeStreamCh() {
	for _, ds := range df.Streams {

		partURL := fmt.Sprintf("%s/part.%02d", url, partCnt)
		if singleFile {
			partURL = url
		}
		if fsClient.FsType() == dbio.TypeFileAzure {
			partURL = fmt.Sprintf("%s/part.%02d", url, partCnt)
		}
		g.Trace("writing to %s [fileRowLimit=%d fileBytesLimit=%d compression=%s concurrency=%d useBufferedStream=%v]", partURL, fileRowLimit, fileBytesLimit, compression, concurrency, useBufferedStream)

		df.Context.Wg.Read.Add()
		ds.SetConfig(fs.Props()) // pass options
		go processStream(ds, partURL)
		partCnt++
	}

	df.Context.Wg.Read.Wait()
	if df.Err() != nil {
		err = g.Error(df.Err())
	}

	return
}

// Delete deletes the provided path
// with some safeguards so to not accidentally delete some root path
func Delete(fs FileSysClient, path string) (err error) {

	u, err := net.NewURL(path)
	if err != nil {
		return g.Error(err, "could not parse url for deletion")
	}

	// add some safefguards
	p := strings.TrimPrefix(strings.TrimSuffix(u.Path(), "/"), "/")
	pArr := strings.Split(p, "/")

	switch fs.FsType() {
	case dbio.TypeFileS3, dbio.TypeFileGoogle:
		if len(p) == 0 {
			return g.Error("will not delete bucket level %s", path)
		}
	case dbio.TypeFileAzure:
		if len(p) == 0 {
			return g.Error("will not delete account level %s", path)
		}
		// container level
		if len(pArr) <= 1 {
			return g.Error("will not delete container level %s", path)
		}
	case dbio.TypeFileLocal:
		if len(u.Hostname()) == 0 && len(p) == 0 {
			return g.Error("will not delete root level %s", path)
		}
	case dbio.TypeFileSftp:
		if len(p) == 0 {
			return g.Error("will not delete root level %s", path)
		}
	}

	err = fs.delete(path)
	if err != nil {
		return g.Error(err, "could not delete path")
	}
	return nil
}

type FileStreamConfig struct {
	Limit   int
	Columns []string
}

// GetDataflow returns a dataflow from specified paths in specified FileSysClient
func GetDataflow(fs FileSysClient, paths []string, cfg FileStreamConfig) (df *iop.Dataflow, err error) {

	if len(paths) == 0 {
		err = g.Error("Provided 0 files for: %#v", paths)
		return
	}

	ctx := g.NewContext(fs.Context().Ctx)
	df = iop.NewDataflow(cfg.Limit)
	df.Context = &ctx
	dsCh := make(chan *iop.Datastream)
	fs.setDf(df)

	go func() {
		defer close(dsCh)
		for _, path := range paths {
			if strings.HasSuffix(path, "/") {
				g.Debug("skipping %s because is not file", path)
				continue
			}

			ds, err := fs.GetDatastream(path)
			if err != nil {
				fs.Context().CaptureErr(g.Error(err, "Unable to process "+path))
				return
			}
			if len(cfg.Columns) > 1 {
				cols := iop.NewColumnsFromFields(cfg.Columns...)
				fm := ds.Columns.FieldMap(true)
				transf := func(in []interface{}) (out []interface{}) {
					for _, col := range cols {
						if i, ok := fm[strings.ToLower(col.Name)]; ok {
							out = append(out, in[i])
						} else {
							ds.Context.CaptureErr(g.Error("column %s not found", col.Name))
						}
					}
					return
				}
				dsCh <- ds.Map(cols, transf)
			} else {
				dsCh <- ds
			}
		}

	}()

	go df.PushStreamChan(dsCh)

	// wait for first ds to start streaming.
	// columns need to be populated
	err = df.WaitReady()
	if err != nil {
		return df, err
	}

	return df, nil
}

// MakeDatastream create a datastream from a reader
func MakeDatastream(reader io.Reader, cfg map[string]string) (ds *iop.Datastream, err error) {

	data, reader2, err := g.Peek(reader, 0)
	if err != nil {
		return nil, err
	}

	peekStr := string(data)
	if strings.HasPrefix(peekStr, "[") || strings.HasPrefix(peekStr, "{") {
		ds = iop.NewDatastream(iop.Columns{})
		ds.SafeInference = true
		ds.SetConfig(cfg)
		err = ds.ConsumeJsonReader(reader2)
		if err != nil {
			return nil, err
		}
	} else {
		csv := iop.CSV{Reader: reader2}
		ds, err = csv.ReadStream()
		if err != nil {
			return nil, err
		}
	}

	return ds, nil
}

// WriteDatastream writes a datasream to a writer
// or use fs.Write(path, ds.NewCsvReader(0))
func WriteDatastream(writer io.Writer, ds *iop.Datastream) (bw int64, err error) {
	reader := ds.NewCsvReader(0, 0)
	return Write(reader, writer)
}

// Write writer to a writer from a reader
func Write(reader io.Reader, writer io.Writer) (bw int64, err error) {
	bw, err = io.Copy(writer, reader)
	if err != nil {
		err = g.Error(err, "Error writing from reader")
	}
	return
}

// TestFsPermissions tests read/write permisions
func TestFsPermissions(fs FileSysClient, pathURL string) (err error) {
	testString := "abcde"

	// Create file/folder
	bw, err := fs.Write(pathURL, strings.NewReader(testString))
	if err != nil {
		return g.Error(err, "failed testing permissions: Create file/folder")
	} else if bw == 0 {
		return g.Error("failed testing permissions: Create file/folder returned 0 bytes")
	}

	// List File
	paths, err := fs.List(pathURL)
	if err != nil {
		return g.Error(err, "failed testing permissions: List File")
	} else if len(paths) == 0 {
		return g.Error("failed testing permissions: List File is zero")
	}

	// Read File
	reader, err := fs.GetReader(pathURL)
	if err != nil {
		return g.Error(err, "failed testing permissions: Read File")
	}

	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return g.Error(err, "failed testing permissions: Read File, reading reader")
	}

	if string(content) != testString {
		return g.Error("failed testing permissions: Read File content mismatch")
	}

	// Delete file/folder
	err = Delete(fs, pathURL)
	if err != nil {
		return g.Error(err, "failed testing permissions: Delete file/folder")
	}

	return
}
