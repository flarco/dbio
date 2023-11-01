package filesys

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/spf13/cast"
)

// S3FileSysClient is a file system client to write file to Amazon's S3 file sys.
type S3FileSysClient struct {
	BaseFileSysClient
	context   g.Context
	bucket    string
	session   *session.Session
	RegionMap map[string]string
	mux       sync.Mutex
}

// Init initializes the fs client
func (fs *S3FileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = g.NewContext(ctx)

	for _, key := range g.ArrStr("BUCKET", "ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "REGION", "SESSION_TOKEN", "ENDPOINT") {
		if fs.GetProp(key) == "" {
			fs.SetProp(key, fs.GetProp("AWS_"+key))
		}
	}

	fs.bucket = fs.GetProp("BUCKET")
	fs.RegionMap = map[string]string{}

	return fs.Connect()
}

const defaultRegion = "us-east-1"

type fakeWriterAt struct {
	w io.Writer
}

func (fw fakeWriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	// ignore 'offset' because we forced sequential downloads
	return fw.w.Write(p)
}

func cleanKey(key string) string {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")
	return key
}

// Connect initiates the Google Cloud Storage client
func (fs *S3FileSysClient) Connect() (err error) {

	if fs.GetProp("ACCESS_KEY_ID") == "" || fs.GetProp("SECRET_ACCESS_KEY") == "" {
		return g.Error("Need to set 'ACCESS_KEY_ID' and 'SECRET_ACCESS_KEY' to interact with S3")
	}

	region := fs.GetProp("REGION")
	endpoint := fs.GetProp("ENDPOINT")
	if region == "" {
		region = defaultRegion
	}

	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/
	fs.session, err = session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(
			fs.GetProp("ACCESS_KEY_ID"),
			fs.GetProp("SECRET_ACCESS_KEY"),
			fs.GetProp("SESSION_TOKEN"),
		),
		Region:                         aws.String(region),
		S3ForcePathStyle:               aws.Bool(true),
		DisableRestProtocolURICleaning: aws.Bool(true),
		Endpoint:                       aws.String(endpoint),
		// LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
	})
	if err != nil {
		err = g.Error(err, "Could not create AWS session")
		return
	}

	return
}

// getSession returns the session and sets the region based on the bucket
func (fs *S3FileSysClient) getSession() (sess *session.Session) {
	fs.mux.Lock()
	defer fs.mux.Unlock()
	endpoint := fs.GetProp("ENDPOINT")
	region := fs.GetProp("REGION")

	if fs.bucket == "" {
		return fs.session
	} else if region != "" {
		fs.RegionMap[fs.bucket] = region
	} else if strings.HasSuffix(endpoint, ".digitaloceanspaces.com") {
		region := strings.TrimSuffix(endpoint, ".digitaloceanspaces.com")
		region = strings.TrimPrefix(region, "https://")
		fs.RegionMap[fs.bucket] = region
	} else if strings.HasSuffix(endpoint, ".cloudflarestorage.com") {
		fs.RegionMap[fs.bucket] = "auto"
	} else if endpoint == "" && fs.RegionMap[fs.bucket] == "" {
		region, err := s3manager.GetBucketRegion(fs.Context().Ctx, fs.session, fs.bucket, defaultRegion)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
				g.Debug("unable to find bucket %s's region not found", fs.bucket)
				g.Debug("Region not found for " + fs.bucket)
			} else {
				g.Debug(g.Error(err, "Error getting Region for "+fs.bucket).Error())
			}
		} else {
			fs.RegionMap[fs.bucket] = region
		}
	}

	fs.session.Config.Region = aws.String(fs.RegionMap[fs.bucket])
	if fs.RegionMap[fs.bucket] == "" {
		fs.session.Config.Region = g.String(defaultRegion)
	}

	return fs.session
}

// Delete deletes the given path (file or directory)
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/file.txt`
func (fs *S3FileSysClient) delete(path string) (err error) {
	bucket, key, err := ParseURL(path)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)

	// Create S3 service client
	svc := s3.New(fs.getSession())

	paths, err := fs.ListRecursive(path)
	if err != nil {
		return
	}

	objects := []*s3.ObjectIdentifier{}
	for _, subPath := range paths {
		_, subPath, err = ParseURL(subPath)
		if err != nil {
			err = g.Error(err, "Error Parsing url: "+path)
			return
		}
		subPath = cleanKey(subPath)
		objects = append(objects, &s3.ObjectIdentifier{Key: aws.String(subPath)})
	}

	if len(objects) == 0 {
		return
	}

	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3.Delete{
			Objects: objects,
			Quiet:   aws.Bool(true),
		},
	}
	_, err = svc.DeleteObjectsWithContext(fs.Context().Ctx, input)

	if err != nil {
		return g.Error(err, fmt.Sprintf("Unable to delete S3 objects:\n%#v", input))
	}

	err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return g.Error(err, "error WaitUntilObjectNotExists")
	}

	return
}

func (fs *S3FileSysClient) getConcurrency() int {
	conc := cast.ToInt(fs.GetProp("CONCURRENCY"))
	if conc == 0 {
		conc = runtime.NumCPU()
	}
	return conc
}

// GetReader return a reader for the given path
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/file.txt` or `s3://my_bucket/key/to/directory`
func (fs *S3FileSysClient) GetReader(path string) (reader io.Reader, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)

	// https://github.com/chanzuckerberg/s3parcp
	PartSize := int64(os.Getpagesize()) * 1024 * 10
	Concurrency := fs.getConcurrency()
	BufferSize := 64 * 1024
	svc := s3.New(fs.getSession())

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(fs.getSession(), func(d *s3manager.Downloader) {
		d.PartSize = PartSize
		d.Concurrency = Concurrency
		d.BufferProvider = s3manager.NewPooledBufferedWriterReadFromProvider(BufferSize)
		d.S3 = svc
	})
	downloader.Concurrency = 1

	pipeR, pipeW := io.Pipe()

	go func() {
		defer pipeW.Close()

		// Write the contents of S3 Object to the file
		_, err := downloader.DownloadWithContext(
			fs.Context().Ctx,
			fakeWriterAt{pipeW},
			&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
		if err != nil {
			fs.Context().CaptureErr(g.Error(err, "Error downloading S3 File -> "+key))
			// g.LogError(fs.Context().Err())
			fs.Context().Cancel()
			return
		}
	}()

	return pipeR, err
}

// GetWriter creates the file if non-existent and return a writer
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/file.txt`
func (fs *S3FileSysClient) GetWriter(path string) (writer io.Writer, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)

	// https://github.com/chanzuckerberg/s3parcp
	PartSize := int64(os.Getpagesize()) * 1024 * 10
	Concurrency := fs.getConcurrency()
	BufferSize := 10485760 // 10MB
	svc := s3.New(fs.getSession())

	uploader := s3manager.NewUploader(fs.getSession(), func(d *s3manager.Uploader) {
		d.PartSize = PartSize
		d.Concurrency = Concurrency
		d.BufferProvider = s3manager.NewBufferedReadSeekerWriteToPool(BufferSize)
		d.S3 = svc
	})
	uploader.Concurrency = fs.Context().Wg.Limit

	pipeR, pipeW := io.Pipe()

	fs.Context().Wg.Write.Add()
	go func() {
		// manage concurrency
		defer fs.Context().Wg.Write.Done()
		defer pipeR.Close()

		// Upload the file to S3.
		_, err := uploader.UploadWithContext(fs.Context().Ctx, &s3manager.UploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   pipeR,
		})
		if err != nil {
			fs.Context().CaptureErr(g.Error(err, "Error uploading S3 File -> "+key))
			g.LogError(fs.Context().Err())
			fs.Context().Cancel()
		}
	}()

	writer = pipeW

	return
}

func (fs *S3FileSysClient) Write(path string, reader io.Reader) (bw int64, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = g.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)

	uploader := s3manager.NewUploader(fs.getSession())
	uploader.Concurrency = fs.Context().Wg.Limit

	// Create pipe to get bytes written
	pr, pw := io.Pipe()
	fs.Context().Wg.Write.Add()
	go func() {
		defer fs.Context().Wg.Write.Done()
		defer pw.Close()
		bw, err = io.Copy(pw, reader)
		if err != nil {
			fs.Context().CaptureErr(g.Error(err, "Error Copying"))
		}
	}()

	// Upload the file to S3.
	_, err = uploader.UploadWithContext(fs.Context().Ctx, &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   pr,
	})
	if err != nil {
		err = g.Error(err, "failed to upload file: "+key)
		return
	}

	err = fs.Context().Err()

	return
}

// Buckets returns the buckets found in the account
func (fs *S3FileSysClient) Buckets() (paths []string, err error) {
	// Create S3 service client
	svc := s3.New(fs.getSession())
	result, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		return nil, g.Error(err, "could not list buckets")
	}

	for _, bucket := range result.Buckets {
		paths = append(paths, g.F("s3://%s", *bucket.Name))
	}
	return
}

// List lists the file in given directory path
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/directory`
func (fs *S3FileSysClient) List(path string) (paths []string, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = g.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)

	urlPrefix := fmt.Sprintf("s3://%s/", bucket)
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(key),
		Delimiter: aws.String("/"),
	}

	// Create S3 service client
	svc := s3.New(fs.getSession())

	paths, err = fs.doList(svc, input, urlPrefix)

	// s3.List return all objects matching the path
	// need to match exactly the parent folder to not
	// return whatever objects partially match the beginning
	for _, p := range paths {
		if !strings.HasSuffix(p, "/") && path == p {
			return []string{p}, err
		}
	}
	prefix := strings.TrimSuffix(path, "/") + "/"
	path2 := []string{}
	for _, p := range paths {
		if strings.HasPrefix(p, prefix) {
			path2 = append(path2, p)
		}
	}

	// if path is folder, need to read inside
	if len(path2) == 1 && strings.HasSuffix(path2[0], "/") {
		return fs.List(path2[0])
	}
	return path2, err
}

// ListRecursive lists the file in given directory path recusively
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/directory`
func (fs *S3FileSysClient) ListRecursive(path string) (paths []string, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = g.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)

	urlPrefix := fmt.Sprintf("s3://%s/", bucket)
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	}

	// Create S3 service client
	svc := s3.New(fs.getSession())

	return fs.doList(svc, input, urlPrefix)
}

func (fs *S3FileSysClient) doList(svc *s3.S3, input *s3.ListObjectsV2Input, urlPrefix string) (paths []string, err error) {

	result, err := svc.ListObjectsV2WithContext(fs.Context().Ctx, input)
	if err != nil {
		err = g.Error(err, "Error with ListObjectsV2 for: %#v", input)
		return paths, err
	}

	ts := fs.GetRefTs()

	prefixes := []string{}
	for {

		for _, cp := range result.CommonPrefixes {
			prefixes = append(prefixes, urlPrefix+*cp.Prefix)
		}

		for _, obj := range result.Contents {
			if obj.LastModified == nil || obj.LastModified.IsZero() || ts.IsZero() || obj.LastModified.After(ts) {
				paths = append(paths, urlPrefix+*obj.Key)
			}
		}

		if *result.IsTruncated {
			input.SetContinuationToken(*result.NextContinuationToken)
			result, err = svc.ListObjectsV2WithContext(fs.Context().Ctx, input)
			if err != nil {
				err = g.Error(err, "Error with ListObjectsV2 for: %#v", input)
				return paths, err
			}
		} else {
			break
		}
	}

	sort.Strings(prefixes)
	sort.Strings(paths)
	paths = append(prefixes, paths...)
	return
}

func (fs *S3FileSysClient) GenerateS3PreSignedURL(s3URL string, dur time.Duration) (httpURL string, err error) {

	s3U, err := net.NewURL(s3URL)
	if err != nil {
		err = g.Error(err, "Could not parse s3 url")
		return
	}

	svc := s3.New(fs.getSession())
	req, _ := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(s3U.Hostname()),
		Key:    aws.String(strings.TrimPrefix(s3U.Path(), "/")),
	})

	httpURL, err = req.Presign(dur)
	if err != nil {
		err = g.Error(err, "Could not request pre-signed s3 url")
		return
	}

	return
}
