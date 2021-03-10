package filesys

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/flarco/g"
	"github.com/minio/minio-go"
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

	fs.bucket = fs.GetProp("AWS_BUCKET")
	if fs.bucket == "" {
		fs.bucket = os.Getenv("AWS_BUCKET")
	}
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
	if strings.HasPrefix(key, "/") {
		key = key[1:]
	}
	if strings.HasSuffix(key, "/") {
		key = key[:len(key)]
	}
	return key
}

// Connect initiates the Google Cloud Storage client
func (fs *S3FileSysClient) Connect() (err error) {

	if fs.GetProp("AWS_ACCESS_KEY_ID") == "" || fs.GetProp("AWS_SECRET_ACCESS_KEY") == "" {
		return g.Error(errors.New("Need to set 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' to interact with S3"))
	}

	region := fs.GetProp("AWS_REGION")
	endpoint := fs.GetProp("AWS_ENDPOINT")

	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/
	fs.session, err = session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(
			fs.GetProp("AWS_ACCESS_KEY_ID"),
			fs.GetProp("AWS_SECRET_ACCESS_KEY"),
			fs.GetProp("AWS_SESSION_TOKEN"),
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
	endpoint := fs.GetProp("AWS_ENDPOINT")
	if strings.HasSuffix(endpoint, ".digitaloceanspaces.com") {
		region := strings.TrimRight(endpoint, ".digitaloceanspaces.com")
		region = strings.TrimLeft(endpoint, "https://")
		fs.RegionMap[fs.bucket] = region
	} else if fs.RegionMap[fs.bucket] == "" && fs.bucket != "" {
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

	return fs.session
}

// Delete deletes the given path (file or directory)
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/file.txt`
func (fs *S3FileSysClient) Delete(path string) (err error) {
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

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(fs.getSession())
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

	uploader := s3manager.NewUploader(fs.getSession())
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

	prefixes := []string{}
	for {

		for _, cp := range result.CommonPrefixes {
			prefixes = append(prefixes, urlPrefix+*cp.Prefix)
		}

		for _, obj := range result.Contents {
			// obj.LastModified
			paths = append(paths, urlPrefix+*obj.Key)
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

///////////////////////////// S3 compatible

// S3cFileSysClient is a file system client to write file to Amazon's S3 file sys.
type S3cFileSysClient struct {
	BaseFileSysClient
	context   g.Context
	bucket    string
	client    *minio.Client
	RegionMap map[string]string
	mux       sync.Mutex
}

// Init initializes the fs client
func (fs *S3cFileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = g.NewContext(ctx)

	fs.bucket = fs.GetProp("AWS_BUCKET")
	if fs.bucket == "" {
		fs.bucket = os.Getenv("AWS_BUCKET")
	}
	fs.RegionMap = map[string]string{}

	return fs.Connect()
}

// Connect initiates the Google Cloud Storage client
func (fs *S3cFileSysClient) Connect() (err error) {

	if fs.GetProp("AWS_ACCESS_KEY_ID") == "" || fs.GetProp("AWS_SECRET_ACCESS_KEY") == "" {
		return g.Error(errors.New("Need to set 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' to interact with S3"))
	}

	endpoint := fs.GetProp("AWS_ENDPOINT")

	fs.client, err = minio.New(endpoint, fs.GetProp("AWS_ACCESS_KEY_ID"), fs.GetProp("AWS_SECRET_ACCESS_KEY"), true)
	if err != nil {
		err = g.Error(err, "Could not create S3 session")
		return
	}

	return
}

// Delete deletes the given path (file or directory)
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/file.txt`
func (fs *S3cFileSysClient) Delete(path string) (err error) {
	bucket, key, err := ParseURL(path)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)

	objectsCh := make(chan string)

	// Send object names that are needed to be removed to objectsCh
	go func() {

		defer close(objectsCh)
		doneCh := make(chan struct{})

		// Indicate to our routine to exit cleanly upon return.
		defer close(doneCh)

		// List all objects from a bucket-name with a matching prefix.
		for object := range fs.client.ListObjects(bucket, key, true, doneCh) {
			if object.Err != nil {
				log.Fatalln(object.Err)
			}
			objectsCh <- object.Key
		}
	}()

	// Call RemoveObjects API
	errorCh := fs.client.RemoveObjects(bucket, objectsCh)

	// Print errors received from RemoveObjects API
	for e := range errorCh {
		err = g.Error(e.Err, "could not delete file")
		return
	}

	return
}

// List lists the file in given directory path
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/directory`
func (fs *S3cFileSysClient) List(path string) (paths []string, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = g.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)
	urlPrefix := fmt.Sprintf("s3://%s/", bucket)

	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{})

	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	// List all objects from a bucket-name with a matching prefix.
	for object := range fs.client.ListObjectsV2(bucket, key, false, doneCh) {
		if object.Err != nil {
			g.LogError(object.Err)
			return
		}
		// fmt.Println(object)
		paths = append(paths, urlPrefix+object.Key)
	}
	return
}

// ListRecursive lists the file in given directory path recusively
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/directory`
func (fs *S3cFileSysClient) ListRecursive(path string) (paths []string, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = g.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)
	urlPrefix := fmt.Sprintf("s3://%s/", bucket)

	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{})

	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	// List all objects from a bucket-name with a matching prefix.
	for object := range fs.client.ListObjectsV2(bucket, key, true, doneCh) {
		if object.Err != nil {
			fmt.Println(object.Err)
			return
		}
		paths = append(paths, urlPrefix+object.Key)
	}
	return
}

// GetReader return a reader for the given path
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/file.txt` or `s3://my_bucket/key/to/directory`
func (fs *S3cFileSysClient) GetReader(path string) (reader io.Reader, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)

	reader, err = fs.client.GetObjectWithContext(fs.Context().Ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		err = g.Error(err, "failed to download file: "+key)
		return
	}

	return
}

func (fs *S3cFileSysClient) Write(path string, reader io.Reader) (bw int64, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = g.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)
	size := int64(-1)

	bw, err = fs.client.PutObjectWithContext(fs.Context().Ctx, bucket, key, reader, size, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		err = g.Error(err, "failed to upload file: "+key)
		return
	}

	err = fs.Context().Err()

	return
}
