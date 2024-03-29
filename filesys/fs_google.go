package filesys

import (
	"context"
	"io"
	"strings"

	gcstorage "cloud.google.com/go/storage"
	"github.com/flarco/g"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GoogleFileSysClient is a file system client to write file to Amazon's S3 file sys.
type GoogleFileSysClient struct {
	BaseFileSysClient
	client  *gcstorage.Client
	context g.Context
	bucket  string
}

// Init initializes the fs client
func (fs *GoogleFileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = g.NewContext(ctx)

	return fs.Connect()
}

// Connect initiates the Google Cloud Storage client
func (fs *GoogleFileSysClient) Connect() (err error) {
	var authOpthion option.ClientOption

	if val := fs.GetProp("GC_CRED_JSON_BODY"); val != "" {
		authOpthion = option.WithCredentialsJSON([]byte(val))
	} else if val := fs.GetProp("GC_CRED_API_KEY"); val != "" {
		authOpthion = option.WithAPIKey(val)
	} else if val := fs.GetProp("GC_CRED_FILE"); val != "" {
		authOpthion = option.WithCredentialsFile(val)
	} else if val := fs.GetProp("credentials_json"); val != "" {
		authOpthion = option.WithCredentialsJSON([]byte(val))
	} else {
		return g.Error("Could not connect. Did not provide credentials")
	}

	fs.bucket = fs.GetProp("GC_BUCKET")

	fs.client, err = gcstorage.NewClient(fs.Context().Ctx, authOpthion)
	if err != nil {
		err = g.Error(err, "Could not connect to GS Storage")
		return
	}

	return nil
}

func (fs *GoogleFileSysClient) Write(path string, reader io.Reader) (bw int64, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = g.Error(err, "Error Parsing url: "+path)
		return
	}
	key = cleanKey(key)

	obj := fs.client.Bucket(bucket).Object(key)
	wc := obj.NewWriter(fs.Context().Ctx)
	bw, err = io.Copy(wc, reader)
	if err != nil {
		err = g.Error(err, "Error Copying")
		return
	}

	if err = wc.Close(); err != nil {
		err = g.Error(err, "Error Closing writer")
		return
	}
	return
}

// GetReader returns the reader for the given path
func (fs *GoogleFileSysClient) GetReader(path string) (reader io.Reader, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = g.Error(err, "Error Parsing url: "+path)
		return
	}
	key = cleanKey(key)
	reader, err = fs.client.Bucket(bucket).Object(key).NewReader(fs.Context().Ctx)
	if err != nil {
		err = g.Error(err, "Could not get reader for "+path)
		return
	}
	return
}

// List returns the list of objects
func (fs *GoogleFileSysClient) List(path string) (paths []string, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = g.Error(err, "Error Parsing url: "+path)
		return
	}
	key = cleanKey(key)
	keyArr := strings.Split(key, "/")

	query := &gcstorage.Query{Prefix: key}
	query.SetAttrSelection([]string{"Name"})
	it := fs.client.Bucket(bucket).Objects(fs.Context().Ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			err = nil
			break
		}
		if err != nil {
			err = g.Error(err, "Error Iterating")
			return paths, err
		}
		if attrs.Name == "" {
			continue
		}
		if len(strings.Split(attrs.Name, "/")) == len(keyArr)+1 {
			// attrs.Updated
			paths = append(paths, g.F("gs://%s/%s", bucket, attrs.Name))
		}
	}
	return
}

// ListRecursive returns the list of objects recursively
func (fs *GoogleFileSysClient) ListRecursive(path string) (paths []string, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = g.Error(err, "Error Parsing url: "+path)
		return
	}
	key = cleanKey(key)

	query := &gcstorage.Query{Prefix: key}
	query.SetAttrSelection([]string{"Name"})
	it := fs.client.Bucket(bucket).Objects(fs.Context().Ctx, query)
	for {
		attrs, err := it.Next()
		// g.P(attrs)
		if err == iterator.Done {
			err = nil
			break
		}
		if err != nil {
			err = g.Error(err, "Error Iterating")
			return paths, err
		}
		if attrs.Name == "" {
			continue
		}
		paths = append(paths, g.F("gs://%s/%s", bucket, attrs.Name))
	}
	return
}

// Delete list objects in path
func (fs *GoogleFileSysClient) Delete(urlStr string) (err error) {
	bucket, key, err := ParseURL(urlStr)
	if err != nil || bucket == "" {
		err = g.Error(err, "Error Parsing url: "+urlStr)
		return
	}
	key = cleanKey(key)
	urlStrs, err := fs.ListRecursive(urlStr)
	if err != nil {
		err = g.Error(err, "Error List from url: "+urlStr)
		return
	}

	delete := func(key string) {
		defer fs.Context().Wg.Write.Done()
		o := fs.client.Bucket(bucket).Object(key)
		if err = o.Delete(fs.Context().Ctx); err != nil {
			if strings.Contains(err.Error(), "doesn't exist") {
				g.Debug("tried to delete %s\n%s", urlStr, err.Error())
				err = nil
			} else {
				err = g.Error(err, "Could not delete "+urlStr)
				fs.Context().CaptureErr(err)
			}
		}
	}

	for _, path := range urlStrs {
		bucket, key, err = ParseURL(path)
		if err != nil || bucket == "" {
			err = g.Error(err, "Error Parsing url: "+path)
			return
		}
		key = cleanKey(key)
		fs.Context().Wg.Write.Add()
		go delete(key)
	}
	fs.Context().Wg.Write.Wait()
	if fs.Context().Err() != nil {
		err = g.Error(fs.Context().Err(), "Could not delete "+urlStr)
	}
	return
}
