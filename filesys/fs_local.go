package filesys

import (
	"bufio"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/spf13/cast"
)

// LocalFileSysClient is a file system client to write file to local file sys.
type LocalFileSysClient struct {
	BaseFileSysClient
	context g.Context
}

// Init initializes the fs client
func (fs *LocalFileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = g.NewContext(ctx)
	return
}

func cleanLocalFilePath(path string) string {
	return strings.TrimPrefix(path, "file://")
}

// Delete deletes the given path (file or directory)
func (fs *LocalFileSysClient) delete(path string) (err error) {
	path = cleanLocalFilePath(path)
	file, err := os.Stat(path)
	if err != nil {
		if strings.Contains(err.Error(), "no such file or directory") {
			// path is already deleted
			return nil
		}

		err = g.Error(err, "Unable to Stat "+path)
		return
	}

	switch mode := file.Mode(); {
	case mode.IsDir():
		err = os.RemoveAll(path)
		if err != nil {
			err = g.Error(err, "Unable to delete "+path)
		}
	case mode.IsRegular():
		err = os.Remove(path)
		if err != nil {
			err = g.Error(err, "Unable to delete "+path)
		}
	}
	return
}

// GetReader return a reader for the given path
func (fs *LocalFileSysClient) GetReader(path string) (reader io.Reader, err error) {
	path = cleanLocalFilePath(path)
	file, err := os.Open(path)
	if err != nil {
		err = g.Error(err, "Unable to open "+path)
		return
	}

	return bufio.NewReader(file), err
}

// GetDatastream return a datastream for the given path
func (fs *LocalFileSysClient) GetDatastream(path string) (ds *iop.Datastream, err error) {
	path = cleanLocalFilePath(path)
	file, err := os.Open(path)
	if err != nil {
		err = g.Error(err, "Unable to open "+path)
		return nil, err
	}

	ds = iop.NewDatastreamContext(fs.Context().Ctx, nil)
	ds.SafeInference = true
	ds.SetMetadata(fs.GetProp("METADATA"))
	ds.Metadata.StreamURL.Value = path
	ds.SetConfig(fs.Props())
	g.Debug("%s, reading datastream from %s", ds.ID, path)

	if strings.Contains(strings.ToLower(path), ".xlsx") {
		eDs, err := getExcelStream(fs.Self(), bufio.NewReader(file))
		if err != nil {
			err = g.Error(err, "Error consuming Excel reader")
			return ds, err
		}
		return eDs, nil
	}

	go func() {
		// manage concurrency
		defer fs.Context().Wg.Read.Done()
		fs.Context().Wg.Read.Add()

		fileFormat := FileType(cast.ToString(fs.GetProp("FORMAT")))
		if string(fileFormat) == "" {
			if strings.Contains(strings.ToLower(path), FileTypeJson.Ext()) {
				fileFormat = FileTypeJson
			} else if strings.HasSuffix(strings.ToLower(path), FileTypeXml.Ext()) {
				fileFormat = FileTypeXml
			} else if strings.HasSuffix(strings.ToLower(path), FileTypeParquet.Ext()) {
				fileFormat = FileTypeParquet
			} else {
				fileFormat = FileTypeCsv
			}
		}

		switch fileFormat {
		case FileTypeJson, FileTypeJsonLines:
			err = ds.ConsumeJsonReader(bufio.NewReader(file))
		case FileTypeXml:
			err = ds.ConsumeXmlReader(bufio.NewReader(file))
		case FileTypeParquet:
			err = ds.ConsumeParquetReaderSeeker(file)
		case FileTypeCsv:
			err = ds.ConsumeCsvReader(bufio.NewReader(file))
		default:
			g.Warn("LocalFileSysClient | File Format not recognized: %s", fileFormat)
		}

		if err != nil {
			fs.Context().CaptureErr(g.Error(err, "Error consuming reader"))
			fs.Context().Cancel()
			g.LogError(fs.Context().Err())
		}

	}()

	return ds, err
}

// GetWriter creates the file if non-existent and return a writer
func (fs *LocalFileSysClient) GetWriter(path string) (writer io.Writer, err error) {
	path = cleanLocalFilePath(path)
	file, err := os.Create(path)
	if err != nil {
		err = g.Error(err, "Unable to open "+path)
		return
	}
	writer = io.Writer(file)
	return
}

// MkdirAll creates child directories
func (fs *LocalFileSysClient) MkdirAll(path string) (err error) {
	path = cleanLocalFilePath(path)
	return os.MkdirAll(path, 0755)
}

// Write creates the file if non-existent and writes from the reader
func (fs *LocalFileSysClient) Write(filePath string, reader io.Reader) (bw int64, err error) {
	filePath = cleanLocalFilePath(filePath)
	// manage concurrency
	defer fs.Context().Wg.Write.Done()
	fs.Context().Wg.Write.Add()

	// create folder if needed
	folderPath := path.Dir(filePath)
	if !g.PathExists(folderPath) {
		err = os.MkdirAll(folderPath, 0777)
		if err != nil {
			go io.Copy(ioutil.Discard, reader)
			err = g.Error(err, "Unable to create folder "+folderPath)
			return
		}
	}

	file, err := os.Create(filePath)
	if err != nil {
		go io.Copy(ioutil.Discard, reader)
		err = g.Error(err, "Unable to open "+filePath)
		return
	}
	bw, err = io.Copy(io.Writer(file), reader)
	if err != nil {
		err = g.Error(err, "Error writing from reader")
	}
	return
}

// List lists the file in given directory path
func (fs *LocalFileSysClient) List(path string) (paths []string, err error) {
	path = cleanLocalFilePath(path)

	s, err := os.Stat(path)
	if err == nil && !s.IsDir() {
		return []string{path}, nil
	}

	files, err := ioutil.ReadDir(path)
	if err != nil {
		err = g.Error(err, "Error listing "+path)
		return
	}

	for _, file := range files {
		// file.ModTime()
		paths = append(paths, path+"/"+file.Name())
	}
	return
}

// ListRecursive lists the file in given directory path recursively
func (fs *LocalFileSysClient) ListRecursive(path string) (paths []string, err error) {
	path = cleanLocalFilePath(path)
	ts := fs.GetRefTs()

	walkFunc := func(subPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && (ts.IsZero() || info.ModTime().IsZero() || info.ModTime().After(ts)) {
			paths = append(paths, subPath)
		}
		return nil
	}
	err = filepath.Walk(path, walkFunc)
	if err != nil {
		err = g.Error(err, "Error listing "+path)
	}
	return
}
