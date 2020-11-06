package filesys

import (
	"bufio"
	"context"
	"io"
	"net/url"
	"os"
	"sort"
	"strings"

	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/pkg/sftp"
	"github.com/spf13/cast"
)

// SftpFileSysClient is for SFTP / SSH file ops
type SftpFileSysClient struct {
	BaseFileSysClient
	context   g.Context
	client    *sftp.Client
	sshClient *iop.SSHClient
}

// Init initializes the fs client
func (fs *SftpFileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = g.NewContext(ctx)
	return fs.Connect()
}

// Connect initiates the Google Cloud Storage client
func (fs *SftpFileSysClient) Connect() (err error) {

	if fs.GetProp("SSH_PRIVATE_KEY") == "" {
		fs.SetProp("SFTP_PRIVATE_KEY", os.Getenv("SSH_PRIVATE_KEY"))
	}

	if fs.GetProp("SFTP_URL") != "" {
		u, err := url.Parse(fs.GetProp("SFTP_URL"))
		if err != nil {
			return g.Error(err, "could not parse SFTP URL")
		}

		host := u.Hostname()
		user := u.User.Username()
		password, _ := u.User.Password()
		sshPort := cast.ToInt(u.Port())
		if sshPort == 0 {
			sshPort = 22
		}

		if user != "" {
			fs.SetProp("SFTP_USER", user)
		}
		if password != "" {
			fs.SetProp("SFTP_PASSWORD", password)
		}
		if host != "" {
			fs.SetProp("SFTP_HOST", host)
		}
		if sshPort != 0 {
			fs.SetProp("SFTP_PORT", cast.ToString(sshPort))
		}
	}

	fs.sshClient = &iop.SSHClient{
		Host:       fs.GetProp("SFTP_HOST"),
		Port:       cast.ToInt(fs.GetProp("SFTP_PORT")),
		User:       fs.GetProp("SFTP_USER"),
		Password:   fs.GetProp("SFTP_PASSWORD"),
		PrivateKey: fs.GetProp("SFTP_PRIVATE_KEY"),
	}

	err = fs.sshClient.Connect()
	if err != nil {
		return g.Error(err, "unable to connect to ssh server ")
	}

	fs.client, err = fs.sshClient.SftpClient()
	if err != nil {
		return g.Error(err, "unable to start SFTP client")
	}

	return nil
}

func (fs *SftpFileSysClient) getPrefix() string {
	return g.F(
		"sftp://%s@%s:%s",
		fs.GetProp("SFTP_USER"),
		fs.GetProp("SFTP_HOST"),
		fs.GetProp("SFTP_PORT"),
	)
}

func (fs *SftpFileSysClient) cleanKey(key string) string {
	if strings.HasPrefix(key, "/") {
		key = key[1:]
	}
	if strings.HasSuffix(key, "/") {
		key = key[:len(key)-1]
	}
	return key
}

// List list objects in path
func (fs *SftpFileSysClient) List(url string) (paths []string, err error) {
	_, path, err := ParseURL(url)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+url)
		return
	}
	path = "/" + fs.cleanKey(path)

	stat, err := fs.client.Stat(path)
	if err != nil {
		return paths, g.Error(err, "error stating path")
	}

	var files []os.FileInfo
	if stat.IsDir() {
		files, err = fs.client.ReadDir(path)
		if err != nil {
			return paths, g.Error(err, "error listing path")
		}
		path = path + "/"
	} else {
		paths = append(paths, g.F("%s%s", fs.getPrefix(), path))
		return
	}

	for _, file := range files {
		path := g.F("%s%s%s", fs.getPrefix(), path, file.Name())
		paths = append(paths, path)
	}
	sort.Strings(paths)

	return
}

// ListRecursive list objects in path recursively
func (fs *SftpFileSysClient) ListRecursive(url string) (paths []string, err error) {
	_, path, err := ParseURL(url)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+url)
		return
	}
	path = "/" + fs.cleanKey(path)

	stat, err := fs.client.Stat(path)
	if err != nil {
		return paths, g.Error(err, "error stating path")
	}

	var files []os.FileInfo
	if stat.IsDir() {
		files, err = fs.client.ReadDir(path)
		if err != nil {
			return paths, g.Error(err, "error listing path")
		}
		path = path + "/"
	} else {
		paths = append(paths, g.F("%s%s", fs.getPrefix(), path))
		return
	}

	for _, file := range files {
		path := g.F("%s%s%s", fs.getPrefix(), path, file.Name())
		if file.IsDir() {
			subPaths, err := fs.ListRecursive(path)
			// g.P(subPaths)
			if err != nil {
				return []string{}, g.Error(err, "error listing sub path")
			}
			paths = append(paths, subPaths...)
		} else {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)

	return
}

// Delete list objects in path
func (fs *SftpFileSysClient) Delete(urlStr string) (err error) {
	_, path, err := ParseURL(urlStr)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+urlStr)
		return
	}
	path = "/" + fs.cleanKey(path)
	paths, err := fs.ListRecursive(urlStr)
	if err != nil {
		return g.Error(err, "error listing path")
	}

	for _, sPath := range paths {
		_, sPath, _ = ParseURL(sPath)
		sPath = "/" + fs.cleanKey(sPath)
		err = fs.client.Remove(sPath)
		if err != nil {
			return g.Error(err, "error deleting path "+sPath)
		}
	}

	err = fs.client.Remove(path)
	if err != nil && !strings.Contains(err.Error(), "not exist") {
		return g.Error(err, "error deleting path")
	}
	return nil
}

// MkdirAll creates child directories
func (fs *SftpFileSysClient) MkdirAll(path string) (err error) {
	return fs.client.MkdirAll(path)
}

func (fs *SftpFileSysClient) Write(urlStr string, reader io.Reader) (bw int64, err error) {
	_, path, err := ParseURL(urlStr)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+urlStr)
		return
	}
	// manage concurrency
	defer fs.Context().Wg.Write.Done()
	fs.Context().Wg.Write.Add()

	filePathArr := strings.Split(path, "/")
	if len(filePathArr) > 1 {
		folderPath := strings.Join(filePathArr[:len(filePathArr)-1], "/")
		err = fs.client.MkdirAll(folderPath)
		if err != nil {
			err = g.Error(err, "Unable to create directory "+folderPath)
			return
		}
	}

	file, err := fs.client.Create(path)
	if err != nil {
		err = g.Error(err, "Unable to open "+path)
		return
	}
	bw, err = io.Copy(io.Writer(file), reader)
	if err != nil {
		err = g.Error(err, "Error writing from reader")
	}
	return
}

// GetReader return a reader for the given path
func (fs *SftpFileSysClient) GetReader(urlStr string) (reader io.Reader, err error) {
	_, path, err := ParseURL(urlStr)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+urlStr)
		return
	}

	file, err := fs.client.Open(path)
	if err != nil {
		err = g.Error(err, "Unable to open "+path)
		return
	}

	pipeR, pipeW := io.Pipe()

	go func() {
		defer pipeW.Close()

		reader = bufio.NewReader(file)

		_, err = io.Copy(pipeW, reader)
		if err != nil {
			fs.Context().CaptureErr(g.Error(err, "Error writing from reader"))
			fs.Context().Cancel()
			g.LogError(fs.Context().Err())
		}

	}()

	return pipeR, err
}

// GetWriter creates the file if non-existent and return a writer
func (fs *SftpFileSysClient) GetWriter(urlStr string) (writer io.Writer, err error) {
	_, path, err := ParseURL(urlStr)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+urlStr)
		return
	}
	file, err := fs.client.Create(path)
	if err != nil {
		err = g.Error(err, "Unable to open "+path)
		return
	}
	writer = io.Writer(file)
	return
}
