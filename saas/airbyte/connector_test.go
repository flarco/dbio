package airbyte

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
)

func TestAirbyte(t *testing.T) {
	connectors, err := GetSourceConnectors(false)
	g.AssertNoError(t, err)
	assert.Greater(t, len(connectors), 0)
	// g.P(connectors.Names())

	c, err := connectors.Get("GitHub")
	if !g.AssertNoError(t, err) {
		return
	}

	// assert.Equal(t, "Github Source Spec", c.Specification.ConnectionSpecification.Title)

	config := g.M(
		"credentials", g.M(
			"personal_access_token", os.Getenv("ACCESS_TOKEN"),
		),
		"start_date", "2022-03-01T00:00:00Z",
		"repository", "flarco/dbio",
	)
	s, err := c.Check(config)
	if !g.AssertNoError(t, err) {
		return
	}
	assert.Equal(t, StatusSucceeded, s.Status)
	// g.P(s)

	ac, err := c.Discover(config)
	if !g.AssertNoError(t, err) {
		return
	}
	assert.NotEmpty(t, ac.Streams)
	g.P(ac.Streams.Names())

	catalog := ConfiguredAirbyteCatalog{
		Streams: []ConfiguredAirbyteStream{
			{
				Stream:              ac.GetStream("commits"),
				SyncMode:            SyncModeIncremental,
				DestinationSyncMode: DestinationSyncModeOverwrite,
			},
		},
	}

	ds, err := c.Read(config, catalog, g.M())
	if g.AssertNoError(t, err) {
		data, err := ds.Collect(0)
		g.AssertNoError(t, err)
		assert.Greater(t, len(data.Columns), 0)
		assert.Greater(t, len(data.Rows), 0)
		g.P(data.Columns.Names())
		g.P(len(data.Rows))
	}
}

func Test1(t *testing.T) {
	bin := "docker"
	args := strings.Split(`run --rm -v /tmp/GitHub1462913144:/work -w /work -i airbyte/source-github:0.2.42 discover --config config.json`, " ")
	cmd := exec.Command(bin, args...)
	// err := cmd.Run()
	// g.LogError(err)

	// works
	out, err := cmd.CombinedOutput()
	g.LogError(err)
	g.Debug(string(out))
}
func Test1a(t *testing.T) {
	bin := "docker"
	args := strings.Split(`run --rm -v /tmp/GitHub1462913144:/work -w /work -i airbyte/source-github:0.2.42 discover --config config.json`, " ")
	cmd := exec.Command(bin, args...)

	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf

	done := make(chan struct{})
	err := cmd.Start()
	g.LogError(err)

	go func() {
		for {
			select {
			case <-done:
				return
			default:
				if buf.Len() > 0 {
					b, err := buf.ReadByte()
					if err == nil {
						fmt.Print(string(b))
					}
				} else {
					time.Sleep(10 * time.Millisecond)
				}
			}
		}
	}()
	err = cmd.Wait()
	g.LogError(err)
	close(done)
	// g.Debug(buf.String())
}

// WINNER
func Test1b(t *testing.T) {
	bin := "docker"
	args := strings.Split(`run --rm -v /tmp/GitHub1462913144:/work -w /work -i airbyte/source-github:0.2.42 discover --config config.json`, " ")
	cmd := exec.Command(bin, args...)

	var stdoutBuf, stderrBuf, combinedBuf bytes.Buffer

	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	errCnt := 0

	done := make(chan struct{})
	err := cmd.Start()
	g.LogError(err)

	go func() {
		processStdoutByte := func(b byte) {
			fmt.Print(string(b))
			combinedBuf.WriteByte(b)
		}
		processStderrByte := func(b byte) {
			fmt.Print(string(b))
			combinedBuf.WriteByte(b)
		}

		var stdoutErr, stderrErr error
		processStreams := func() {
			var b byte

			stdoutErr = nil
			for stdoutErr == nil {
				b, stdoutErr = stdoutBuf.ReadByte()
				if stdoutErr == nil {
					processStdoutByte(b)
				}
			}

			stderrErr = nil
			for stderrErr == nil {
				b, stderrErr = stderrBuf.ReadByte()
				if stderrErr == nil {
					processStderrByte(b)
				}
			}
		}

		for {
			select {
			case <-done:
				// drain remaining
				processStreams()
				return
			default:
				processStreams()
				time.Sleep(10 * time.Millisecond)
				errCnt++
			}
		}
	}()

	err = cmd.Wait()
	g.LogError(err)
	close(done)
	g.Debug("errCnt: %d", errCnt)
}

func Test2(t *testing.T) {
	bin := "docker"
	args := strings.Split(`run --rm -v /tmp/GitHub1462913144:/work -w /work -i airbyte/source-github:0.2.42 discover --config config.json`, " ")
	cmd := exec.Command(bin, args...)
	// err := cmd.Run()
	// g.LogError(err)

	// works
	stdout, err := cmd.StdoutPipe()
	g.LogError(err)
	err = cmd.Start()
	g.LogError(err)

	scanner := bufio.NewScanner(stdout)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		m := scanner.Text()
		fmt.Println(m)
	}
	err = cmd.Wait()
	g.LogError(err)
}

func Test3(t *testing.T) {
	bin := "docker"
	args := strings.Split(`run --rm -v /tmp/GitHub1462913144:/work -w /work -i airbyte/source-github:0.2.42 discover --config config.json`, " ")
	// bin = "ls"
	// args = strings.Split(`-l /`, " ")
	cmd := exec.Command(bin, args...)
	// err := cmd.Run()
	// g.LogError(err)

	// works
	stdout, err := cmd.StdoutPipe()
	g.LogError(err)
	stderr, err := cmd.StderrPipe()
	g.LogError(err)

	err = cmd.Start()
	g.LogError(err)

	var combinedBuf bytes.Buffer
	var stdoutBuf, stdoutLineBuf bytes.Buffer
	// var stderrBuf, stderrLineBuf bytes.Buffer
	var errOut, errErr error
	var outDone, errDone bool
	stdoutByte := make([]byte, 1)
	stderrByte := make([]byte, 1)

	g.Debug(cmd.String())
	for {
		if errOut == nil {
			_, errOut = stdout.Read(stdoutByte)
			g.LogError(errOut)
			if errOut != nil {
				outDone = true
				break
			}
		}
		if errErr == nil {
			_, errErr = stderr.Read(stderrByte)
			g.LogError(errErr)
			if errErr != nil {
				errDone = true
			}
		}

		if errOut == nil {
			_, err = stdoutLineBuf.Write(stdoutByte)
			if err != nil {
				g.LogError(err)
				println()
				break
			}
			stdoutBuf.Write(stdoutByte)
			combinedBuf.Write(stdoutByte)
			fmt.Print(string(stdoutByte[0]))
			if stdoutByte[0] == '\n' {
				// is new line
				fmt.Print(stdoutLineBuf.String())
				stdoutLineBuf.Reset()
			}
		}

		// if errErr == nil {
		// 	_, err = stderrLineBuf.Write(stderrByte)
		// 	if err != nil {
		// 		g.LogError(err)
		// 		println()
		// 		break
		// 	}
		// 	stderrBuf.Write(stderrByte)
		// 	combinedBuf.Write(stderrByte)
		// 	fmt.Print(string(stderrByte[0]))
		// 	if stderrByte[0] == '\n' {
		// 		// is new line
		// 		fmt.Print(stderrLineBuf.String())
		// 		stderrLineBuf.Reset()
		// 	}
		// }
		if errOut != nil && errErr != nil {
			break
		} else if outDone && errDone {
			break
		}
	}

	err = cmd.Wait()
	g.LogError(err)
	// fmt.Print(combinedBuf.String())
}

func Test4(t *testing.T) {

	var buf bytes.Buffer
	g.Debug(buf.String())
	buf.WriteByte('1')
	buf.WriteByte('2')
	g.Debug(buf.String())
	g.Debug(buf.String())

	b, _ := buf.ReadByte()
	g.Debug(string(b))
	g.Debug(buf.String())

	b, _ = buf.ReadByte()
	g.Debug(string(b))
	g.Debug(buf.String())

	b, _ = buf.ReadByte()
	g.Debug(string(b))
	g.Debug(buf.String())
}
