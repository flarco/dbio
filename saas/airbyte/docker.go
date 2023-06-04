package airbyte

import (
	"context"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/flarco/g"
	"github.com/flarco/g/process"
)

func (c *Connector) dockerScanner(msgChan chan AirbyteMessage) func(stderr bool, text string) {
	return func(stderr bool, text string) {
		// g.Debug("(stderr: %t)\n%s", stderr, text)
		g.Trace(text)

		if stderr || !strings.HasPrefix(strings.TrimSpace(text), "{") {
			// g.Debug(text)
			return
		}
		msg := AirbyteMessage{}
		err := g.Unmarshal(text, &msg)
		g.LogError(err, "could not unmarshall airbyte message for %s", c.Key())
		if err == nil {
			if g.In(msg.Type, TypeState) {
				c.State = msg.State.Data
			} else {
				msgChan <- msg
			}
		}
	}
}

// DockerRun runs a docker command and waits for the end
func (c *Connector) DockerRun(args ...string) (messages AirbyteMessages, err error) {
	msgChan, err := c.DockerStart(args...)
	if err != nil {
		return messages, g.Error(err, "could not start process")
	}

	for msg := range msgChan {
		messages = append(messages, msg)
	}

	return
}

// DockerStart starts the process and returns the channel of messages
func (c *Connector) DockerStart(args ...string) (msgChan chan AirbyteMessage, err error) {
	_ = c.startUsingClient
	return c.startUsingShell(args...)
	// return c.startUsingClient(args...)
}

func (c *Connector) startUsingClient(args ...string) (msgChan chan AirbyteMessage, err error) {

	msgChan = make(chan AirbyteMessage)

	opts := ContainerOptions{
		Image:      c.Definition.Image(),
		Mounts:     map[string]string{c.tempFolder: "/work"},
		WorkingDir: "/work",
		Cmd:        args,
		Scanner:    c.dockerScanner(msgChan),
		AutoRemove: true,
	}
	cont, err := ContainerStart(c.ctx.Ctx, &opts)
	if err != nil {
		return msgChan, g.Error(err, "error starting docker command: "+c.Definition.Name)
	}

	g.Debug("open msgChan %#v", args)
	go func() {
		err = cont.Wait()
		g.LogError(err)
		g.Debug("closing msgChan %#v", args)
		close(msgChan)
		// g.Info(p.Combined.String())
		// g.Info(p.Stdout.String())
		// println(p.Stderr.String())
	}()

	return
}

func (c *Connector) startUsingShell(args ...string) (msgChan chan AirbyteMessage, err error) {
	msgChan = make(chan AirbyteMessage)
	p, err := process.NewProc("docker")
	if err != nil {
		return msgChan, g.Error(err, "could not create process")
	}

	p.SetScanner(c.dockerScanner(msgChan))

	contName := g.RandSuffix("airbyte_"+c.Key()+"_", 6)
	defArgs := []string{
		"run", "--rm",
		"-v", c.tempFolder + ":/work",
		"-w", "/work",
		"--name", contName,
		"-i", c.Definition.Image(),
	}
	args = append(defArgs, args...)

	p.Workdir = c.tempFolder
	err = p.Start(args...)
	if err != nil {
		return msgChan, g.Error(err, "error starting docker process: "+c.Definition.Name)
	}

	g.Debug(p.CmdStr())

	dockerClient, _ := client.NewClientWithOpts()

	// get container ID
	contID := ""
	go func() {
		if dockerClient == nil {
			return
		}

		tries := 0
		for contID == "" && tries <= 5 {
			tries++
			containers, _ := dockerClient.ContainerList(context.Background(), types.ContainerListOptions{All: true})
			for _, cont := range containers {
				if len(cont.Names) > 0 && strings.HasSuffix(cont.Names[0], contName) {
					contID = cont.ID
					return
				}
			}
			time.Sleep(time.Duration(tries) * 100 * time.Millisecond)
		}
	}()

	go func() {
		defer close(msgChan)
		err = p.Wait()
		g.LogError(err)
	}()

	// listen for context cancel
	go func() {
		select {
		case <-p.Done:
			return
		case <-c.ctx.Ctx.Done():
		}

		g.Debug("stopping container %s (%s)", contName, contID)
		to := time.Duration(5 * time.Second)
		go dockerClient.ContainerStop(context.Background(), contID, &to)

		select {
		case <-p.Done:
			return
		case <-time.NewTimer(to).C:
			g.Debug("removing container %s", contID)
			removeOptions := types.ContainerRemoveOptions{RemoveVolumes: true, Force: true}
			go dockerClient.ContainerRemove(context.Background(), contID, removeOptions)
			close(p.Done)
		}
	}()

	return
}
