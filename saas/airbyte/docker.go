package airbyte

import (
	"strings"

	"github.com/flarco/g"
	"github.com/flarco/g/process"
)

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
	msgChan = make(chan AirbyteMessage)

	scanner := func(stderr bool, text string) {
		// g.Debug("(stderr: %t)\n%s", stderr, text)
		// g.Debug(text)

		if stderr || !strings.HasPrefix(strings.TrimSpace(text), "{") {
			// g.Debug(text)
			return
		}
		msg := AirbyteMessage{}
		err = g.Unmarshal(text, &msg)
		g.LogError(err, "could not unmarshall airbyte message for %s", c.Key())
		if err == nil {
			if g.In(msg.Type, TypeState) {
				c.State = msg.State.Data
			} else {
				msgChan <- msg
			}
		}
	}

	opts := process.ContainerOptions{
		Image:      c.Definition.Image(),
		Mounts:     map[string]string{c.tempFolder: "/work"},
		WorkingDir: "/work",
		Cmd:        args,
		Scanner:    scanner,
		AutoRemove: true,
	}
	cont, err := process.ContainerStart(c.ctx.Ctx, &opts)
	if err != nil {
		return msgChan, g.Error(err, "error starting docker command: "+c.Definition.Name)
	}

	go func() {
		defer close(msgChan)
		err = cont.Wait()
		g.LogError(err)
		// g.Info(p.Combined.String())
		// g.Info(p.Stdout.String())
		// println(p.Stderr.String())
	}()

	return
}
