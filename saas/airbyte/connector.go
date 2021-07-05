package airbyte

import (
	"context"
	"embed"
	"io/ioutil"
	"os"

	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/flarco/g/process"
	"gopkg.in/yaml.v2"
)

// SourceDefinitionsURL is where the master source list is
const SourceDefinitionsURL = "https://raw.githubusercontent.com/airbytehq/airbyte/master/airbyte-config/init/src/main/resources/seed/source_definitions.yaml"

// AirbyteFolder is the airbyte folder
//go:embed *
var AirbyteFolder embed.FS

// NewAirbyteMessage creates a new airbyte message
func NewAirbyteMessage() (am *AirbyteMessage) {
	am = &AirbyteMessage{}
	return
}

// GetSourceConnectors polls and retrieves the latest connectors sources
func GetSourceConnectors(fetch bool) (connectors Connectors, err error) {

	sourceDefinitionsBytes, _ := AirbyteFolder.ReadFile("sources.yaml")
	if fetch {
		_, respBytes, err := net.ClientDo("GET", SourceDefinitionsURL, nil, nil, 5)
		if err != nil {
			g.Warn("Using local cache since we are unable to Reach URL: " + SourceDefinitionsURL)
		} else {
			sourceDefinitionsBytes = respBytes
		}
	}

	cds := []ConnectorDefinition{}
	if err = yaml.Unmarshal(sourceDefinitionsBytes, &cds); err != nil {
		return connectors, g.Error(err, "could not unmarshal ConnectorDefinition")
	}

	if len(cds) == 0 {
		return connectors, g.Error("empty list of connectors sources")
	}

	connectors = make(Connectors, len(cds))
	for i, cd := range cds {
		connectors[i] = Connector{Definition: cd, State: g.M()}
	}

	return
}

// Connector is an airbyte connector
type Connector struct {
	Definition    ConnectorDefinition
	Specification ConnectorSpecification
	State         map[string]interface{}
	tempFolder    string
}

// InitTempDir initalize temp directory
func (c *Connector) InitTempDir() (err error) {
	if c.tempFolder == "" || !g.PathExists(c.tempFolder) {
		c.tempFolder, err = os.MkdirTemp("", c.Definition.Name)
		if err != nil {
			return g.Error(err, "could not make temp dir")
		}
	}
	return nil
}

func (c *Connector) file(name string) string {
	return c.tempFolder + "/" + name
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
	msgChan = make(chan AirbyteMessage)
	p, err := process.NewProc("docker")
	if err != nil {
		return msgChan, g.Error(err, "could not create process")
	}

	p.SetScanner(func(stderr bool, text string) {
		if stderr {
			// g.Debug(text)
			return
		}
		msg := AirbyteMessage{}
		err = g.Unmarshal(text, &msg)
		g.LogError(err, "could not unmarshall airbyte message")
		if err == nil {
			if msg.Type == TypeLog {
				// g.Debug("LOG: " + strings.TrimSpace(msg.Log.Message))
			} else if g.In(msg.Type, TypeState) {
				c.State = msg.State.Data
			} else {
				msgChan <- msg
			}
		}
	})

	defArgs := []string{
		"run", "--rm",
		"-v", c.tempFolder + ":/work",
		"-w", "/work",
		"-i", c.Definition.Image(),
	}
	args = append(defArgs, args...)

	err = p.Start(args...)
	if err != nil {
		return msgChan, g.Error(err, "error getting spec for "+c.Definition.Name)
	}

	g.P(p.CmdStr())

	go func() {
		defer close(msgChan)
		err = p.Wait()
		g.LogError(err)
		// println(p.Stdout.String())
		// println(p.Stderr.String())
	}()

	return
}

// GetSpec retrieve spec from docker command
func (c *Connector) GetSpec() (err error) {
	messages, err := c.DockerRun("spec")
	if err != nil {
		return g.Error(err, "error getting spec for "+c.Definition.Name)
	} else if len(messages) == 0 {
		return g.Error("no messages received")
	}

	c.Specification = messages[0].Spec
	return
}

// Check attempts to connect to the connector with
// the provided config credentials
func (c *Connector) Check(config map[string]interface{}) (s AirbyteConnectionStatus, err error) {
	err = c.InitTempDir()
	if err != nil {
		err = g.Error(err, "could not create temp dir")
		return
	}
	defer os.RemoveAll(c.tempFolder)

	err = ioutil.WriteFile(c.file("config.json"), []byte(g.Marshal(config)), 0755)
	if err != nil {
		err = g.Error(err, "could not write to config file")
		return
	}

	messages, err := c.DockerRun("check", "--config", "config.json")
	if err != nil {
		return s, g.Error(err, "error checking "+c.Definition.Name)
	} else if len(messages) == 0 {
		return s, g.Error("no messages received")
	}

	s = messages.First(TypeConnectionStatus).ConnectionStatus
	return
}

// Discover detects the structure of the data in the data source.
func (c *Connector) Discover(config map[string]interface{}) (ac AirbyteCatalog, err error) {
	err = c.InitTempDir()
	if err != nil {
		err = g.Error(err, "could not create temp dir")
		return
	}
	defer os.RemoveAll(c.tempFolder)

	err = ioutil.WriteFile(c.file("config.json"), []byte(g.Marshal(config)), 0755)
	if err != nil {
		err = g.Error(err, "could not write to config file")
		return
	}

	messages, err := c.DockerRun("discover", "--config", "config.json")
	if err != nil {
		return ac, g.Error(err, "error discovering "+c.Definition.Name)
	} else if len(messages) == 0 {
		return ac, g.Error("no messages received")
	}

	ac = messages.First(TypeCatalog).Catalog
	return
}

// Discover detects the structure of the data in the data source.
func (c *Connector) Read(config map[string]interface{}, catalog ConfiguredAirbyteCatalog, state map[string]interface{}) (ds *iop.Datastream, err error) {
	err = c.InitTempDir()
	if err != nil {
		err = g.Error(err, "could not create temp dir")
		return
	}

	err = ioutil.WriteFile(c.file("config.json"), []byte(g.Marshal(config)), 0755)
	if err != nil {
		err = g.Error(err, "could not write to config file")
		return
	}

	err = ioutil.WriteFile(c.file("catalog.json"), []byte(g.Marshal(catalog)), 0755)
	if err != nil {
		err = g.Error(err, "could not write to catalog file")
		return
	}

	err = ioutil.WriteFile(c.file("state.json"), []byte(g.Marshal(state)), 0755)
	if err != nil {
		err = g.Error(err, "could not write to state file")
		return
	}

	msgChan, err := c.DockerStart("read", "--config", "config.json", "--catalog", "catalog.json", "--state", "state.json")
	if err != nil {
		return ds, g.Error(err, "error reading "+c.Definition.Name)
	}

	if len(catalog.Streams) == 0 {
		return ds, g.Error("no streams provided")
	}

	columns := catalog.Streams[0].Stream.JsonSchema.Columns()
	fm := columns.FieldMap(true)

	nextFunc := func(it *iop.Iterator) bool {
		for msg := range msgChan {
			it.Row = make([]interface{}, len(fm))
			for k, i := range fm {
				it.Row[i] = msg.Record.Data[k]
			}
			return true
		}
		return false
	}

	cont := g.NewContext(context.Background())
	ds = iop.NewDatastreamIt(cont.Ctx, columns, nextFunc)
	ds.Inferred = true
	ds.Defer(func() { os.RemoveAll(c.tempFolder) })

	err = ds.Start()
	if err != nil {
		cont.Cancel()
		return ds, g.Error(err, "could start datastream")
	}

	return
}
