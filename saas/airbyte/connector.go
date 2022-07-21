package airbyte

import (
	"context"
	"embed"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strings"

	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/flarco/g/process"
	"github.com/spf13/cast"
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

			// write to file
			_, filename, _, ok := runtime.Caller(0)
			if ok {
				filePath := g.F("%s/sources.yaml", path.Dir(filename))
				if g.PathExists(filePath) {
					err = ioutil.WriteFile(filePath, respBytes, 0755)
					if !g.LogError(err) {
						g.Debug("wrote latest to %s", filePath)
					}
				}
			}
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
		ctx := g.NewContext(context.Background())
		connectors[i] = Connector{
			Definition: cd,
			State:      g.M(),
			ctx:        &ctx,
		}
	}

	return
}

// Connector is an airbyte connector
type Connector struct {
	Definition    ConnectorDefinition
	Specification ConnectorSpecification
	State         map[string]interface{}
	tempFolder    string
	ctx           *g.Context
}

// InitTempDir initalize temp directory
func (c *Connector) InitTempDir() (err error) {
	if c.tempFolder == "" || !g.PathExists(c.tempFolder) {
		if c.tempFolder == "" {
			c.tempFolder, err = os.MkdirTemp("", c.Definition.Name)
		} else {
			err = os.MkdirAll(c.tempFolder, 0700)
		}
		if err != nil {
			return g.Error(err, "could not make temp dir")
		}
	}
	return nil
}

func (c *Connector) file(name string) string {
	return c.tempFolder + "/" + name
}

func (c *Connector) Key() string {
	key := strings.ReplaceAll(
		c.Definition.DockerRepository, "airbyte/source-", "",
	)
	key = strings.ReplaceAll(key, "-singer", "")
	c.Specification.ConnectionSpecification.Title = c.Definition.Name
	return key
}

// GetSpec retrieve spec from docker command
func (c *Connector) GetSpec() (err error) {
	err = c.InitTempDir()
	if err != nil {
		err = g.Error(err, "could not create temp dir")
		return
	}
	defer os.RemoveAll(c.tempFolder)

	messages, err := c.DockerRun("spec")
	if err != nil {
		return g.Error(err, "error getting spec for "+c.Definition.Name)
	} else if len(messages) == 0 {
		return g.Error("no messages received")
	}

	if err := messages.CheckError(); err != nil {
		return g.Error(err)
	}

	if messages[0].Spec != nil {
		c.Specification = *messages[0].Spec
	}
	return
}

// Pull pulls the docker image. Useful for outputing
func (c *Connector) Pull() (err error) {
	p, err := process.NewProc("docker")
	if err != nil {
		return g.Error(err, "could not create process")
	}
	args := []string{"pull", c.Definition.Image()}

	p.Workdir = g.UserHomeDir()
	p.SetScanner(func(stderr bool, text string) {
		if strings.Contains(text, "Pulling from ") {
			g.Info(text)
		} else {
			g.Debug(text)
		}
	})

	err = p.Run(args...)
	if err != nil {
		return g.Error(err, "error pulling image "+c.Definition.Name)
	}

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

	if msg := messages.First(TypeConnectionStatus); msg.ConnectionStatus != nil {
		s = *msg.ConnectionStatus
		if s.Status == StatusFailed {
			err = g.Error(s.Message)
			return
		}
	}

	if err := messages.CheckError(); err != nil {
		return s, g.Error(err)
	}
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

	if err := messages.CheckError(); err != nil {
		return ac, g.Error(err)
	}

	if msg := messages.First(TypeCatalog); msg.Catalog != nil {
		ac = *msg.Catalog
	}
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

	if state == nil {
		state = map[string]interface{}{}
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
			if err := msg.CheckError(); err != nil {
				ds.Context.CaptureErr(err)
				return false
			} else if msg.Record == nil {
				if msg.Log != nil {
					g.Debug(cast.ToString(msg.Log.Message))
				}
				continue
			}

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
	ds.Defer(func() { os.RemoveAll(c.tempFolder) })

	err = ds.Start()
	if err != nil {
		cont.Cancel()
		return ds, g.Error(err, "could start datastream")
	}

	return
}
