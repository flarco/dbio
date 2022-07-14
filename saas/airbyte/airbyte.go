package airbyte

import (
	"context"
	"strings"
	"time"

	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
)

// Airbyte is for connections for Airbyte
type Airbyte struct {
	Context    *g.Context
	Connector  *Connector
	Catalog    AirbyteCatalog
	properties map[string]string
	config     map[string]interface{}
}

// NewAirbyteConnection creates a new airbyte connection object
func NewAirbyteConnection(name string, config map[string]interface{}) (a *Airbyte, err error) {
	connectors, err := GetSourceConnectors(false)
	if err != nil {
		err = g.Error(err, "could not get connectors")
		return
	}

	c, err := connectors.Get(name)
	if err != nil {
		err = g.Error(err, "could not get connector: %s", name)
		return
	}

	context := g.NewContext(context.Background())
	a = &Airbyte{
		Connector: &c, config: config,
		Context: &context, properties: map[string]string{},
	}
	return
}

// Init initializes the airbyte connection
func (a *Airbyte) Init() (err error) {
	status, err := a.Connector.Check(a.config)
	if err != nil {
		err = g.Error(err, "could not check credentials")
		return
	} else if status.Status == StatusFailed {
		err = g.Error("failed credentials check: %s", status.Message)
	}
	_, err = a.ListObjects()
	if err != nil {
		err = g.Error(err, "could not discover streams")
		return
	}
	return
}

// Close closes the connection
func (a *Airbyte) Close() (err error) {
	a.Context.Cancel()
	return
}

// GetProp returns the value of a property
func (a *Airbyte) GetProp(key string) (val string) {
	return a.properties[strings.ToLower(key)]
}

// SetProp sets the value of a property
func (a *Airbyte) SetProp(key string, val string) {
	if a.properties == nil {
		a.properties = map[string]string{}
	}
	a.properties[strings.ToLower(key)] = val
}

// ListObjects returns the list of objects with their properties
func (a *Airbyte) ListObjects() (objs Objects, err error) {
	a.Catalog, err = a.Connector.Discover(a.config)
	if err != nil {
		err = g.Error(err, "could not get objects")
		return
	} else if len(a.Catalog.Streams) == 0 {
		err = g.Error("returned 0 objects")
		return
	}
	objs = make(Objects, len(a.Catalog.Streams))
	for i, stream := range a.Catalog.Streams {
		objs[i] = Object{
			Name:    stream.Name,
			Columns: stream.JsonSchema.Columns(),
			PK:      stream.SourceDefinedPrimaryKey,
		}
	}
	return
}

// GetObject returns the Object
func (a *Airbyte) GetObject(name string) (o Object, err error) {
	return
}

// Stream stream the object data
// needs work to provide the `state` for incremental reading
// there are 2 ways it seems
// - providing the `start_date` as part of the config
// - or providing the `state` object when reading.
// it doesn't look to be consistent. Github uses `state`, salesforce uses `start_date`
func (a *Airbyte) Stream(name string, startDate time.Time) (ds *iop.Datastream, err error) {
	stream := a.Catalog.GetStream(name)
	if stream.Name == "" {
		err = g.Error("no stream returned for " + name)
		return
	}
	syncMode := SyncModeIncremental
	if len(stream.SupportedSyncModes) > 0 {
		syncMode = stream.SupportedSyncModes[0]
	}

	catalog := ConfiguredAirbyteCatalog{
		Streams: []ConfiguredAirbyteStream{
			{
				Stream:              stream,
				SyncMode:            syncMode,
				DestinationSyncMode: DestinationSyncModeOverwrite,
			},
		},
	}

	if !startDate.IsZero() {
		a.config["start_date"] = startDate.Format("2006-01-02T15:04:05Z")
	}

	state := g.M() // TODO: how to store and retrieve / reconstruct state?
	ds, err = a.Connector.Read(a.config, catalog, state)
	if err != nil {
		err = g.Error(err, "could not read stream for "+name)
		return
	}
	return
}

// Objects is a list of objects
type Objects []Object

// Object is an object or endpoint
type Object struct {
	Name    string
	Columns iop.Columns
	PK      [][]string
}
