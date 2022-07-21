package airbyte

import (
	"context"
	"strings"

	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/samber/lo"
)

// Airbyte is for connections for Airbyte
type Airbyte struct {
	Context    *g.Context
	Connector  *Connector
	Catalog    AirbyteCatalog
	discovered bool
	properties map[string]string
	config     map[string]interface{}
}

type AirbyteOptions struct {
	Config     map[string]interface{}
	TempFolder string
	DateLayout string
	DateField  string
}

// NewAirbyteConnection creates a new airbyte connection object
func NewAirbyteConnection(name string, opts AirbyteOptions) (a *Airbyte, err error) {
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

	c.tempFolder = opts.TempFolder

	context := g.NewContext(context.Background())
	a = &Airbyte{
		Connector:  &c,
		config:     opts.Config,
		Context:    &context,
		properties: map[string]string{},
	}

	if opts.DateLayout == "" {
		opts.DateLayout = "YYYY-MM-DD"
	}
	if opts.DateField == "" {
		opts.DateField = "start_date"
	}

	a.SetProp("date_layout", opts.DateLayout)
	a.SetProp("date_field", opts.DateField)
	a.SetProp("temp_folder", opts.TempFolder)

	return
}

// Init initializes the airbyte connection
func (a *Airbyte) Init(check bool) (err error) {
	if check {
		status, err := a.Connector.Check(a.config)
		if err != nil {
			return g.Error(err, "could not check credentials")
		} else if status.Status == StatusFailed {
			return g.Error("failed credentials check: %s", status.Message)
		}
	}

	// check if already discovered
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

// Discover returns the list of streams with their properties
func (a *Airbyte) Discover() (streams AirbyteStreams, err error) {
	a.Catalog, err = a.Connector.Discover(a.config)
	if err != nil {
		err = g.Error(err, "could not get objects")
		return
	} else if len(a.Catalog.Streams) == 0 {
		err = g.Error("returned 0 stream")
		return
	}
	a.discovered = true
	streams = a.Catalog.Streams
	return
}

type StreamConfig struct {
	Columns    []string
	PrimaryKey []string
	SyncMode   SyncMode
	StartDate  string

	// TODO: how to store and retrieve / reconstruct state?
	State map[string]interface{}
}

func (a *Airbyte) GetConfiguredStream(name string, sc StreamConfig) (cas ConfiguredAirbyteStream, err error) {
	if !a.discovered {
		_, err = a.Discover()
		if err != nil {
			err = g.Error(err, "could discover streams")
			return
		}
	}

	stream := a.Catalog.GetStream(name)
	if stream.Name == "" {
		err = g.Error("no stream returned for " + name)
		return
	}

	if len(sc.Columns) > 0 {
		stream = stream.Select(sc.Columns)
	}

	primaryKey := []string{}
	if len(stream.SourceDefinedPrimaryKey) > 0 {
		primaryKey = stream.SourceDefinedPrimaryKey[0]
	}
	_ = primaryKey

	syncMode := SyncModeFullRefresh
	if lo.Contains(stream.SupportedSyncModes, sc.SyncMode) {
		syncMode = sc.SyncMode
	}

	cas = ConfiguredAirbyteStream{
		Stream:              stream,
		SyncMode:            syncMode,
		DestinationSyncMode: DestinationSyncModeOverwrite,
		// PrimaryKey:          primaryKey, // only use as destination
		// CursorField:         stream.DefaultCursorField, // only use as destination
	}

	return
}

// Stream stream the object data
// needs work to provide the `state` for incremental reading
// there are 2 ways it seems
// - providing the `start_date` as part of the config
// - or providing the `state` object when reading.
// it doesn't look to be consistent. Github uses `state`, salesforce uses `start_date`
func (a *Airbyte) Stream(name string, sc StreamConfig) (ds *iop.Datastream, err error) {
	cas, err := a.GetConfiguredStream(name, sc)
	if err != nil {
		err = g.Error(err, "could not get configured stream for "+name)
		return
	}

	catalog := ConfiguredAirbyteCatalog{
		Streams: []ConfiguredAirbyteStream{cas},
	}

	config := a.config
	if sc.StartDate != "" {
		config[a.GetProp("date_field")] = sc.StartDate
	}

	ds, err = a.Connector.Read(config, catalog, sc.State)
	if err != nil {
		err = g.Error(err, "could not read stream for "+name)
		return
	}
	return
}
