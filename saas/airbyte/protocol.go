package airbyte

import (
	"strings"

	"github.com/flarco/dbio/iop"

	"github.com/flarco/g"
)

// https://github.com/airbytehq/airbyte/blob/master/airbyte-protocol/models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml

// AirbyteMessage is the AirbyteMessage
type AirbyteMessage struct {
	Type             Type                    `json:"type"`
	Log              AirbyteLogMessage       `json:"log,omitempty"`
	Spec             ConnectorSpecification  `json:"spec,omitempty"`
	ConnectionStatus AirbyteConnectionStatus `json:"connectionStatus,omitempty"`
	Catalog          AirbyteCatalog          `json:"catalog,omitempty"`
	Record           AirbyteRecordMessage    `json:"record,omitempty"`
	State            AirbyteStateMessage     `json:"state,omitempty"`
}

// AirbyteMessages is a list of messages
type AirbyteMessages []AirbyteMessage

// First returns the first message of specified kind
func (msgs AirbyteMessages) First(t Type) (msg AirbyteMessage) {
	for _, msg = range msgs {
		if msg.Type == t {
			return msg
		}
	}
	return AirbyteMessage{}
}

// Type is for AirbyteMessage
type Type string

const TypeRecord Type = "RECORD"
const TypeState Type = "STATE"
const TypeLog Type = "LOG"
const TypeSpec Type = "SPEC"
const TypeConnectionStatus Type = "CONNECTION_STATUS"
const TypeCatalog Type = "CATALOG"

// AirbyteRecordMessage is the AirbyteRecordMessage
type AirbyteRecordMessage struct {
	Stream    string                 `json:"stream"`
	Data      map[string]interface{} `json:"data"`
	EmittedAt int64                  `json:"emitted_at"`
}

// AirbyteStateMessage is the AirbyteStateMessage
type AirbyteStateMessage struct {
	Data map[string]interface{} `json:"data"`
}

// AirbyteLogMessage is the AirbyteLogMessage
type AirbyteLogMessage struct {
	Level   Level  `json:"status"`
	Message string `json:"message"`
}

// Level is for AirbyteLogMessage
type Level string

const LevelFatal Level = "FATAL"
const LevelError Level = "ERROR"
const LevelWarn Level = "WARN"
const LevelInfo Level = "INFO"
const LevelDebug Level = "DEBUG"
const LevelTrace Level = "TRACE"

// AirbyteConnectionStatus is the Airbyte connection status
type AirbyteConnectionStatus struct {
	Status  Status `json:"status"`
	Message string `json:"message"`
}

// Status is for the AirbyteConnectionStatus
type Status string

const StatusSucceeded Status = "SUCCEEDED"
const StatusFailed Status = "FAILED"

// AirbyteCatalog is the Airbyte stream schema catalog
type AirbyteCatalog struct {
	Streams []AirbyteStream `json:"streams"`
}

// GetStream returns the stream by name
func (ac AirbyteCatalog) GetStream(name string) (s AirbyteStream) {
	for _, s = range ac.Streams {
		if strings.EqualFold(name, s.Name) {
			return s
		}
	}
	return AirbyteStream{}
}

// AirbyteStream is the AirbyteStream
type AirbyteStream struct {
	Name                    string           `json:"name"`
	JsonSchema              StreamJsonSchema `json:"json_schema"`
	SupportedSyncModes      []SyncMode       `json:"supported_sync_modes"`
	SourceDefinedCursor     bool             `json:"source_defined_cursor"`
	DefaultCursorField      []string         `json:"default_cursor_field"`
	SourceDefinedPrimaryKey []string         `json:"source_defined_primary_key"`
}

type StreamJsonSchema struct {
	AdditionalProperties bool                   `json:"additionalProperties"`
	Properties           map[string]interface{} `json:"properties"`
}

// Columns returns the properties as columns
func (sjs StreamJsonSchema) Columns() (cols iop.Columns) {
	cols = make(iop.Columns, len(sjs.Properties))
	i := 0
	for k := range sjs.Properties {
		cols[i] = iop.Column{
			Name: k,
			Type: "string",
		}
		i++
	}
	return
}

// ConfiguredAirbyteCatalog is the Airbyte stream schema catalog
type ConfiguredAirbyteCatalog struct {
	Streams []ConfiguredAirbyteStream `json:"streams"`
}

// ConfiguredAirbyteStream is the ConfiguredAirbyteStream
type ConfiguredAirbyteStream struct {
	Stream              AirbyteStream       `json:"stream"`
	SyncMode            SyncMode            `json:"sync_mode"`
	CursorField         []string            `json:"cursor_field"`
	DestinationSyncMode DestinationSyncMode `json:"destination_sync_mode"`
	PrimaryKey          []string            `json:"primary_key"`
}

// SyncMode is the SyncMode
type SyncMode string

const SyncModeFullRefresh SyncMode = "full_refresh"
const SyncModeIncremental SyncMode = "incremental"

// DestinationSyncMode is the DestinationSyncMode
type DestinationSyncMode string

const DestinationSyncModeAppend DestinationSyncMode = "append"
const DestinationSyncModeOverwrite DestinationSyncMode = "overwrite"
const DestinationSyncModeUpsertDedup DestinationSyncMode = "upsert_dedup"
const DestinationSyncModeAppendDedup DestinationSyncMode = "append_dedup"

// ConnectorSpecification is the Specification of a connector (source/destination)
type ConnectorSpecification struct {
	DocumentationUrl              string                  `json:"documentationUrl"`
	ChangelogUrl                  string                  `json:"changelogUrl"`
	ConnectionSpecification       ConnectionSpecification `json:"connectionSpecification"`
	SupportsIncremental           bool                    `json:"supportsIncremental"`
	SupportedDestinationSyncModes []DestinationSyncMode   `json:"supported_destination_sync_modes"`
}

type ConnectionSpecification struct {
	Title                string               `json:"title"`
	Type                 string               `json:"type"`
	AdditionalProperties bool                 `json:"additionalProperties"`
	Required             []string             `json:"required"`
	Properties           ConnectionProperties `json:"properties"`
}

type ConnectionProperties map[string]ConnectionProperty
type ConnectionProperty struct {
	Description   string        `json:"description"`
	AirbyteSecret bool          `json:"airbyte_secret"`
	Type          string        `json:"type"`
	Minimum       interface{}   `json:"minimum"`
	Maximum       interface{}   `json:"maximum"`
	Default       interface{}   `json:"default"`
	Examples      []interface{} `json:"examples"`
}

// Connectors is a list of Connector
type Connectors []Connector

// Names returns the Connector names
func (cs Connectors) Names() (n []string) {
	n = make([]string, len(cs))
	for i, c := range cs {
		n[i] = c.Definition.Name
	}
	return
}

// Get returns the Connector with the spec
func (cs Connectors) Get(name string) (c Connector, err error) {
	for _, c = range cs {
		if strings.EqualFold(name, c.Definition.Name) {
			err = c.GetSpec()
			if err != nil {
				err = g.Error(err, "could not get spec for ", name)
			}
			return c, err
		}
	}
	return c, g.Error("could not find connector: " + name)
}

// ConnectorDefinition is a connector information
// https://github.com/airbytehq/airbyte/blob/master/airbyte-config/init/src/main/resources/seed/source_definitions.yaml
type ConnectorDefinition struct {
	SourceDefinitionId string `yaml:"sourceDefinitionId"`
	Name               string `yaml:"name"`
	DockerRepository   string `yaml:"dockerRepository"`
	DockerImageTag     string `yaml:"dockerImageTag"`
	DocumentationUrl   string `yaml:"documentationUrl"`
}

// Image returns the docker image
func (cd ConnectorDefinition) Image() string {
	return cd.DockerRepository + ":" + cd.DockerImageTag
}
