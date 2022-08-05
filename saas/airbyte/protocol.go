package airbyte

import (
	"sort"
	"strings"

	"github.com/samber/lo"
	"github.com/spf13/cast"

	"github.com/flarco/dbio/iop"

	"github.com/flarco/g"
)

// https://docs.airbyte.io/architecture/airbyte-specification
// https://github.com/airbytehq/airbyte/blob/master/airbyte-protocol/models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml

// AirbyteMessage is the AirbyteMessage
type AirbyteMessage struct {
	Type             Type                     `json:"type"`
	Log              *AirbyteLogMessage       `json:"log,omitempty"`
	Trace            *AirbyteTraceMessage     `json:"trace,omitempty"`
	Spec             *ConnectorSpecification  `json:"spec,omitempty"`
	ConnectionStatus *AirbyteConnectionStatus `json:"connectionStatus,omitempty"`
	Catalog          *AirbyteCatalog          `json:"catalog,omitempty"`
	Record           *AirbyteRecordMessage    `json:"record,omitempty"`
	State            *AirbyteStateMessage     `json:"state,omitempty"`
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

// First returns the first message of specified kind
func (msgs AirbyteMessages) CheckError() (err error) {

	for _, msg := range msgs {
		if err := msg.CheckError(); err != nil {
			return g.Error(err)
		}
	}

	return nil
}

// First returns the first message of specified kind
func (msg AirbyteMessage) CheckError() (err error) {

	if msg.Log != nil && g.In(msg.Log.Level, LevelFatal, LevelError) {
		// format message
		errMsg := cast.ToString(msg.Log.Message)
		g.Debug(errMsg) // put full error in log

		errMsgLines := strings.Split(cast.ToString(msg.Log.Message), "\n")
		if len(errMsgLines) > 1 {
			errMsg = errMsgLines[len(errMsgLines)-1] // take last line
		}
		return g.Error(errMsg)
	}

	if msg.Trace != nil && g.In(msg.Trace.Type, LevelFatal, LevelError) {
		errMsg := g.Marshal(msg.Trace.Error)
		g.Debug(errMsg) // put full error in log
		return g.Error(errMsg)
	}
	return nil
}

// Type is for AirbyteMessage
type Type string

const TypeRecord Type = "RECORD"
const TypeState Type = "STATE"
const TypeTrace Type = "TRACE"
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
	Level   Level       `json:"level"`
	Message interface{} `json:"message"`
}

// AirbyteTraceMessage is the AirbyteTraceMessage
type AirbyteTraceMessage struct {
	Type      Level                  `json:"type"`
	EmittedAt float64                `json:"emitted_at"`
	Error     map[string]interface{} `json:"error"`
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
	Streams AirbyteStreams `json:"streams"`
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

// AirbyteStreams is a list of AirbyteStream
type AirbyteStreams []AirbyteStream

// Names returns the stream names
func (ass AirbyteStreams) Names() []string {
	names := make([]string, len(ass))
	for i, s := range ass {
		names[i] = s.Name
	}
	return names
}

// AirbyteStream is the AirbyteStream
type AirbyteStream struct {
	Name                    string           `json:"name"`
	JsonSchema              StreamJsonSchema `json:"json_schema"`
	SupportedSyncModes      []SyncMode       `json:"supported_sync_modes"`
	SourceDefinedCursor     bool             `json:"source_defined_cursor"`
	DefaultCursorField      []string         `json:"default_cursor_field"`
	SourceDefinedPrimaryKey [][]string       `json:"source_defined_primary_key"`
}

// Columns returns the stream columns
func (as AirbyteStream) Columns() iop.Columns {
	return as.JsonSchema.Columns()
}

// Select returns the stream with columns selected
func (as AirbyteStream) Select(columns []string) (nas AirbyteStream) {
	nas = as
	nas.JsonSchema.Properties = map[string]interface{}{}

	columnsMap := lo.KeyBy(columns, func(c string) string {
		return strings.ToLower(c)
	})

	for k, v := range as.JsonSchema.Properties {
		if _, ok := columnsMap[strings.ToLower(k)]; ok {
			nas.JsonSchema.Properties[k] = v
		}
	}

	return nas
}

type StreamJsonSchema struct {
	AdditionalProperties bool                   `json:"additionalProperties"`
	Properties           map[string]interface{} `json:"properties"`
}

var typeMap = map[string]string{
	"string":  "string",
	"integer": "integer",
	"number":  "decimal",
	"boolean": "bool",
	"object":  "json",
	"array":   "json",
}

// Columns returns the properties as columns
func (sjs StreamJsonSchema) Columns() (cols iop.Columns) {
	cols = make(iop.Columns, len(sjs.Properties))
	i := 0
	for k, val := range sjs.Properties {
		ct := ""
		// g.P(val)
		valM, ok := val.(map[string]interface{})
		if ok {
			switch t := valM["type"].(type) {
			case []interface{}:
				for _, v := range valM["type"].([]interface{}) {
					if !strings.EqualFold(cast.ToString(v), "null") {
						ct = cast.ToString(v)
						break
					}
				}
			default:
				_ = t
				ct = cast.ToString(valM["type"])
			}
		}

		if ct == "" {
			ct = "string"
		} else {
			ct2, ok := typeMap[ct]
			if ok {
				ct = ct2
			} else {
				g.Warn("did not find type in typeMap: %s", ct)
				ct = "string"
			}
		}
		cols[i] = iop.Column{
			Name: k,
			Type: iop.ColumnType(ct),
		}
		i++
	}

	sort.Slice(cols, func(i, j int) bool { return cols[i].Name < cols[j].Name })
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
	DocumentationUrl              string                  `json:"documentationUrl" yaml:"documentationUrl"`
	ChangelogUrl                  string                  `json:"changelogUrl" yaml:"changelogUrl"`
	ConnectionSpecification       ConnectionSpecification `json:"connectionSpecification" yaml:"connectionSpecification"`
	SupportsIncremental           bool                    `json:"supportsIncremental" yaml:"supportsIncremental"`
	SupportedDestinationSyncModes []DestinationSyncMode   `json:"supported_destination_sync_modes" yaml:"supported_destination_sync_modes"`
}

type ConnectionSpecification struct {
	Name                 string               `json:"name,omitempty" yaml:"name,omitempty"`
	Title                string               `json:"title" yaml:"title"`
	Type                 string               `json:"type" yaml:"type"`
	AdditionalProperties bool                 `json:"additionalProperties" yaml:"additionalProperties"`
	Required             []string             `json:"required" yaml:"required"`
	Properties           ConnectionProperties `json:"properties" yaml:"properties"`
}

type ConnectionProperties map[string]ConnectionProperty
type ConnectionProperty struct {
	Title         string                    `json:"title,omitempty" yaml:"title,omitempty"`
	Const         string                    `json:"const,omitempty" yaml:"const,omitempty"`
	Description   string                    `json:"description,omitempty" yaml:"description,omitempty"`
	AirbyteSecret bool                      `json:"airbyte_secret,omitempty" yaml:"airbyte_secret,omitempty"`
	Type          string                    `json:"type,omitempty" yaml:"type,omitempty"`
	Order         *int                      `json:"order,omitempty" yaml:"order,omitempty"`
	OneOf         []ConnectionSpecification `json:"oneOf,omitempty" yaml:"oneOf,omitempty"`
	Minimum       interface{}               `json:"minimum,omitempty" yaml:"minimum,omitempty"`
	Maximum       interface{}               `json:"maximum,omitempty" yaml:"maximum,omitempty"`
	Default       interface{}               `json:"default,omitempty" yaml:"default,omitempty"`
	Examples      []interface{}             `json:"examples,omitempty" yaml:"examples,omitempty"`
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
func (cs Connectors) Get(key string) (c Connector, err error) {
	for _, c = range cs {
		if strings.EqualFold(key, c.Key()) {

			err = c.Pull()
			g.LogError(err, "could not pull image: "+c.Definition.Image())

			err = c.GetSpec()
			if err != nil {
				err = g.Error(err, "could not get spec for %s", key)
			}
			return c, err
		}
	}
	return c, g.Error("could not find connector: %s", key)
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

// GetAirbyteSpecs returns the key to specs map
func GetAirbyteSpecs() (abs map[string]ConnectionSpecification, err error) {
	abs = map[string]ConnectionSpecification{}
	connectors, err := GetSourceConnectors(false)
	if err != nil {
		err = g.Error(err, "could not get airbyte source connectors")
		return
	}

	for _, c := range connectors {
		abs[c.Key()] = c.Specification.ConnectionSpecification
	}
	return
}
