package iop

import (
	"database/sql"
	"math"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/spf13/cast"
)

var (
	// ShowProgress use the progress bar to show progress
	ShowProgress = false

	// RemoveTrailingDecZeros removes the trailing zeros in CastToString
	RemoveTrailingDecZeros = false
	SampleSize             = 900
)

// Column represents a schemata column
type Column struct {
	Position    int    `json:"position"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	DbType      string `json:"db_type"`
	DbPrecision int    `json:"db_precision"`
	DbScale     int    `json:"db_scale"`
	Sourced     bool   `json:"sourced"` // whether is was sourced from a typed source
	Stats       ColumnStats
	ColType     *sql.ColumnType
	goType      reflect.Type
}

// Columns represent many columns
type Columns []Column

// ColumnStats holds statistics for a column
type ColumnStats struct {
	MinLen    int
	MaxLen    int
	MaxDecLen int
	Min       int64
	Max       int64
	NullCnt   int64
	IntCnt    int64
	DecCnt    int64
	BoolCnt   int64
	StringCnt int64
	DateCnt   int64
	TotalCnt  int64
	Checksum  uint64
}

func init() {
	if os.Getenv("DBIO_SAMPLE_SIZE") != "" {
		SampleSize = cast.ToInt(os.Getenv("DBIO_SAMPLE_SIZE"))
	}
	if os.Getenv("DBIO_REMOVE_TRAILING_ZEROS") != "" {
		RemoveTrailingDecZeros = cast.ToBool(os.Getenv("DBIO_REMOVE_TRAILING_ZEROS"))
	}
}

// Row is a row
func Row(vals ...interface{}) []interface{} {
	return vals
}

//NewStreamProcessor returns a new StreamProcessor
func NewStreamProcessor() *StreamProcessor {
	sp := StreamProcessor{
		stringTypeCache: map[int]string{},
		colStats:        map[int]*ColumnStats{},
		decReplRegex:    regexp.MustCompile(`^(\d*[\d.]*?)\.?0*$`),
		config:          &streamConfig{delimiter: ",", emptyAsNull: true, maxDecimals: -1},
	}
	if os.Getenv("MAX_DECIMALS") != "" {
		sp.config.maxDecimals = cast.ToFloat64(math.Pow10(cast.ToInt(os.Getenv("MAX_DECIMALS"))))
	}
	sp.parseFuncs = map[string]func(s string) (interface{}, error){
		"int": func(s string) (interface{}, error) {
			return strconv.ParseInt(s, 10, 64)
		},
		"float": func(s string) (interface{}, error) {
			return strconv.ParseFloat(s, 64)
		},
		"time": func(s string) (interface{}, error) {
			return sp.ParseTime(s)
		},
		"bool": func(s string) (interface{}, error) {
			return cast.ToBoolE(s)
		},
	}
	sp.dateLayouts = []string{
		"2006-01-02",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.000",
		"2006-01-02T15:04:05.000Z",
		"02-Jan-06",
		"02-Jan-06 15:04:05",
		"02-Jan-06 03:04:05 PM",
		"02-Jan-06 03.04.05.000000 PM",
		"2006-01-02T15:04:05-0700",
		time.RFC3339,
		"2006-01-02T15:04:05",  // iso8601 without timezone
		"2006-01-02T15:04:05Z", // iso8601 with timezone
		time.RFC1123Z,
		time.RFC1123,
		time.RFC822Z,
		time.RFC822,
		time.RFC850,
		time.ANSIC,
		time.UnixDate,
		time.RubyDate,
		"2006-01-02 15:04:05.999999999 -0700 MST", // Time.String()
		"02 Jan 2006",
		"2006-01-02T15:04:05-0700", // RFC3339 without timezone hh:mm colon
		"2006-01-02 15:04:05 -07:00",
		"2006-01-02 15:04:05 -0700",
		"2006-01-02 15:04:05Z07:00", // RFC3339 without T
		"2006-01-02 15:04:05Z0700",  // RFC3339 without T or timezone hh:mm colon
		"2006-01-02 15:04:05 MST",
		time.Kitchen,
		time.Stamp,
		time.StampMilli,
		time.StampMicro,
		time.StampNano,
		"1/2/06",
		"01/02/06",
		"1/2/2006",
		"01/02/2006",
		"01/02/2006 15:04",
		"01/02/2006 15:04:05",
		"01/02/2006 03:04:05 PM", // "8/17/1994 12:00:00 AM"
		"01/02/2006 03:04:05 PM", // "8/17/1994 12:00:00 AM"
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02T15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04",
		"2006-01-02T15:04",
		"2006/01/02 15:04:05",
	}
	return &sp
}

// IsDummy returns true if the columns are injected by CreateDummyFields
func IsDummy(columns []Column) bool {
	return Columns(columns).IsDummy()
}

// NewColumnsFromFields creates Columns from fields
func NewColumnsFromFields(fields ...string) (cols Columns) {
	cols = make(Columns, len(fields))
	for i, field := range fields {
		cols[i].Name = field
		cols[i].Position = i + 1
	}
	return
}

// IsDummy returns true if the columns are injected by CreateDummyFields
func (cols Columns) IsDummy() bool {
	for _, col := range cols {
		if !strings.HasPrefix(col.Name, "col_") || len(col.Name) != 8 {
			return false
		}
	}
	return true
}

// Names return the column names
func (cols Columns) Names() []string {
	fields := make([]string, len(cols))
	for j, column := range cols {
		fields[j] = column.Name
	}
	return fields
}

// FieldMap return the fields map of indexes
// when `toLower` is true, field keys are lower cased
func (cols Columns) FieldMap(toLower bool) map[string]int {
	fieldColIDMap := map[string]int{}
	for i, col := range cols {
		if toLower {
			fieldColIDMap[strings.ToLower(col.Name)] = i
		} else {
			fieldColIDMap[col.Name] = i
		}
	}
	return fieldColIDMap
}

// Dataset return an empty inferred dataset
func (cols Columns) Dataset() Dataset {
	d := NewDataset(cols)
	d.Inferred = true
	return d
}

// GetColumn returns the matched Col
func (cols Columns) GetColumn(name string) Column {
	colsMap := map[string]Column{}
	for _, col := range cols {
		colsMap[strings.ToLower(col.Name)] = col
	}
	return colsMap[strings.ToLower(name)]
}

// Normalize returns the normalized field name
func (cols Columns) Normalize(name string) string {
	return cols.GetColumn(name).Name
}

// CompareColumns compared two columns to see if there are similar
func CompareColumns(columns1 []Column, columns2 []Column) (err error) {
	if len(columns1) != len(columns2) {
		return g.Error("columns mismatch: %d fields != %d fields", len(columns1), len(columns2))
	}

	eG := g.ErrorGroup{}
	for i, c1 := range columns1 {
		c2 := columns2[i]

		if c1.Type != c2.Type {
			// too unpredictable to mark as error? sometimes one column
			// has not enough data to represent true type. Warn instead
			// eG.Add(g.Error("type mismatch: %s (%s) != %s (%s)", c1.Name, c1.Type, c2.Name, c2.Type))
			g.Warn("type mismatch: %s (%s) != %s (%s)", c1.Name, c1.Type, c2.Name, c2.Type)
		}
	}
	return eG.Err()
}

// SyncColumns syncs two columns together
func SyncColumns(columns1 []Column, columns2 []Column) (columns []Column, err error) {
	if err = CompareColumns(columns1, columns2); err != nil {
		err = g.Error(err)
		return
	}

	columns = columns1

	for i := range columns2 {
		columns[i].Stats.TotalCnt += columns2[i].Stats.TotalCnt
		columns[i].Stats.NullCnt += columns2[i].Stats.NullCnt
		columns[i].Stats.StringCnt += columns2[i].Stats.StringCnt
		columns[i].Stats.IntCnt += columns2[i].Stats.IntCnt
		columns[i].Stats.DecCnt += columns2[i].Stats.DecCnt
		columns[i].Stats.BoolCnt += columns2[i].Stats.BoolCnt
		columns[i].Stats.DateCnt += columns2[i].Stats.DateCnt

		if columns[i].Stats.Min < columns2[i].Stats.Min {
			columns[i].Stats.Min = columns2[i].Stats.Min
		}
		if columns[i].Stats.Max > columns2[i].Stats.Max {
			columns[i].Stats.Max = columns2[i].Stats.Max
		}
		if columns[i].Stats.MaxLen > columns2[i].Stats.MaxLen {
			columns[i].Stats.MaxLen = columns2[i].Stats.MaxLen
		}
		if columns[i].Stats.MaxDecLen > columns2[i].Stats.MaxDecLen {
			columns[i].Stats.MaxDecLen = columns2[i].Stats.MaxDecLen
		}
	}
	return
}

// MakeColumns makes columns from a struct
func MakeColumns(obj interface{}, useTag string, typeMap ...map[string]string) Columns {
	TypeMap := map[string]string{
		"string":    "string",
		"bool":      "boolean",
		"g.Map":     "json",
		"int":       "integer",
		"int64":     "bigint",
		"float32":   "float",
		"float64":   "float",
		"time.Time": "datetime",
	}
	if len(typeMap) > 0 {
		for k, v := range typeMap[0] {
			TypeMap[k] = v
		}
	}

	cols := Columns{}

	var t reflect.Type
	value := reflect.ValueOf(obj)
	if value.Kind() == reflect.Ptr {
		t = reflect.Indirect(value).Type()
	} else {
		t = reflect.TypeOf(obj)
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fType := field.Type.String()
		mappedType, ok := TypeMap[fType]
		switch {
		case ok:
		case strings.HasPrefix(fType, "int"):
			mappedType = "integer"
		case strings.HasPrefix(fType, "map["):
			mappedType = "json"
		default:
			g.Warn("could not map type '%s' using iop.MakeColumns", fType)
			mappedType = "string"
		}

		col := Column{Name: field.Name, Type: mappedType}
		if useTag != "" {
			tagName := field.Tag.Get(useTag)
			if tagName != "" && tagName != "-" {
				col.Name = tagName
				cols = append(cols, col)
			}
		} else {
			cols = append(cols, col)
		}
	}

	return cols
}

// InferFromStats using the stats to infer data types
func InferFromStats(columns []Column, safe bool, noTrace bool) []Column {
	for j := range columns {
		if columns[j].Stats.StringCnt > 0 || columns[j].Stats.NullCnt == columns[j].Stats.TotalCnt {
			if columns[j].Stats.MaxLen > 255 {
				columns[j].Type = "text"
			} else {
				columns[j].Type = "string"
			}
			if safe {
				columns[j].Type = "text" // max out
			}
			columns[j].goType = reflect.TypeOf("string")
		} else if columns[j].Stats.BoolCnt+columns[j].Stats.NullCnt == columns[j].Stats.TotalCnt {
			columns[j].Type = "bool"
			columns[j].goType = reflect.TypeOf(true)
		} else if columns[j].Stats.IntCnt+columns[j].Stats.NullCnt == columns[j].Stats.TotalCnt {
			if columns[j].Stats.Min*10 < -2147483648 || columns[j].Stats.Max*10 > 2147483647 {
				columns[j].Type = "bigint"
			} else {
				columns[j].Type = "integer"
			}
			if safe {
				columns[j].Type = "bigint" // max out
			}
			columns[j].goType = reflect.TypeOf(int64(0))
		} else if columns[j].Stats.DateCnt+columns[j].Stats.NullCnt == columns[j].Stats.TotalCnt {
			columns[j].Type = "datetime"
			columns[j].goType = reflect.TypeOf(time.Now())
		} else if columns[j].Stats.DecCnt+columns[j].Stats.IntCnt+columns[j].Stats.NullCnt == columns[j].Stats.TotalCnt {
			columns[j].Type = "decimal"
			columns[j].goType = reflect.TypeOf(float64(0.0))
		}
		if !noTrace {
			g.Trace(
				"%s - %s (maxLen: %d, nullCnt: %d, totCnt: %d, strCnt: %d, dtCnt: %d, intCnt: %d, decCnt: %d)",
				columns[j].Name, columns[j].Type,
				columns[j].Stats.MaxLen, columns[j].Stats.NullCnt,
				columns[j].Stats.TotalCnt, columns[j].Stats.StringCnt,
				columns[j].Stats.DateCnt, columns[j].Stats.IntCnt,
				columns[j].Stats.DecCnt,
			)
		}
	}
	return columns
}

// MakeDataFlow create a dataflow from datastreams
func MakeDataFlow(dss ...*Datastream) (df *Dataflow, err error) {

	if len(dss) == 0 {
		err = g.Error("Provided 0 datastreams for: %#v", dss)
		return
	}

	df = NewDataflow()

	go func() {
		defer df.Close()
		df.PushStreams(dss...)
	}()

	// wait for first ds to start streaming.
	// columns need to be populated
	err = df.WaitReady()
	if err != nil {
		return df, err
	}

	return df, nil
}

// MakeRowsChan returns a buffered channel with default size
func MakeRowsChan() chan []interface{} {
	return make(chan []interface{})
}

// IsString returns whether the column is a string
func (col *Column) IsString() bool {
	switch col.Type {
	case "string", "text", "json", "time", "bytes", "":
		return true
	}
	return false
}

// IsInteger returns whether the column is an integer
func (col *Column) IsInteger() bool {
	return col.Type == "smallint" || col.Type == "bigint" || col.Type == "integer"
}

// IsDecimal returns whether the column is a decimal
func (col *Column) IsDecimal() bool {
	return col.Type == "float" || col.Type == "decimal"
}

// IsNumber returns whether the column is a decimal or an integer
func (col *Column) IsNumber() bool {
	return col.IsInteger() || col.IsDecimal()
}

// IsBool returns whether the column is a boolean
func (col *Column) IsBool() bool {
	return col.Type == "bool"
}

// IsDatetime returns whether the column is a datetime object
func (col *Column) IsDatetime() bool {
	return col.Type == "datetime" || col.Type == "date" || col.Type == "timestamp"
}
