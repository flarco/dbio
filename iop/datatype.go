package iop

import (
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

var (
	// RemoveTrailingDecZeros removes the trailing zeros in CastToString
	RemoveTrailingDecZeros = false
	SampleSize             = 900
)

// Column represents a schemata column
type Column struct {
	Position    int          `json:"position"`
	Name        string       `json:"name"`
	Type        ColumnType   `json:"type"`
	DbType      string       `json:"db_type,omitempty"`
	DbPrecision int          `json:"-"`
	DbScale     int          `json:"-"`
	Sourced     bool         `json:"-"` // whether is was sourced from a typed source
	Stats       ColumnStats  `json:"stats,omitempty"`
	goType      reflect.Type `json:"-"`

	Table    string `json:"table,omitempty"`
	Schema   string `json:"schema,omitempty"`
	Database string `json:"database,omitempty"`
}

// Columns represent many columns
type Columns []Column

type ColumnType string

const (
	BigIntType     ColumnType = "bigint"
	BinaryType     ColumnType = "binary"
	BoolType       ColumnType = "bool"
	DateType       ColumnType = "date"
	DatetimeType   ColumnType = "datetime"
	DecimalType    ColumnType = "decimal"
	IntegerType    ColumnType = "integer"
	JsonType       ColumnType = "json"
	SmallIntType   ColumnType = "smallint"
	StringType     ColumnType = "string"
	TextType       ColumnType = "text"
	TimestampType  ColumnType = "timestamp"
	TimestampzType ColumnType = "timestampz"
	FloatType      ColumnType = "float"
	TimeType       ColumnType = "time"
	TimezType      ColumnType = "timez"
)

// ColumnStats holds statistics for a column
type ColumnStats struct {
	MinLen    int    `json:"min_len,omitempty"`
	MaxLen    int    `json:"max_len,omitempty"`
	MaxDecLen int    `json:"max_dec_len,omitempty"`
	Min       int64  `json:"min"`
	Max       int64  `json:"max"`
	NullCnt   int64  `json:"null_cnt"`
	IntCnt    int64  `json:"int_cnt,omitempty"`
	DecCnt    int64  `json:"dec_cnt,omitempty"`
	BoolCnt   int64  `json:"bool_cnt,omitempty"`
	JsonCnt   int64  `json:"json_cnt,omitempty"`
	StringCnt int64  `json:"string_cnt,omitempty"`
	DateCnt   int64  `json:"date_cnt,omitempty"`
	TotalCnt  int64  `json:"total_cnt"`
	UniqCnt   int64  `json:"uniq_cnt"`
	Checksum  uint64 `json:"checksum"`
}

func (cs *ColumnStats) DistinctPercent() float64 {
	val := (cs.UniqCnt) * 100 / cs.TotalCnt
	return cast.ToFloat64(val) / 100
}

func (cs *ColumnStats) DuplicateCount() int64 {
	return cs.TotalCnt - cs.UniqCnt
}

func (cs *ColumnStats) DuplicatePercent() float64 {
	val := (cs.TotalCnt - cs.UniqCnt) * 100 / cs.TotalCnt
	return cast.ToFloat64(val) / 100
}

func init() {
	if os.Getenv("SAMPLE_SIZE") != "" {
		SampleSize = cast.ToInt(os.Getenv("SAMPLE_SIZE"))
	}
	if os.Getenv("REMOVE_TRAILING_ZEROS") != "" {
		RemoveTrailingDecZeros = cast.ToBool(os.Getenv("REMOVE_TRAILING_ZEROS"))
	}
}

// Row is a row
func Row(vals ...interface{}) []interface{} {
	return vals
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
func (cols Columns) Clone() (newCols Columns) {
	newCols = make(Columns, len(cols))
	for j, col := range cols {
		newCols[j] = Column{
			Position:    col.Position,
			Name:        col.Name,
			Type:        col.Type,
			DbType:      col.DbType,
			DbPrecision: col.DbPrecision,
			DbScale:     col.DbScale,
			Sourced:     col.Sourced,
			Stats:       col.Stats,
			goType:      col.goType,
			Table:       col.Table,
			Schema:      col.Schema,
			Database:    col.Database,
		}
	}
	return newCols
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
func CompareColumns(columns1 []Column, columns2 []Column) (reshape bool, err error) {
	if len(columns1) != len(columns2) {
		return reshape, g.Error("columns mismatch: %d fields != %d fields", len(columns1), len(columns2))
	}

	eG := g.ErrorGroup{}
	col2Map := lo.KeyBy(columns2, func(c Column) string { return strings.ToLower(c.Name) })
	for i, c1 := range columns1 {
		c2 := columns2[i]

		_, found := col2Map[strings.ToLower(c1.Name)]
		if c1.Name != c2.Name {
			if found {
				// sometimes the orders of columns is different
				// (especially, multiple json files), shape ds to match columns1
				reshape = true
			} else {
				eG.Add(g.Error("column name mismatch: %s (%s) != %s (%s)", c1.Name, c1.Type, c2.Name, c2.Type))
			}
		} else if c1.Type != c2.Type {
			// too unpredictable to mark as error? sometimes one column
			// has not enough data to represent true type. Warn instead
			// eG.Add(g.Error("type mismatch: %s (%s) != %s (%s)", c1.Name, c1.Type, c2.Name, c2.Type))
			g.Warn("type mismatch: %s (%s) != %s (%s)", c1.Name, c1.Type, c2.Name, c2.Type)
		}
	}
	return reshape, eG.Err()
}

// SyncColumns syncs two columns together
func SyncColumns(columns1 []Column, columns2 []Column) (columns []Column, err error) {
	if _, err = CompareColumns(columns1, columns2); err != nil {
		err = g.Error(err)
		return
	}

	columns = columns1

	for i := range columns2 {
		columns[i].Stats.TotalCnt += columns2[i].Stats.TotalCnt
		columns[i].Stats.NullCnt += columns2[i].Stats.NullCnt
		columns[i].Stats.StringCnt += columns2[i].Stats.StringCnt
		columns[i].Stats.JsonCnt += columns2[i].Stats.JsonCnt
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

// InferFromStats using the stats to infer data types
func InferFromStats(columns []Column, safe bool, noTrace bool) []Column {
	for j := range columns {
		if columns[j].Stats.TotalCnt == 0 {
			// do nothing
		} else if columns[j].Stats.StringCnt > 0 || columns[j].Stats.NullCnt == columns[j].Stats.TotalCnt {
			if columns[j].Stats.MaxLen > 255 {
				columns[j].Type = TextType
			} else {
				columns[j].Type = StringType
			}
			if safe {
				columns[j].Type = TextType // max out
			}
			columns[j].goType = reflect.TypeOf("string")

			columns[j].Stats.Min = 0
			if columns[j].Stats.NullCnt == columns[j].Stats.TotalCnt {
				columns[j].Stats.MinLen = 0
			}
		} else if columns[j].Stats.JsonCnt+columns[j].Stats.NullCnt == columns[j].Stats.TotalCnt {
			columns[j].Type = JsonType
			columns[j].goType = reflect.TypeOf("json")
		} else if columns[j].Stats.BoolCnt+columns[j].Stats.NullCnt == columns[j].Stats.TotalCnt {
			columns[j].Type = BoolType
			columns[j].goType = reflect.TypeOf(true)
			columns[j].Stats.Min = 0
		} else if columns[j].Stats.IntCnt+columns[j].Stats.NullCnt == columns[j].Stats.TotalCnt {
			if columns[j].Stats.Min*10 < -2147483648 || columns[j].Stats.Max*10 > 2147483647 {
				columns[j].Type = BigIntType
			} else {
				columns[j].Type = IntegerType
			}
			if safe {
				columns[j].Type = BigIntType // max out
			}
			columns[j].goType = reflect.TypeOf(int64(0))
		} else if columns[j].Stats.DateCnt+columns[j].Stats.NullCnt == columns[j].Stats.TotalCnt {
			columns[j].Type = DatetimeType
			columns[j].goType = reflect.TypeOf(time.Now())
			columns[j].Stats.Min = 0
		} else if columns[j].Stats.DecCnt+columns[j].Stats.IntCnt+columns[j].Stats.NullCnt == columns[j].Stats.TotalCnt {
			columns[j].Type = DecimalType
			columns[j].goType = reflect.TypeOf(float64(0.0))
		}
		if !noTrace {
			g.Trace("%s - %s %s", columns[j].Name, columns[j].Type, g.Marshal(columns[j].Stats))
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

func (col *Column) Key() string {
	return col.Schema + "." + col.Table + "." + col.Name
}

func (col *Column) IsUnique() bool {
	if col.Stats.TotalCnt <= 0 {
		return false
	}
	return col.Stats.TotalCnt == col.Stats.UniqCnt
}

// IsString returns whether the column is a string
func (col *Column) IsString() bool {
	return col.Type.IsString()
}

// IsInteger returns whether the column is an integer
func (col *Column) IsInteger() bool {
	return col.Type.IsInteger()
}

// IsDecimal returns whether the column is a decimal
func (col *Column) IsDecimal() bool {
	return col.Type.IsDecimal()
}

// IsNumber returns whether the column is a decimal or an integer
func (col *Column) IsNumber() bool {
	return col.Type.IsNumber()
}

// IsBool returns whether the column is a boolean
func (col *Column) IsBool() bool {
	return col.Type.IsBool()
}

// IsDatetime returns whether the column is a datetime object
func (col *Column) IsDatetime() bool {
	return col.Type.IsDatetime()
}

// IsString returns whether the column is a string
func (ct ColumnType) IsString() bool {
	switch ct {
	case StringType, TextType, JsonType, TimeType, BinaryType, "":
		return true
	}
	return false
}

// IsInteger returns whether the column is an integer
func (ct ColumnType) IsInteger() bool {
	switch ct {
	case IntegerType, BigIntType, SmallIntType:
		return true
	}
	return false
}

// IsDecimal returns whether the column is a decimal
func (ct ColumnType) IsDecimal() bool {
	return ct == FloatType || ct == DecimalType
}

// IsNumber returns whether the column is a decimal or an integer
func (ct ColumnType) IsNumber() bool {
	return ct.IsInteger() || ct.IsDecimal()
}

// IsBool returns whether the column is a boolean
func (ct ColumnType) IsBool() bool {
	return ct == BoolType
}

// IsDatetime returns whether the column is a datetime object
func (ct ColumnType) IsDatetime() bool {
	switch ct {
	case DatetimeType, DateType, TimestampType, TimestampzType:
		return true
	}
	return false
}
