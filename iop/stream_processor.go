package iop

import (
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/godror/godror"
	"github.com/spf13/cast"
)

// StreamProcessor processes rows and values
type StreamProcessor struct {
	N                uint64
	dateLayoutCache  string
	stringTypeCache  map[int]string
	colStats         map[int]*ColumnStats
	unrecognizedDate string
	warn             bool
	parseFuncs       map[string]func(s string) (interface{}, error)
	decReplRegex     *regexp.Regexp
	ds               *Datastream
	dateLayouts      []string
	config           *streamConfig
	rowBlankValCnt   int
}

type streamConfig struct {
	trimSpace      bool
	emptyAsNull    bool
	header         bool
	compression    string // AUTO | ZIP | GZIP | SNAPPY | NONE
	nullIf         string
	datetimeFormat string
	skipBlankLines bool
	delimiter      string
	fileMaxRows    int64
	maxDecimals    float64
}

// SetConfig sets the data.Sp.config values
func (sp *StreamProcessor) SetConfig(configMap map[string]string) {
	if sp == nil {
		sp = NewStreamProcessor()
	}

	if configMap["delimiter"] != "" {
		sp.config.delimiter = cast.ToString(configMap["delimiter"])
	} else {
		sp.config.delimiter = ","
	}

	if configMap["file_max_rows"] != "" {
		sp.config.fileMaxRows = cast.ToInt64(configMap["file_max_rows"])
	}

	if configMap["header"] != "" {
		sp.config.header = cast.ToBool(configMap["header"])
	} else {
		sp.config.header = true
	}

	if configMap["empty_field_as_null"] != "" {
		sp.config.emptyAsNull = cast.ToBool(configMap["empty_field_as_null"])
	}
	if configMap["null_if"] != "" {
		sp.config.nullIf = configMap["null_if"]
	}
	if configMap["trim_space"] != "" {
		sp.config.trimSpace = cast.ToBool(configMap["trim_space"])
	}
	if configMap["skip_blank_lines"] != "" {
		sp.config.skipBlankLines = cast.ToBool(configMap["skip_blank_lines"])
	}
	sp.config.compression = configMap["compression"]

	if configMap["datetime_format"] != "" {
		sp.config.datetimeFormat = iso8601ToGoLayout(configMap["datetime_format"])
		// put in first
		sp.dateLayouts = append(
			[]string{sp.config.datetimeFormat},
			sp.dateLayouts...)
	}
}

// CastVal  casts the type of an interface based on its value
// From html/template/content.go
// Copyright 2011 The Go Authors. All rights reserved.
// indirect returns the value, after dereferencing as many times
// as necessary to reach the base type (or nil).
func (sp *StreamProcessor) indirect(a interface{}) interface{} {
	if a == nil {
		return nil
	}
	if t := reflect.TypeOf(a); t.Kind() != reflect.Ptr {
		// Avoid creating a reflect.Value if it's not a pointer.
		return a
	}
	v := reflect.ValueOf(a)
	for v.Kind() == reflect.Ptr && !v.IsNil() {
		v = v.Elem()
	}
	return v.Interface()
}

func (sp *StreamProcessor) toFloat64E(i interface{}) (float64, error) {
	i = sp.indirect(i)

	switch s := i.(type) {
	case float64:
		return s, nil
	case float32:
		return float64(s), nil
	case int:
		return float64(s), nil
	case int64:
		return float64(s), nil
	case int32:
		return float64(s), nil
	case int16:
		return float64(s), nil
	case int8:
		return float64(s), nil
	case uint:
		return float64(s), nil
	case uint64:
		return float64(s), nil
	case uint32:
		return float64(s), nil
	case uint16:
		return float64(s), nil
	case uint8:
		return float64(s), nil
	case []uint8:
		return cast.ToFloat64(cast.ToString(s)), nil
	case string:
		v, err := strconv.ParseFloat(s, 64)
		if err == nil {
			return v, nil
		}
		return 0, g.Error("unable to cast %#v of type %T to float64", i, i)
	case bool:
		if s {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, g.Error("unable to cast %#v of type %T to float64", i, i)
	}
}

// CastType casts the type of an interface
// CastType is used to cast the interface place holders?
func (sp *StreamProcessor) CastType(val interface{}, typ string) interface{} {
	var nVal interface{}

	switch typ {
	case "string", "text", "json", "time", "bytes":
		nVal = cast.ToString(val)
	case "smallint":
		nVal = cast.ToInt(val)
	case "integer", "bigint":
		nVal = cast.ToInt64(val)
	case "decimal", "float":
		// nVal = cast.ToFloat64(val)
		nVal = val
	case "bool":
		// nVal = cast.ToBool(val)
		nVal = val
	case "datetime", "date", "timestamp":
		nVal = cast.ToTime(val)
	default:
		nVal = cast.ToString(val)
	}

	return nVal
}

// GetType returns the type of an interface
func (sp *StreamProcessor) GetType(val interface{}) (typ string) {

	switch v := val.(type) {
	case time.Time:
		typ = "timestamp"
	case int8, int16, uint8, uint16:
		typ = "smallint"
	case int, int32, uint, uint32:
		typ = "integer"
	case int64, uint64:
		typ = "bigint"
	case float32, float64:
		typ = "decimal"
	case bool:
		typ = "bool"
	case string, []uint8:
		typ = "string"
	default:
		_ = v
		typ = "string"
	}
	return
}

// CastVal casts values with stats collection
// which degrades performance by ~10%
// go test -benchmem -run='^$ github.com/flarco/dbio/iop' -bench '^BenchmarkProcessVal'
func (sp *StreamProcessor) CastVal(i int, val interface{}, typ string) interface{} {
	cs, ok := sp.colStats[i]
	if !ok {
		sp.colStats[i] = &ColumnStats{}
		cs = sp.colStats[i]
	}

	var nVal interface{}
	var sVal string

	switch v := val.(type) {
	case godror.Number:
		val = sp.ParseString(cast.ToString(val), i)
	case []uint8:
		val = cast.ToString(val)
	case nil:
		cs.TotalCnt++
		cs.NullCnt++
		sp.rowBlankValCnt++
		return nil
	case string:
		sVal = val.(string)
		if sp.config.trimSpace {
			sVal = strings.TrimSpace(sVal)
			val = sVal
		}
		if sVal == "" {
			sp.rowBlankValCnt++
			if sp.config.emptyAsNull || !sp.ds.Columns[i].IsString() {
				cs.TotalCnt++
				cs.NullCnt++
				return nil
			}
		} else if sp.config.nullIf == sVal {
			cs.TotalCnt++
			cs.NullCnt++
			return nil
		}
	default:
		_ = v
	}

	switch typ {
	case "string", "text", "json", "time", "bytes", "":
		sVal = cast.ToString(val)
		if len(sVal) > cs.MaxLen {
			cs.MaxLen = len(sVal)
		}

		if cs.TotalCnt > 0 && cs.NullCnt == cs.TotalCnt && sp.ds != nil {
			// this is an attempt to cast correctly "uncasted" columns
			// (defaulting at string). This will not work in most db insert cases,
			// as the ds.Shape() function will change it back to the "string" type,
			// to match the target table column type. This takes priority.
			nVal = sp.ParseString(sVal)
			sp.ds.Columns[i].Type = sp.GetType(nVal)
			if !sp.ds.Columns[i].IsString() { // so we don't loop
				return sp.CastVal(i, nVal, sp.ds.Columns[i].Type)
			}
			cs.StringCnt++
			cs.Checksum = cs.Checksum + uint64(len(sVal))
			nVal = sVal
		} else {
			cs.StringCnt++
			cs.Checksum = cs.Checksum + uint64(len(sVal))
			nVal = sVal
		}
	case "smallint":
		iVal := cast.ToInt(val)
		if int64(iVal) > cs.Max {
			cs.Max = int64(iVal)
		}
		cs.IntCnt++
		if iVal < 0 {
			cs.Checksum = cs.Checksum + uint64(-iVal)
		} else {
			cs.Checksum = cs.Checksum + uint64(iVal)
		}
		if int64(iVal) < cs.Min {
			cs.Min = int64(iVal)
		}
		nVal = iVal
	case "integer", "bigint":
		iVal := cast.ToInt64(val)
		if iVal > cs.Max {
			cs.Max = iVal
		}
		if int64(iVal) < cs.Min {
			cs.Min = int64(iVal)
		}
		cs.IntCnt++
		if iVal < 0 {
			cs.Checksum = cs.Checksum + uint64(-iVal)
		} else {
			cs.Checksum = cs.Checksum + uint64(iVal)
		}
		nVal = iVal
	case "decimal", "float":
		fVal, _ := sp.toFloat64E(val)
		if int64(fVal) > cs.Max {
			cs.Max = int64(fVal)
		}
		if int64(fVal) < cs.Min {
			cs.Min = int64(fVal)
		}
		cs.DecCnt++
		if fVal < 0 {
			cs.Checksum = cs.Checksum + cast.ToUint64(-fVal)
		} else {
			cs.Checksum = cs.Checksum + cast.ToUint64(fVal)
		}
		// max 9 decimals for bigquery compatibility
		if sp.config.maxDecimals > -1 {
			nVal = math.Round(fVal*sp.config.maxDecimals) / sp.config.maxDecimals
		} else {
			nVal = val // use string to keep accuracy
		}
	case "bool":
		cs.BoolCnt++
		nVal = cast.ToBool(val)
		if nVal.(bool) {
			cs.Checksum++
		}
	case "datetime", "date", "timestamp":
		dVal, err := sp.CastToTime(val)
		if err != nil {
			// sp.unrecognizedDate = g.F(
			// 	"N: %d, ind: %d, val: %s", sp.N, i, cast.ToString(val),
			// )
			// sp.warn = true
			nVal = val // keep string
			cs.StringCnt++
		} else if dVal.IsZero() {
			nVal = nil
			cs.NullCnt++
			sp.rowBlankValCnt++
		} else {
			nVal = dVal
			cs.DateCnt++
			cs.Checksum = cs.Checksum + uint64(dVal.Unix())
		}
	}
	cs.TotalCnt++
	return nVal
}

// CastToString to string
func (sp *StreamProcessor) oldCastToString(i int, val interface{}, valType ...string) string {
	switch v := val.(type) {
	case time.Time:
		tVal, _ := sp.CastToTime(val)
		if tVal.IsZero() {
			return ""
		}
		return tVal.Format("2006-01-02 15:04:05.000")
	default:
		_ = v
		return cast.ToString(val)
	}
}

// CastToString to string. used for csv writing
func (sp *StreamProcessor) CastToString(i int, val interface{}, valType ...string) string {
	typ := ""
	switch v := val.(type) {
	case time.Time:
		typ = "datetime"
	default:
		_ = v
	}

	if len(valType) > 0 {
		typ = valType[0]
	}

	switch typ {
	case "decimal", "float":
		if RemoveTrailingDecZeros {
			// attempt to remove trailing zeros, but is 10 times slower
			return sp.decReplRegex.ReplaceAllString(cast.ToString(val), "$1")
		} else if sp.config.maxDecimals > -1 {
			fVal, _ := sp.toFloat64E(val)
			val = math.Round(fVal*sp.config.maxDecimals) / sp.config.maxDecimals
		}
		return cast.ToString(val)
	case "datetime", "date", "timestamp":
		tVal, _ := sp.CastToTime(val)
		if tVal.IsZero() {
			return ""
		} else if sp.config.datetimeFormat != "" && strings.ToLower(sp.config.datetimeFormat) != "auto" {
			return tVal.Format(sp.config.datetimeFormat)
		}
		return tVal.Format("2006-01-02 15:04:05.000")
	default:
		return cast.ToString(val)
	}
}

// CastValWithoutStats casts the value without counting stats
func (sp *StreamProcessor) CastValWithoutStats(i int, val interface{}, typ string) interface{} {
	var nVal interface{}
	if nil == val {
		return nil
	}

	switch v := val.(type) {
	case godror.Number:
		val = sp.ParseString(cast.ToString(val), i)
	case []uint8:
		val = cast.ToString(val)
	default:
		_ = v
	}

	switch typ {
	case "string", "text", "json", "time", "bytes":
		nVal = cast.ToString(val)
		if nVal == "" {
			nVal = nil
		}
	case "smallint":
		nVal = cast.ToInt(val)
	case "integer", "bigint":
		nVal = cast.ToInt64(val)
	case "decimal", "float":
		// max 9 decimals for bigquery compatibility
		// nVal = math.Round(fVal*1000000000) / 1000000000
		nVal = val // use string to keep accuracy
	case "bool":
		nVal = cast.ToBool(val)
	case "datetime", "date", "timestamp":
		dVal, err := sp.CastToTime(val)
		if err != nil {
			nVal = val // keep string
		} else if dVal.IsZero() {
			nVal = nil
		} else {
			nVal = dVal
		}
	default:
		nVal = cast.ToString(val)
		if nVal == "" {
			nVal = nil
		}
	}

	return nVal
}

// CastToTime converts interface to time
func (sp *StreamProcessor) CastToTime(i interface{}) (t time.Time, err error) {
	i = sp.indirect(i)
	switch v := i.(type) {
	case nil:
		return
	case time.Time:
		return v, nil
	case string:
		return sp.ParseTime(i.(string))
	default:
		return cast.ToTimeE(i)
	}
}

// ParseTime parses a date string and returns time.Time
func (sp *StreamProcessor) ParseTime(i interface{}) (t time.Time, err error) {
	s := cast.ToString(i)
	if s == "" {
		return t, g.Error("blank val")
	}

	// date layouts to try out
	for _, layout := range sp.dateLayouts {
		// use cache to decrease parsing computation next iteration
		if sp.dateLayoutCache != "" {
			t, err = time.Parse(sp.dateLayoutCache, s)
			if err == nil {
				return
			}
		}
		t, err = time.Parse(layout, s)
		if err == nil {
			sp.dateLayoutCache = layout
			return
		}
	}
	return
}

// ParseString return an interface
// string: "varchar"
// integer: "integer"
// decimal: "decimal"
// date: "date"
// datetime: "timestamp"
// timestamp: "timestamp"
// text: "text"
func (sp *StreamProcessor) ParseString(s string, jj ...int) interface{} {
	if s == "" {
		return nil
	}

	j := -1
	if len(jj) > 0 {
		j = jj[0]
	}

	stringTypeCache := sp.stringTypeCache[j]

	if stringTypeCache != "" {
		i, err := sp.parseFuncs[stringTypeCache](s)
		if err == nil {
			return i
		}
	}

	// int
	i, err := sp.parseFuncs["int"](s)
	if err == nil {
		sp.stringTypeCache[j] = "int"
		return i
	}

	// float
	f, err := sp.parseFuncs["float"](s)
	if err == nil {
		sp.stringTypeCache[j] = "float"
		return f
	}

	t, err := sp.parseFuncs["time"](s)
	if err == nil {
		sp.stringTypeCache[j] = "time"
		return t
	}

	// boolean
	// causes issues in SQLite and Oracle
	// b, err := sp.parseFuncs["bool"](s)
	// if err == nil {
	// 	sp.stringTypeCache[j] = "bool"
	// 	return b
	// }

	return s
}

// ProcessVal processes a value
func (sp *StreamProcessor) ProcessVal(val interface{}) interface{} {
	var nVal interface{}
	switch v := val.(type) {
	case godror.Number:
		nVal = sp.ParseString(cast.ToString(val))
	case []uint8:
		nVal = cast.ToString(val)
	default:
		nVal = val
		_ = v
	}
	return nVal

}

// ParseVal parses the value into its appropriate type
func (sp *StreamProcessor) ParseVal(val interface{}) interface{} {
	var nVal interface{}
	switch v := val.(type) {
	case time.Time:
		nVal = cast.ToTime(val)
	case nil:
		nVal = val
	case int:
		nVal = cast.ToInt64(val)
	case int8:
		nVal = cast.ToInt64(val)
	case int16:
		nVal = cast.ToInt64(val)
	case int32:
		nVal = cast.ToInt64(val)
	case int64:
		nVal = cast.ToInt64(val)
	case float32:
		nVal = cast.ToFloat32(val)
	case float64:
		nVal = cast.ToFloat64(val)
	case godror.Number:
		nVal = sp.ParseString(cast.ToString(val))
	case bool:
		nVal = cast.ToBool(val)
	case []uint8:
		nVal = sp.ParseString(cast.ToString(val))
	default:
		nVal = sp.ParseString(cast.ToString(val))
		_ = v
		// fmt.Printf("%T\n", val)
	}
	return nVal
}

// CastRow casts each value of a row
func (sp *StreamProcessor) CastRow(row []interface{}, columns []Column) []interface{} {
	sp.N++
	// Ensure usable types
	sp.rowBlankValCnt = 0
	for i, val := range row {
		// fmt.Printf("| (%s) %#v", columns[i].Type, val)
		row[i] = sp.CastVal(i, val, columns[i].Type)
	}

	// debug a row, prev
	if sp.warn {
		g.Trace("%s -> %#v", sp.unrecognizedDate, row)
		sp.warn = false
	}

	return row
}

// ProcessRow processes a row
func (sp *StreamProcessor) ProcessRow(row []interface{}) []interface{} {
	// Ensure usable types
	for i, val := range row {
		row[i] = sp.ProcessVal(val)
	}
	return row
}

func (sp *StreamProcessor) processRec(rec map[string]interface{}) map[string]interface{} {
	// Ensure usable types
	for i, val := range rec {
		rec[i] = sp.ProcessVal(val)
	}
	return rec
}

func (sp *StreamProcessor) castRowInterf(row []interface{}) []interface{} {
	return row
}
