package iop

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/flarco/g"
	"github.com/flarco/g/csv"
	"github.com/jedib0t/go-pretty/table"
	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

// Dataset is a query returned dataset
type Dataset struct {
	Result        *sqlx.Rows
	Columns       Columns
	Rows          [][]interface{}
	SQL           string
	Duration      float64
	Sp            *StreamProcessor
	Inferred      bool
	SafeInference bool
	NoTrace       bool
}

// NewDataset return a new dataset
func NewDataset(columns Columns) (data Dataset) {
	data = Dataset{
		Result:  nil,
		Columns: NewColumns(columns...),
		Rows:    [][]interface{}{},
		Sp:      NewStreamProcessor(),
	}

	return
}

// NewDatasetFromMap return a new dataset
func NewDatasetFromMap(m map[string]interface{}) (data Dataset) {
	data = Dataset{
		Result:  nil,
		Columns: Columns{},
		Rows:    [][]interface{}{},
		Sp:      NewStreamProcessor(),
	}

	if fieldsI, ok := m["headers"]; ok {
		fields := []string{}
		for _, f := range fieldsI.([]interface{}) {
			fields = append(fields, cast.ToString(f))
		}
		data.Columns = NewColumnsFromFields(fields...)
	}
	if rowsI, ok := m["rows"]; ok {
		for _, rowI := range rowsI.([]interface{}) {
			data.Rows = append(data.Rows, rowI.([]interface{}))
		}
	}

	return
}

// Sort sorts by cols
// example: `data.Sort(0, 2, 3, false)` will sort
// col0, col2, col3 descending
// example: `data.Sort(0, 2, true)` will sort
// col0, col2 ascending
func (data *Dataset) Sort(args ...any) {

	colIDs := []int{}

	asc := true
	for _, arg := range args {
		switch val := arg.(type) {
		case int:
			colIDs = append(colIDs, val)
		case bool:
			asc = val
		}
	}

	less := func(i, j int) bool {
		arrA := []string{}
		arrB := []string{}
		var a, b string

		for _, colID := range colIDs {
			colType := data.Columns[colID].Type
			valI := data.Rows[i][colID]
			valJ := data.Rows[j][colID]

			if colType.IsInteger() {
				// zero pad for correct sorting
				a = fmt.Sprintf("%20d", valI)
				b = fmt.Sprintf("%20d", valJ)
			} else if colType.IsDecimal() {
				// zero pad for correct sorting
				a = fmt.Sprintf("%20.9f", valI)
				b = fmt.Sprintf("%20.9f", valJ)
			} else {
				a = data.Sp.CastToString(colID, valI, colType)
				b = data.Sp.CastToString(colID, valJ, colType)
			}

			arrA = append(arrA, a)
			arrB = append(arrB, b)
		}
		if asc {
			return strings.Join(arrA, "-") < strings.Join(arrB, "-")
		}
		return strings.Join(arrA, "-") > strings.Join(arrB, "-")
	}

	sort.SliceStable(data.Rows, less)
}

// Print pretty prints the data with a limit
// 0 is unlimited
func (data *Dataset) Print(limit int) {

	tf := "2006-01-02 15:04:05"
	T := table.NewWriter()
	header := table.Row{}
	for _, val := range data.Columns.Names() {
		header = append(header, val)
	}
	T.AppendHeader(header)

	limited := false
	for j, row := range data.Rows {
		for i, col := range data.Columns {
			sVal := cast.ToString(row[i])
			switch {
			case col.IsDatetime() || (strings.HasPrefix(sVal, "20") && strings.HasSuffix(sVal, "Z")):
				val, err := data.Sp.CastToTime(row[i])
				if err != nil {
					row[i] = sVal
				} else {
					row[i] = val.Format(tf)
				}
			case col.IsNumber():
				row[i] = humanize.Comma(cast.ToInt64(row[i]))
			default:
				row[i] = sVal
			}
		}
		T.AppendRow(row)

		if limit > 0 && j+1 == limit {
			limited = true
			break
		}
	}

	println(T.Render())

	if limited {
		g.Warn("results were limited to %d rows.", limit)
	}
}

// WriteCsv writes to a writer
func (data *Dataset) WriteCsv(dest io.Writer) (tbw int, err error) {
	w := csv.NewWriter(dest)
	defer w.Flush()

	tbw, err = w.Write(data.GetFields())
	if err != nil {
		return tbw, g.Error(err, "error write row to csv file")
	}

	for _, row := range data.Rows {
		rec := make([]string, len(row))
		for i, val := range row {
			rec[i] = data.Sp.CastToString(i, val, data.Columns[i].Type)
		}
		bw, err := w.Write(rec)
		if err != nil {
			return tbw, g.Error(err, "error write row to csv file")
		}
		tbw = tbw + bw
	}
	return
}

// GetFields return the fields of the Data
func (data *Dataset) GetFields(lower ...bool) []string {
	Lower := false
	if len(lower) > 0 {
		Lower = lower[0]
	}
	fields := make([]string, len(data.Columns))

	for j, column := range data.Columns {
		if Lower {
			fields[j] = strings.ToLower(column.Name)
		} else {
			fields[j] = column.Name
		}
	}

	return fields
}

// SetFields sets the fields/columns of the Datastream
func (data *Dataset) SetFields(fields []string) {
	if data.Columns == nil || len(data.Columns) != len(fields) {
		data.Columns = make(Columns, len(fields))
	}

	for i, field := range fields {
		data.Columns[i].Name = field
		data.Columns[i].Position = i + 1
	}
}

// Append appends a new row
func (data *Dataset) Append(row []interface{}) {
	data.Rows = append(data.Rows, row)
}

// Stream returns a datastream of the dataset
func (data *Dataset) Stream() *Datastream {
	ds := NewDatastream(data.Columns)
	ds.Inferred = data.Inferred

	go func() {
		ds.SetReady()
		defer ds.Close()

	loop:
		for _, row := range data.Rows {
			select {
			case <-ds.Context.Ctx.Done():
				break loop
			default:
				ds.Push(row)
			}
		}
		ds.SetEmpty()
	}()

	return ds
}

// FirstVal returns the first value from the first row
func (data *Dataset) FirstVal() interface{} {
	if len(data.Rows) > 0 && len(data.Rows[0]) > 0 {
		return data.Rows[0][0]
	}
	return nil
}

// FirstRow returns the first row
func (data *Dataset) FirstRow() []interface{} {
	if len(data.Rows) > 0 {
		return data.Rows[0]
	}
	return nil
}

// ColValues returns the values of a one column as array
func (data *Dataset) ColValues(col int) []interface{} {
	vals := make([]interface{}, len(data.Rows))
	for i, row := range data.Rows {
		vals[i] = row[col]
	}
	return vals
}

// ColValuesStr returns the values of a one column as array or string
func (data *Dataset) ColValuesStr(col int) []string {
	vals := make([]string, len(data.Rows))
	for i, row := range data.Rows {
		vals[i] = data.Sp.CastToString(i, row[col], data.Columns[i].Type)
	}
	return vals

}

// Records return rows of maps
func (data *Dataset) Records() []map[string]interface{} {
	records := make([]map[string]interface{}, len(data.Rows))
	for i, row := range data.Rows {
		rec := map[string]interface{}{}
		for j, field := range data.GetFields(true) {
			rec[field] = row[j]
		}
		records[i] = rec
	}
	return records
}

// ToJSONMap converst to a JSON object
func (data *Dataset) ToJSONMap() map[string]interface{} {
	return g.M("headers", data.GetFields(), "rows", data.Rows)
}

// InferColumnTypes determines the columns types
func (data *Dataset) InferColumnTypes() {
	var columns Columns

	if len(data.Rows) == 0 {
		g.Trace("skipping InferColumnTypes [no rows]")
		return
	}

	for i, column := range data.Columns {
		column.Type = lo.Ternary(column.Type == "", StringType, column.Type)
		column.Stats = ColumnStats{
			Min:    math.MaxInt64,
			Max:    math.MinInt64,
			MinLen: math.MaxInt32,
		}
		column.Position = i + 1
		columns = append(columns, column)
	}

	// g.Trace("InferColumnTypes with sample size %d", SampleSize)
	for i, row := range data.Rows {
		if i >= SampleSize {
			break
		}

		for j, val := range row {
			val = data.Sp.ParseString(strings.TrimSpace(cast.ToString(val)), j)
			columns[j].Stats.TotalCnt++

			valStr := cast.ToString(val)
			l := len(valStr)
			if val == nil || l == 0 {
				columns[j].Stats.NullCnt++
				continue
			} else {
				if l > columns[j].Stats.MaxLen {
					columns[j].Stats.MaxLen = l
				}
				if l < columns[j].Stats.MinLen {
					columns[j].Stats.MinLen = l
				}
			}

			switch v := val.(type) {
			case time.Time:
				columns[j].Stats.DateCnt++
			case nil:
				columns[j].Stats.NullCnt++
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				columns[j].Stats.IntCnt++
				val0 := cast.ToInt64(val)
				if val0 > columns[j].Stats.Max {
					columns[j].Stats.Max = val0
				}
				if val0 < columns[j].Stats.Min {
					columns[j].Stats.Min = val0
				}
			case float32, float64:
				columns[j].Stats.DecCnt++
				val0 := cast.ToInt64(val)
				if val0 > columns[j].Stats.Max {
					columns[j].Stats.Max = val0
				}
				if val0 < columns[j].Stats.Min {
					columns[j].Stats.Min = val0
				}

				if strings.Contains(valStr, ".") {
					decLen := len(strings.Split(cast.ToString(val), ".")[1])
					if decLen > columns[j].Stats.MaxDecLen {
						columns[j].Stats.MaxDecLen = decLen
					}
				}

			case bool:
				columns[j].Stats.BoolCnt++
			case string, []uint8:
				if looksLikeJson(valStr) {
					var v interface{}
					if err := g.Unmarshal(valStr, &v); err == nil {
						columns[j].Stats.JsonCnt++
					} else {
						columns[j].Stats.StringCnt++
					}
				} else {
					columns[j].Stats.StringCnt++
				}

			default:
				_ = v
			}
		}
	}

	data.Columns = InferFromStats(columns, data.SafeInference, data.NoTrace)
	data.Inferred = true
}

func looksLikeJson(s string) bool {
	return (strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}")) || (strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]"))
}
