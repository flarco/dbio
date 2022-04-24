package iop

import (
	"encoding/csv"
	"math"
	"os"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/jmoiron/sqlx"
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
		Columns: columns,
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

// WriteCsv writes to a csv file
func (data *Dataset) WriteCsv(path string) error {
	file, err := os.Create(path)

	w := csv.NewWriter(file)
	defer w.Flush()

	err = w.Write(data.GetFields())
	if err != nil {
		return g.Error(err, "error write row to csv file")
	}

	for _, row := range data.Rows {
		rec := make([]string, len(row))
		for i, val := range row {
			rec[i] = cast.ToString(val)
		}
		err := w.Write(rec)
		if err != nil {
			return g.Error(err, "error write row to csv file")
		}
	}
	return nil
}

// GetFields return the fields of the Data
func (data *Dataset) GetFields() []string {
	fields := make([]string, len(data.Columns))

	for j, column := range data.Columns {
		fields[j] = strings.ToLower(column.Name)
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
		ds.Ready = true
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
		vals[i] = cast.ToString(row[col])
	}
	return vals

}

// Records return rows of maps
func (data *Dataset) Records() []map[string]interface{} {
	records := make([]map[string]interface{}, len(data.Rows))
	for i, row := range data.Rows {
		rec := map[string]interface{}{}
		for j, field := range data.GetFields() {
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
		g.Debug("skipping InferColumnTypes [no rows]")
		return
	}

	for i, field := range data.GetFields() {
		columns = append(columns, Column{
			Name:     field,
			Position: i + 1,
			Type:     "string",
			Stats: ColumnStats{
				Min:    math.MaxInt64,
				MinLen: math.MaxInt32,
			},
		})
	}

	// g.Trace("InferColumnTypes with sample size %d", SampleSize)
	for i, row := range data.Rows {
		if i >= SampleSize {
			break
		}

		for j, val := range row {
			val = data.Sp.ParseString(strings.TrimSpace(cast.ToString(val)), j)
			columns[j].Stats.TotalCnt++

			l := len(cast.ToString(val))
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

				valStr := cast.ToString(val)
				if strings.Contains(valStr, ".") {
					decLen := len(strings.Split(cast.ToString(val), ".")[1])
					if decLen > columns[j].Stats.MaxDecLen {
						columns[j].Stats.MaxDecLen = decLen
					}
				}

			case bool:
				columns[j].Stats.BoolCnt++
			case string, []uint8:
				columns[j].Stats.StringCnt++

			default:
				_ = v
			}
		}
	}

	data.Columns = InferFromStats(columns, data.SafeInference, data.NoTrace)
	data.Inferred = true
}
