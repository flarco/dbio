package iop

import (
	"os"
	"testing"
	"time"

	"github.com/flarco/g"
)

var (
	AWS_BUCKET = os.Getenv("AWS_BUCKET")
)

func bParseString(sp *StreamProcessor, val string, b *testing.B) {
	for n := 0; n < b.N; n++ {
		sp.ParseString(val)
	}
}

// go test -run BenchmarkParseString -bench=.
// go test -benchmem -run='^$ github.com/flarco/dbio/iop' -bench '^BenchmarkParseString'
// assume worst case 1000ns * 100 columns * 100000 rows = 0.01sec
func BenchmarkParseString1String(b *testing.B) {
	sp := NewStreamProcessor()
	bParseString(sp, "hello my name is", b)
}
func BenchmarkParseString2Date1(b *testing.B) {
	sp := NewStreamProcessor()
	bParseString(sp, "01-JAN-02 15:04:05", b)
}
func BenchmarkParseString3Date2(b *testing.B) {
	sp := NewStreamProcessor()
	bParseString(sp, "2006-01-02 15:04:05", b)
}
func BenchmarkParseString4Date3(b *testing.B) {
	sp := NewStreamProcessor()
	bParseString(sp, "2006-01-02", b)
}
func BenchmarkParseString5Int(b *testing.B) {
	sp := NewStreamProcessor()
	bParseString(sp, "239189210510", b)
}
func BenchmarkParseString6Float(b *testing.B) {
	sp := NewStreamProcessor()
	bParseString(sp, "239189210510.25234", b)
}
func BenchmarkParseString7Blank(b *testing.B) {
	sp := NewStreamProcessor()
	bParseString(sp, "", b)
}
func BenchmarkParseString8Bool(b *testing.B) {
	sp := NewStreamProcessor()
	bParseString(sp, "true", b)
}

func initProcessRow(name string) (columns []Column, row []interface{}) {

	if name == "float" {
		row = []interface{}{5535.21414}
	} else if name == "decimal" {
		row = []interface{}{[]byte{0x31, 0x30, 0x33, 0x32, 0x2e, 0x34, 0x34, 0x32, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30}}
		row = []interface{}{"5535.214140000"}
	} else if name == "int" {
		row = []interface{}{int(48714719874194)}
	} else if name == "int64" {
		row = []interface{}{int64(48714719874194)}
	} else if name == "string" {
		row = []interface{}{g.RandString(g.AplhanumericRunes, 1000)}
	} else if name == "timestamp" || name == "timestampz" {
		row = []interface{}{time.Now()}
		// row = []interface{}{"17-OCT-20 07.01.59.000000 PM"}
	} else if name == "bool" {
		row = []interface{}{false}
	} else if name == "blank" {
		row = []interface{}{""}
	} else {
		row = []interface{}{
			"fritz", "larco", 55, 563525, 5535.21414, true, time.Now(),
		}
	}

	data := NewDataset(nil)
	fields := make([]string, len(row))
	for i := range row {
		fields[i] = g.F("col%d", i)
	}
	data.SetFields(fields)
	data.Append(row)
	data.InferColumnTypes()
	columns = data.Columns
	return
}

// go test -benchmem -run='^$ github.com/flarco/dbio/iop' -bench '^BenchmarkProcessRow'
func BenchmarkProcessRow1(b *testing.B) {
	sp := NewStreamProcessor()
	_, row := initProcessRow("")
	for n := 0; n < b.N; n++ {
		sp.ProcessRow(row)
	}
}

func BenchmarkProcessRow2(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastVal(i, val, &columns[i])
		}
	}
}
func BenchmarkProcessRow2b(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("")
	for n := 0; n < b.N; n++ {
		row = sp.CastRow(row, columns)
	}
}

func BenchmarkProcessRow3(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastType(val, columns[i].Type)
		}
	}
}

// go test -benchmem -run='^$ github.com/flarco/dbio/iop' -bench '^BenchmarkProcessRows'
func BenchmarkProcessRows(b *testing.B) {
	columns, row := initProcessRow("")
	ds := NewDatastream(columns)
	go func() {
		for range ds.Rows {
		}
	}()
	for n := 0; n < b.N; n++ {
		ds.Push(row)
	}
}

// go test -benchmem -run='^$ github.com/flarco/dbio/iop' -bench '^BenchmarkProcessVal'
func BenchmarkProcessValFloat(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("float")
	sp.ds = NewDatastream(columns)
	for n := 0; n < b.N; n++ {
		row = sp.CastRow(row, columns)
	}
}
func BenchmarkProcessValNumeric(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("decimal")
	sp.ds = NewDatastream(columns)
	for n := 0; n < b.N; n++ {
		row = sp.CastRow(row, columns)
	}
}
func BenchmarkProcessValInt(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("int")
	sp.ds = NewDatastream(columns)
	for n := 0; n < b.N; n++ {
		row = sp.CastRow(row, columns)
	}
}
func BenchmarkProcessValInt64(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("int64")
	sp.ds = NewDatastream(columns)
	for n := 0; n < b.N; n++ {
		row = sp.CastRow(row, columns)
	}
}
func BenchmarkProcessValString(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("string")
	sp.ds = NewDatastream(columns)
	for n := 0; n < b.N; n++ {
		row = sp.CastRow(row, columns)
	}
}
func BenchmarkProcessValBool(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("bool")
	sp.ds = NewDatastream(columns)
	for n := 0; n < b.N; n++ {
		row = sp.CastRow(row, columns)
	}
}
func BenchmarkProcessValTimestamp(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("timestamp")
	sp.ds = NewDatastream(columns)
	for n := 0; n < b.N; n++ {
		row = sp.CastRow(row, columns)
	}
}
func BenchmarkProcessValBlank(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("blank")
	sp.ds = NewDatastream(columns)
	for n := 0; n < b.N; n++ {
		row = sp.CastRow(row, columns)
	}
}

// go test -benchmem -run='^$ github.com/flarco/dbio/iop' -bench '^BenchmarkCastToString'
func BenchmarkCastToStringTime(b *testing.B) {
	sp := NewStreamProcessor()
	_, row := initProcessRow("timestamp")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastToString(i, val)
		}
	}
}
func BenchmarkCastToStringFloat(b *testing.B) {
	sp := NewStreamProcessor()
	_, row := initProcessRow("float")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastToString(i, val)
		}
	}
}
func BenchmarkCastToStringNumeric(b *testing.B) {
	sp := NewStreamProcessor()
	_, row := initProcessRow("decimal")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastToString(i, val, "decimal")
		}
	}
}
func BenchmarkCastToStringInt(b *testing.B) {
	sp := NewStreamProcessor()
	_, row := initProcessRow("int")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastToString(i, val)
		}
	}
}
func BenchmarkCastToStringInt64(b *testing.B) {
	sp := NewStreamProcessor()
	_, row := initProcessRow("int64")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastToString(i, val)
		}
	}
}
func BenchmarkCastToStringString(b *testing.B) {
	sp := NewStreamProcessor()
	_, row := initProcessRow("string")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastToString(i, val)
		}
	}
}
func TestInterfVal(t *testing.T) {
	row := make([]interface{}, 3)
	g.P(row[0])
	row[0] = float64(0)
	g.P(row[0])
	row[0] = nil
	g.P(row[0])
}

func TestParseDate(t *testing.T) {
	sp := NewStreamProcessor()
	val := "17-OCT-20 07.01.59.000000 PM"
	g.P(sp.ParseString(val))
	val = "17-OCT-20"
	g.P(sp.ParseString(val))
	val = `1/17/20`
	g.P(sp.ParseString(val))
}
