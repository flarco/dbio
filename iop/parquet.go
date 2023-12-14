package iop

import (
	// "encoding/csv"
	// "io"

	"io"
	"strings"

	"github.com/flarco/g"
	"github.com/samber/lo"

	// "github.com/xitongsys/parquet-go/reader"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

// Parquet is a parquet object
type Parquet struct {
	Path   string
	Reader *goparquet.FileReader
	Data   *Dataset
	colMap map[string]int
}

func NewParquetStream(reader io.ReadSeeker, columns Columns) (p *Parquet, err error) {
	fr, err := goparquet.NewFileReader(reader, columns.Names()...)
	if err != nil {
		err = g.Error(err, "could not read parquet reader")
		return
	}
	p = &Parquet{Reader: fr}
	p.colMap = p.Columns().FieldMap(true)
	return
}

func (p *Parquet) Columns() Columns {

	typeMap := map[parquet.Type]ColumnType{
		parquet.Type_BOOLEAN:              BoolType,
		parquet.Type_INT32:                IntegerType,
		parquet.Type_INT64:                BigIntType,
		parquet.Type_INT96:                BigIntType,
		parquet.Type_FLOAT:                DecimalType,
		parquet.Type_DOUBLE:               DecimalType,
		parquet.Type_BYTE_ARRAY:           StringType,
		parquet.Type_FIXED_LEN_BYTE_ARRAY: StringType,
	}

	cols := Columns{}
	for _, col := range p.Reader.Columns() {
		colType := col.Type()
		if colType == nil {
			ct := parquet.Type_BYTE_ARRAY
			colType = &ct
		}

		c := Column{
			Name:     CleanName(col.Name()),
			Type:     typeMap[*colType],
			Position: len(cols) + 1,
		}

		lType := col.Element().LogicalType
		switch {
		case lType == nil:
		case lType.IsSetSTRING():
			c.Type = StringType
		case lType.IsSetMAP():
			c.Type = JsonType
		case lType.IsSetLIST():
			c.Type = JsonType
		case lType.IsSetENUM():
			c.Type = JsonType
		case lType.IsSetDECIMAL():
			c.Type = DecimalType
		case lType.IsSetDATE():
			c.Type = TimestampType
		case lType.IsSetTIME():
			c.Type = TimestampType
		case lType.IsSetTIMESTAMP():
			c.Type = TimestampType
		case lType.IsSetINTEGER():
			c.Type = BigIntType
		case lType.IsSetUNKNOWN():
			c.Type = StringType
		case lType.IsSetJSON():
			c.Type = JsonType
		case lType.IsSetBSON():
			c.Type = JsonType
		case lType.IsSetUUID():
			c.Type = StringType
		}

		cols = append(cols, c)
	}
	return cols
}

func (p *Parquet) nextFunc(it *Iterator) bool {
	newRec, err := p.Reader.NextRow()
	if err == io.EOF {
		return false
	} else if err != nil {
		it.Context.CaptureErr(g.Error(err, "could not read Parquet row"))
		return false
	}

	it.Row = make([]interface{}, len(it.ds.Columns))
	for k, v := range newRec {
		col := it.ds.Columns[p.colMap[strings.ToLower(k)]]
		i := col.Position - 1
		it.Row[i] = v
	}
	return true
}

func getParquetColumns(cols Columns) *parquetschema.ColumnDefinition {
	colDef := parquetschema.ColumnDefinition{
		Children:      []*parquetschema.ColumnDefinition{},
		SchemaElement: &parquet.SchemaElement{Name: "table"},
	}

	for _, col := range cols {
		lt := parquet.NewLogicalType()
		var ct *parquet.ConvertedType
		var t parquet.Type
		switch col.Type {
		case StringType, TextType, BinaryType:
			lt.STRING = parquet.NewStringType()
			ct = parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8)
			t = parquet.Type_BYTE_ARRAY
		case JsonType:
			lt.JSON = parquet.NewJsonType()
			ct = parquet.ConvertedTypePtr(parquet.ConvertedType_JSON)
			t = parquet.Type_BYTE_ARRAY
		case DateType, DatetimeType, TimestampType, TimestampzType:
			lt.DATE = parquet.NewDateType()
			ct = parquet.ConvertedTypePtr(parquet.ConvertedType_DATE)
			t = parquet.Type_BYTE_ARRAY
		case SmallIntType, IntegerType, BigIntType:
			lt.INTEGER = parquet.NewIntType()
			ct0, _ := parquet.ConvertedTypeFromString("INT_64")
			ct = &ct0
			t = parquet.Type_INT64
		case DecimalType, FloatType:
			lt.INTEGER = parquet.NewIntType()
			ct0, _ := parquet.ConvertedTypeFromString("INT_64")
			ct = &ct0
			t = parquet.Type_DOUBLE
		case BoolType:
			lt.STRING = parquet.NewStringType()
			ct = parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8)
			t = parquet.Type_BYTE_ARRAY
		default:
			g.Warn("unhandled parquet column type: %s", col.Type)
			lt.STRING = parquet.NewStringType()
			ct = parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8)
			t = parquet.Type_BYTE_ARRAY
		}

		children := &parquetschema.ColumnDefinition{
			// Children: []*parquetschema.ColumnDefinition{},
			SchemaElement: &parquet.SchemaElement{
				Name: col.Name,
				// LogicalType:   lt,
				ConvertedType: ct,
				Type:          &t,
			},
		}
		colDef.Children = append(colDef.Children, children)
	}

	return &colDef
}

func getParquetSchemaDef(cols Columns) (*parquetschema.SchemaDefinition, error) {

	colTexts := []string{}
	for _, col := range cols {
		// logical-type ::= 'STRING'
		//   | 'DATE'
		//   | 'TIMESTAMP' '(' <time-unit> ',' <boolean> ')'
		//   | 'UUID'
		//   | 'ENUM'
		//   | 'JSON'
		//   | 'BSON'
		//   | 'INT' '(' <bit-width> ',' <boolean> ')'
		//   | 'DECIMAL' '(' <precision> ',' <scale> ')'

		// type ::= 'binary'
		// | 'float'
		// | 'double'
		// | 'boolean'
		// | 'int32'
		// | 'int64'
		// | 'int96'
		// | 'fixed_len_byte_array' '(' <number> ')'

		lt := ""
		t := ""
		switch col.Type {
		case StringType, TextType, BinaryType, TimeType, TimezType:
			lt = "(STRING)"
			t = "binary"
		case JsonType:
			lt = "(JSON)"
			t = "binary"
		case DateType, DatetimeType, TimestampType, TimestampzType:
			lt = "(TIMESTAMP(NANOS, true))"
			t = "int64"
		case SmallIntType:
			lt = ""
			t = "int32"
		case IntegerType, BigIntType:
			lt = ""
			t = "int64"
		case DecimalType, FloatType:

			scale := lo.Ternary(col.DbScale < 9, 9, col.DbScale)
			scale = lo.Ternary(scale < col.Stats.MaxDecLen, col.Stats.MaxDecLen, scale)
			scale = lo.Ternary(scale > 24, 24, scale)

			precision := lo.Ternary(col.DbPrecision < 24, 24, col.DbPrecision)
			precision = lo.Ternary(precision < (scale*2), scale*2, precision)
			precision = lo.Ternary(precision > 38, 38, precision)

			lt = g.F("(DECIMAL(%d,%d))", precision, scale)
			t = "binary"
		case BoolType:
			lt = ""
			t = "boolean"
		default:
			g.Warn("unhandled parquet column type: %s", col.Type)
			lt = "(STRING)"
			t = "binary"
		}

		_ = lt
		t = strings.ToLower(t)

		// <type> <identifier> <logical-type-annotation>
		colTexts = append(colTexts, g.F("  optional %s %s %s;", strings.ToLower(t), col.Name, lt))
		// colTexts = append(colTexts, g.F("  optional %s %s;", t, col.Name))
	}

	schemaText := g.F("message table {\n%s\n}", strings.Join(colTexts, "\n"))
	g.Trace(schemaText)
	schemaDef, err := parquetschema.ParseSchemaDefinition(schemaText)
	if err != nil {
		g.Warn(schemaText)
		err = g.Error(err, "could not ParseSchemaDefinition")
	}
	return schemaDef, err
}

// WriteStream to Parquet file from datastream
// func (p *Parquet) WriteStream(ds *Datastream) error {

// 	if p.File == nil {
// 		file, err := os.Create(p.Path)
// 		if err != nil {
// 			return err
// 		}
// 		p.File = file
// 	}

// 	p.PFile = writerfile.NewWriterFile(p.File)

// 	defer p.File.Close()

// 	schema := getParquetCsvSchema(ds.Columns)

// 	pw, err := writer.NewCSVWriter(schema, p.PFile, 4)
// 	if err != nil {
// 		return err
// 	}
// 	// defer pw.Flush(true)

// 	for row := range ds.Rows() {
// 		err := pw.Write(row)
// 		if err != nil {
// 			return g.Error(err, "error write row to parquet file")
// 		}
// 	}

// 	err = pw.WriteStop()

// 	return err
// }
