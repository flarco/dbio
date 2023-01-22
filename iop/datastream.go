package iop

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"

	"github.com/flarco/g"
	"github.com/flarco/g/csv"
	"github.com/flarco/g/json"
	jit "github.com/json-iterator/go"

	"github.com/samber/lo"
	"github.com/spf13/cast"
)

var (
	jsoniter       = jit.ConfigCompatibleWithStandardLibrary
	replacePattern = regexp.MustCompile("[^_0-9a-zA-Z]+") // to clea header fields
)

// Datastream is a stream of rows
type Datastream struct {
	Columns       Columns
	Buffer        [][]any
	BatchChan     chan *Batch
	Batches       []*Batch
	Count         uint64
	Context       *g.Context
	Ready         bool
	Bytes         uint64
	Sp            *StreamProcessor
	SafeInference bool
	NoTrace       bool
	Inferred      bool
	deferFuncs    []func()
	closed        bool
	empty         bool
	clones        []*Datastream
	it            *Iterator
	config        *streamConfig
	df            *Dataflow
	bwRows        chan []any // for correct byte written
	readyChn      chan struct{}
	schemaChgChan chan struct{}
	bwCsv         *csv.Writer // for correct byte written
	ID            string
	Metadata      Metadata // map of column name to metadata type
	paused        bool
	pauseChan     chan struct{}
}

type KeyValue struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

type Metadata struct {
	StreamURL KeyValue `json:"stream_url"`
	LoadedAt  KeyValue `json:"loaded_at"`
}

// AsMap return as map
func (m *Metadata) AsMap() map[string]any {
	m0 := g.M()
	g.JSONConvert(m, &m0)
	return m0
}

// Iterator is the row provider for a datastream
type Iterator struct {
	Row      []any
	Counter  uint64
	Context  g.Context
	Closed   bool
	ds       *Datastream
	nextFunc func(it *Iterator) bool
	limitCnt uint64 // to not check for df limit each cycle
}

// NewDatastream return a new datastream
func NewDatastream(columns Columns) (ds *Datastream) {
	return NewDatastreamContext(context.Background(), columns)
}

// NewDatastreamIt with it
func NewDatastreamIt(ctx context.Context, columns Columns, nextFunc func(it *Iterator) bool) (ds *Datastream) {
	ds = NewDatastreamContext(ctx, columns)
	ds.it = &Iterator{
		Row:      make([]any, len(columns)),
		nextFunc: nextFunc,
		Context:  g.NewContext(ctx),
		ds:       ds,
	}
	return
}

// NewDatastreamContext return a new datastream
func NewDatastreamContext(ctx context.Context, columns Columns) (ds *Datastream) {
	context := g.NewContext(ctx)

	ds = &Datastream{
		ID:            g.NewTsID("ds"),
		BatchChan:     make(chan *Batch, 4),
		Batches:       []*Batch{},
		Columns:       columns,
		Context:       &context,
		Sp:            NewStreamProcessor(),
		config:        &streamConfig{emptyAsNull: true, header: true},
		deferFuncs:    []func(){},
		clones:        []*Datastream{},
		bwCsv:         csv.NewWriter(ioutil.Discard),
		bwRows:        make(chan []any, 100),
		readyChn:      make(chan struct{}),
		schemaChgChan: make(chan struct{}, 1),
		pauseChan:     make(chan struct{}),
	}
	ds.Sp.ds = ds

	return
}

func (ds *Datastream) Df() *Dataflow {
	return ds.df
}

func (ds *Datastream) processBwRows() {
	// bwRows slows process speed by 10x, but this is needed for byte sizing
	go func() {
		if os.Getenv("DBIO_CSV_BYTES") == "TRUE" {
			for row := range ds.bwRows {
				ds.writeBwCsv(ds.CastRowToString(row))
				ds.bwCsv.Flush()
			}
		} else {
			for range ds.bwRows {
				// drain channel
			}
		}
	}()
}

// SetReady sets the ds.ready
func (ds *Datastream) SetReady() {
	if !ds.Ready {
		ds.Ready = true
		go func() { ds.readyChn <- struct{}{} }()
	}
}

// SetEmpty sets the ds.Rows channel as empty
func (ds *Datastream) SetEmpty() {
	ds.empty = true
}

// SetConfig sets the ds.config values
func (ds *Datastream) SetConfig(configMap map[string]string) {
	// lower the keys
	for _, k := range lo.Keys(configMap) {
		configMap[strings.ToLower(k)] = configMap[k]
	}
	ds.Sp.SetConfig(configMap)
	ds.config = ds.Sp.config
}

// CastRowToString returns the row as string casted
func (ds *Datastream) CastRowToString(row []any) []string {
	rowStr := make([]string, len(row))
	for i, val := range row {
		rowStr[i] = ds.Sp.CastToString(i, val, ds.Columns[i].Type)
	}
	return rowStr
}

// writeBwCsv writes to the nullCsv
func (ds *Datastream) writeBwCsv(row []string) {
	bw, _ := ds.bwCsv.Write(row)
	ds.AddBytes(int64(bw))
}

// Push return the fields of the Data
func (ds *Datastream) Push(row []any) {
	batch := ds.CurrentBatch()

	if batch == nil {
		batch = ds.NewBatch(ds.Columns)
	}

	batch.Push(row)
}

// Clone returns a new datastream of the same source
func (ds *Datastream) Clone() *Datastream {
	cDs := NewDatastreamContext(ds.Context.Ctx, ds.Columns)
	cDs.Inferred = ds.Inferred
	cDs.config = ds.config
	cDs.Sp.config = ds.Sp.config
	cDs.SetReady()
	ds.clones = append(ds.clones, cDs)
	return cDs
}

// IsClosed is true is ds is closed
func (ds *Datastream) IsClosed() bool {
	return ds.closed
}

// WaitReady waits until datastream is ready
func (ds *Datastream) WaitReady() error {
	if ds.Ready {
		return ds.Context.Err()
	}

	select {
	case <-ds.readyChn:
		return ds.Context.Err()
	case <-ds.Context.Ctx.Done():
		return ds.Context.Err()
	}
}

// Defer runs a given function as close of Datastream
func (ds *Datastream) Defer(f func()) {
	if !cast.ToBool(os.Getenv("KEEP_TEMP_FILES")) {
		ds.deferFuncs = append(ds.deferFuncs, f)
	}
	if ds.closed { // mutex?
		for _, f := range ds.deferFuncs {
			f()
		}
	}
}

// Close closes the datastream
func (ds *Datastream) Close() {
	if !ds.closed {
		close(ds.bwRows)
		close(ds.BatchChan)

	loop:
		for {
			select {
			case <-ds.pauseChan:
			case <-ds.readyChn:
			default:
				break loop
			}
		}

		select {
		case <-ds.Sp.typeChangedChan: // clean up
		default:
		}

		for _, f := range ds.deferFuncs {
			f()
		}

		if ds.Sp.unrecognizedDate != "" {
			g.Warn("unrecognized date format (%s)", ds.Sp.unrecognizedDate)
		}

		ds.Buffer = nil // clear buffer
	}
	if ds.it != nil {
		ds.it.close()
	}
	ds.closed = true
	select {
	case <-ds.readyChn:
	default:
	}
}

// SetColumns sets the columns
func (ds *Datastream) AddColumns(newCols Columns, overwrite bool) (added Columns) {
	ds.Columns, added = ds.Columns.Add(newCols, overwrite)
	if df := ds.df; df != nil {
		df.AddColumns(newCols, overwrite)
	}
	return added
}

// ChangeColumn applies a column type change
func (ds *Datastream) ChangeColumn(i int, newType ColumnType) {

	switch {
	case ds == nil || ds.Columns[i].Type == newType:
		return
	case ds.Columns[i].Type == TextType && newType == StringType:
		return
	}

	g.Debug("column type change for %s (%s to %s)", ds.Columns[i].Name, ds.Columns[i].Type, newType)
	ds.Columns[i].Type = newType
	if df := ds.df; df != nil {
		df.ChangeColumn(i, newType)
	}
}

// GetFields return the fields of the Data
func (ds *Datastream) GetFields(args ...bool) []string {
	lower := false
	cleanUp := false
	if len(args) > 1 {
		lower = args[0]
		cleanUp = args[1]
	} else if len(args) > 0 {
		lower = args[0]
	}
	fields := make([]string, len(ds.Columns))

	for j, column := range ds.Columns {
		field := column.Name

		if lower {
			field = strings.ToLower(column.Name)
		}
		if cleanUp {
			field = string(replacePattern.ReplaceAll([]byte(field), []byte("_"))) // clean up
		}

		fields[j] = field
	}

	return fields
}

// SetFields sets the fields/columns of the Datastream
func (ds *Datastream) SetFields(fields []string) {
	if ds.Columns == nil || len(ds.Columns) != len(fields) {
		ds.Columns = make(Columns, len(fields))
	}

	for i, field := range fields {
		ds.Columns[i].Name = field
		ds.Columns[i].Position = i + 1
	}
}

// Collect reads a stream and return a dataset
// limit of 0 is unlimited
func (ds *Datastream) Collect(limit int) (Dataset, error) {
	data := NewDataset(ds.Columns)

	// wait for first ds to start streaming.
	// columns/buffer need to be populated
	err := ds.WaitReady()
	if err != nil {
		return data, g.Error(err)
	}

	data.Result = nil
	data.Columns = ds.Columns
	data.Rows = [][]any{}
	limited := false

	for row := range ds.Rows() {
		data.Rows = append(data.Rows, row)
		if limit > 0 && len(data.Rows) == limit {
			limited = true
			break
		}
	}

	if !limited {
		ds.SetEmpty()
	}

	ds.Buffer = nil // clear buffer
	if ds.Err() != nil {
		return data, g.Error(ds.Err())
	}
	return data, nil
}

// Err return the error if any
func (ds *Datastream) Err() (err error) {
	return ds.Context.Err()
}

// Start generates the stream
// Should cycle the Iter Func until done
func (ds *Datastream) Start() (err error) {
	if ds.it == nil {
		err = g.Error("iterator not defined")
		return g.Error(err, "need to define iterator")
	}

loop:
	for ds.it.next() {
		select {
		case <-ds.Context.Ctx.Done():
			if ds.Context.Err() != nil {
				err = g.Error(ds.Context.Err())
				return
			}
			break loop
		case <-ds.it.Context.Ctx.Done():
			if ds.it.Context.Err() != nil {
				err = g.Error(ds.it.Context.Err())
				ds.Context.CaptureErr(err, "Failed to scan")
				return
			}
			break loop
		default:
			if ds.it.Counter == 1 && !ds.NoTrace {
				g.Trace("%#v", ds.it.Row) // trace first row for debugging
			}

			row := ds.Sp.ProcessRow(ds.it.Row)
			ds.Buffer = append(ds.Buffer, row)
			if ds.it.Counter >= cast.ToUint64(SampleSize) {
				break loop
			}
		}
	}

	// infer types
	if !ds.Inferred {
		sampleData := NewDataset(ds.Columns)
		sampleData.Rows = ds.Buffer
		sampleData.NoTrace = ds.NoTrace
		sampleData.SafeInference = ds.SafeInference
		sampleData.Sp.dateLayouts = ds.Sp.dateLayouts
		sampleData.InferColumnTypes()
		ds.Columns = sampleData.Columns
		ds.Inferred = true
	}

	if ds.it.Context.Err() != nil {
		err = g.Error(ds.it.Context.Err(), "error in getting rows")
		return
	}

	// add metadata
	metaValuesMap := map[int]any{}
	{
		// ensure there are no duplicates
		ensureName := func(name string) string {
			colNames := lo.Keys(ds.Columns.FieldMap(true))
			for lo.Contains(colNames, strings.ToLower(name)) {
				name = name + "_"
			}
			return name
		}

		if ds.Metadata.LoadedAt.Key != "" && ds.Metadata.LoadedAt.Value != nil {
			ds.Metadata.LoadedAt.Key = ensureName(ds.Metadata.LoadedAt.Key)
			col := Column{
				Name:     ds.Metadata.LoadedAt.Key,
				Type:     IntegerType,
				Position: len(ds.Columns) + 1,
			}
			ds.Columns = append(ds.Columns, col)
			metaValuesMap[col.Position-1] = ds.Metadata.LoadedAt.Value
		}

		if ds.Metadata.StreamURL.Key != "" && ds.Metadata.StreamURL.Value != nil {
			ds.Metadata.StreamURL.Key = ensureName(ds.Metadata.StreamURL.Key)
			col := Column{
				Name:     ds.Metadata.StreamURL.Key,
				Type:     StringType,
				Position: len(ds.Columns) + 1,
			}
			ds.Columns = append(ds.Columns, col)
			metaValuesMap[col.Position-1] = ds.Metadata.StreamURL.Value
		}
	}

	// setMetaValues sets mata column values
	setMetaValues := func(row []any) []any { return row }
	if len(metaValuesMap) > 0 {
		setMetaValues = func(row []any) []any {
			for i, v := range metaValuesMap {
				row[i] = v
			}
			return row
		}
	}

	go ds.processBwRows()

	go func() {
		var batch *Batch
		var err error
		defer ds.Close()

		ds.SetReady()
		batch = ds.NewBatch(ds.Columns)

		// push := func(row []any) {
		// retry:
		// 	newRow := row
		// 	for _, f := range batch.transforms {
		// 		newRow = f(newRow) // allows transformations
		// 	}
		// 	select {
		// 	case <-ds.Context.Ctx.Done():
		// 		ds.Close()
		// 		return
		// 	case <-ds.pauseChan:
		// 		<-ds.pauseChan // wait for unpause
		// 		goto retry
		// 	case batch.Rows <- newRow:
		// 		batch.ds.bwRows <- newRow
		// 		batch.ds.Count++
		// 	}
		// }

		for _, row := range ds.Buffer {
			row = ds.Sp.CastRow(row, ds.Columns)
			if ds.config.skipBlankLines && ds.Sp.rowBlankValCnt == len(row) {
				continue
			}
			batch.Push(setMetaValues(row))
		}

		row := make([]any, len(ds.Columns))
		rowPtrs := make([]any, len(ds.Columns))
		for i := range row {
			// cast the interface place holders
			row[i] = ds.Sp.CastType(row[i], ds.Columns[i].Type)
			rowPtrs[i] = &row[i]
		}

		defer func() {
			// if any error occurs during iteration
			if ds.it.Context.Err() != nil {
				ds.Context.CaptureErr(g.Error(ds.it.Context.Err(), "error during iteration"))
			}
		}()

	loop:
		for ds.it.next() {
			select {
			case <-ds.pauseChan:
				<-ds.pauseChan // wait for unpause
			default:
			}

			row = ds.Sp.CastRow(ds.it.Row, ds.Columns)
			if ds.config.skipBlankLines && ds.Sp.rowBlankValCnt == len(row) {
				continue
			}

			select {
			case <-ds.schemaChgChan:
				batch = ds.NewBatch(ds.Columns)
			default:
				if batch.closed {
					batch = ds.NewBatch(ds.Columns)
				}
			}

			select {
			case <-ds.Context.Ctx.Done():
				if ds.df != nil {
					ds.df.Context.CaptureErr(ds.Err())
				}
				break loop
			case <-ds.it.Context.Ctx.Done():
				if ds.it.Context.Err() != nil {
					err = g.Error(ds.it.Context.Err())
					ds.Context.CaptureErr(err, "Failed to scan")
					if ds.df != nil {
						ds.df.Context.CaptureErr(ds.Err())
					}
				}
				break loop
			default:
				batch.Push(setMetaValues(row))
			}
		}

		// close batch
		batch.Close()

		ds.SetEmpty()

		if !ds.NoTrace {
			g.Trace("Got %d rows", ds.it.Counter)
		}
	}()

	return
}

func (ds *Datastream) Rows() chan []any {
	rows := MakeRowsChan()

	go func() {
		defer close(rows)
		for batch := range ds.BatchChan {
			for row := range batch.Rows {
				rows <- row
			}
		}
	}()

	return rows
}

func (ds *Datastream) SetMetadata(jsonStr string) {
	if jsonStr != "" {
		g.Unmarshal(jsonStr, &ds.Metadata)
		ds.Metadata.LoadedAt.Value = cast.ToInt64(ds.Metadata.LoadedAt.Value)
	}
}

// ConsumeJsonReader uses the provided reader to stream JSON
// This will put each JSON rec as one string value
// so payload can be processed downstream
func (ds *Datastream) ConsumeJsonReader(reader io.Reader) (err error) {
	reader2, err := AutoDecompress(reader)
	if err != nil {
		return g.Error(err, "Could not decompress reader")
	}

	decoder := json.NewDecoder(reader2)
	js := NewJSONStream(ds, decoder, ds.Sp.config.flatten)

	ds.it = &Iterator{
		Row:      make([]any, len(ds.Columns)),
		nextFunc: js.nextFunc,
		Context:  g.NewContext(ds.Context.Ctx),
		ds:       ds,
	}

	err = ds.Start()
	if err != nil {
		return g.Error(err, "could start datastream")
	}
	return
}

// ConsumeXmlReader uses the provided reader to stream XML
// This will put each XML rec as one string value
// so payload can be processed downstream
func (ds *Datastream) ConsumeXmlReader(reader io.Reader) (err error) {
	reader2, err := AutoDecompress(reader)
	if err != nil {
		return g.Error(err, "Could not decompress reader")
	}

	decoder := xml.NewDecoder(reader2)
	js := NewJSONStream(ds, decoder, ds.Sp.config.flatten)

	ds.it = &Iterator{
		Row:      make([]any, len(ds.Columns)),
		nextFunc: js.nextFunc,
		Context:  g.NewContext(ds.Context.Ctx),
		ds:       ds,
	}

	err = ds.Start()
	if err != nil {
		return g.Error(err, "could start datastream")
	}
	return
}

// ConsumeCsvReader uses the provided reader to stream rows
func (ds *Datastream) ConsumeCsvReader(reader io.Reader) (err error) {
	c := CSV{Reader: reader, NoHeader: !ds.config.header}

	r, err := c.getReader(ds.config.delimiter)
	if err != nil {
		err = g.Error(err, "could not get reader")
		ds.Context.CaptureErr(err)
		return err
	}

	// set delimiter
	ds.config.delimiter = string(c.Delimiter)

	row0, err := r.Read()
	if err == io.EOF {
		g.Warn("csv stream provided is empty")
		ds.SetReady()
		ds.Close()
		return nil
	} else if err != nil {
		err = g.Error(err, "could not parse header line")
		ds.Context.CaptureErr(err)
		return err
	}
	ds.SetFields(CleanHeaderRow(row0))

	nextFunc := func(it *Iterator) bool {

		row, err := r.Read()
		if err == io.EOF {
			c.File.Close()
			return false
		} else if err != nil {
			it.Context.CaptureErr(g.Error(err, "Error reading file"))
			return false
		}

		it.Row = make([]any, len(row))
		var val any
		for i, val0 := range row {
			if !it.ds.Columns[i].IsString() {
				val0 = strings.TrimSpace(val0)
				if val0 == "" {
					val = nil
				} else {
					val = val0
				}
			} else {
				val = val0
			}
			it.Row[i] = val
		}

		return true
	}

	ds.it = &Iterator{
		Row:      make([]any, len(ds.Columns)),
		nextFunc: nextFunc,
		Context:  g.NewContext(ds.Context.Ctx),
		ds:       ds,
	}

	err = ds.Start()
	if err != nil {
		return g.Error(err, "could start datastream")
	}

	return
}

// ResetStats resets the stats
func (ds *Datastream) ResetStats() {
	for i := range ds.Columns {
		// set totals to 0
		ds.Columns[i].Stats.TotalCnt = 0
		ds.Columns[i].Stats.NullCnt = 0
		ds.Columns[i].Stats.StringCnt = 0
		ds.Columns[i].Stats.JsonCnt = 0
		ds.Columns[i].Stats.IntCnt = 0
		ds.Columns[i].Stats.DecCnt = 0
		ds.Columns[i].Stats.BoolCnt = 0
		ds.Columns[i].Stats.DateCnt = 0
		ds.Columns[i].Stats.Checksum = 0
	}
}

// AddBytes add bytes as processed
func (ds *Datastream) AddBytes(b int64) {
	ds.Bytes = ds.Bytes + cast.ToUint64(b)
}

// Records return rows of maps
func (ds *Datastream) Records() <-chan map[string]any {
	chnl := make(chan map[string]any, 1000)
	ds.WaitReady()

	fields := ds.GetFields(true)

	go func() {
		defer close(chnl)

		for row := range ds.Rows() {
			// get records
			rec := map[string]any{}

			for i, field := range fields {
				rec[field] = row[i]
			}
			chnl <- rec
		}
	}()

	return chnl
}

// Chunk splits the datastream into chunk datastreams (in sequence)
func (ds *Datastream) Chunk(limit uint64) (chDs chan *Datastream) {
	chDs = make(chan *Datastream)
	if limit == 0 {
		limit = 200000
	}

	go func() {
		defer close(chDs)
		nDs := NewDatastreamContext(ds.Context.Ctx, ds.Columns)
		chDs <- nDs

		defer func() { nDs.Close() }()

	loop:
		for row := range ds.Rows() {
			select {
			case <-nDs.Context.Ctx.Done():
				break loop
			default:
				nDs.Push(row)

				if nDs.Count == limit {
					nDs.Close()
					nDs = NewDatastreamContext(ds.Context.Ctx, ds.Columns)
					chDs <- nDs
				}
			}
		}
		ds.SetEmpty()
	}()
	return
}

// Split splits the datastream into parallel datastreams
func (ds *Datastream) Split(numStreams ...int) (dss []*Datastream) {
	// TODO: Split freezes the flow in some situation, such as S3 -> PG.
	return []*Datastream{ds}

	conncurrency := lo.Ternary(
		os.Getenv("CONCURRENCY") != "",
		cast.ToInt(os.Getenv("CONCURRENCY")),
		runtime.NumCPU(),
	)
	if len(numStreams) > 0 {
		conncurrency = numStreams[0]
	}
	for i := 0; i < conncurrency; i++ {
		nDs := NewDatastreamContext(ds.Context.Ctx, ds.Columns)
		nDs.SetReady()
		dss = append(dss, nDs)
	}

	var nDs *Datastream
	go func() {
		defer ds.Close()
		i := 0
		for row := range ds.Rows() {
			if i == len(dss) {
				i = 0 // cycle through datastreams
			}

			nDs = dss[i]
			select {
			case <-nDs.Context.Ctx.Done():
				break
			default:
				nDs.Push(row)
			}
			i++
		}

		for _, nDs := range dss {
			go nDs.Close()
		}
		ds.SetEmpty()
	}()
	return
}

func (ds *Datastream) Pause() {
	if ds.Ready && !ds.closed {
		ds.pauseChan <- struct{}{}
		ds.paused = true
	}
}

// Unpause unpauses all streams
func (ds *Datastream) Unpause() {
	if ds.Ready && !ds.closed {
		ds.pauseChan <- struct{}{}
		ds.paused = false
	}
}

// Shape changes the column types as needed, to the provided columns var
// It will cast the already wrongly casted rows, and not recast the
// correctly casted rows
func (ds *Datastream) Shape(columns Columns) (nDs *Datastream, err error) {
	if len(columns) != len(ds.Columns) {
		err = g.Error("number of columns do not match")
		return ds, err
	}

	// we need to recast up to this marker
	counterMarker := ds.Count + 10

	// determine diff, and match order of target columns
	srcColNames := lo.Map(ds.Columns, func(c Column, i int) string { return strings.ToLower(c.Name) })
	diffCols := false
	colMap := map[int]int{}
	for i, col := range columns {
		j := lo.IndexOf(srcColNames, strings.ToLower(col.Name))
		if j == -1 {
			err = g.Error("column %s not found in source columns", col.Name)
			return ds, err
		}
		colMap[j] = i
		if columns[i].Type != ds.Columns[j].Type {
			diffCols = true
			ds.Columns[j].Type = columns[i].Type
		} else if columns[i].Name != ds.Columns[j].Name {
			diffCols = true
		} else if j != i {
			diffCols = true
		}
	}

	if !diffCols {
		return ds, nil
	}
	// return ds, nil

	g.Trace("shaping ds...")
	nDs = NewDatastreamContext(ds.Context.Ctx, columns)
	nDs.Inferred = ds.Inferred
	nDs.config = ds.config
	nDs.Sp.config = ds.Sp.config
	ds.df.ReplaceStream(ds, nDs)

	mapRowCol := func(row []interface{}) []interface{} {
		newRow := make([]interface{}, len(row))
		for o, t := range colMap {
			newRow[t] = row[o]
		}
		return newRow
	}

	go func() {
		defer nDs.Close()
		nDs.SetReady()
	loop:
		for row := range ds.Rows() {
			row = mapRowCol(row)
			if nDs.Count <= counterMarker {
				row = nDs.Sp.CastRow(row, nDs.Columns)
			}

			select {
			case <-nDs.Context.Ctx.Done():
				break loop
			default:
				nDs.Push(row)
			}
		}
		ds.SetEmpty()
	}()

	return
}

// Map applies the provided function to every row
// and returns the result
func (ds *Datastream) Map(newColumns Columns, transf func([]any) []any) (nDs *Datastream) {

	nDs = NewDatastreamContext(ds.Context.Ctx, newColumns)
	ds.df.ReplaceStream(ds, nDs)

	go func() {
		defer nDs.Close()

	loop:
		for row := range ds.Rows() {
			select {
			case <-nDs.Context.Ctx.Done():
				break loop
			default:
				nDs.Push(transf(row))
			}
		}
		ds.SetEmpty()
	}()

	return
}

// MapParallel applies the provided function to every row in parallel and returns the result. Order is not maintained.
func (ds *Datastream) MapParallel(transf func([]any) []any, numWorkers int) (nDs *Datastream) {
	var wg sync.WaitGroup
	nDs = NewDatastreamContext(ds.Context.Ctx, ds.Columns)

	transform := func(wDs *Datastream, wg *sync.WaitGroup) {
		defer wg.Done()

	loop:
		for row := range wDs.Rows() {
			select {
			case <-nDs.Context.Ctx.Done():
				break loop
			case <-wDs.Context.Ctx.Done():
				break loop
			default:
				nDs.Rows() <- transf(row)
				nDs.Count++
			}
		}
	}

	wStreams := map[int]*Datastream{}
	for i := 0; i < numWorkers; i++ {
		wStream := NewDatastreamContext(ds.Context.Ctx, ds.Columns)
		wStreams[i] = wStream

		wg.Add(1)
		go transform(wStream, &wg)
	}

	go func() {
		wi := 0

	loop:
		for row := range ds.Rows() {
			select {
			case <-nDs.Context.Ctx.Done():
				break loop
			default:
				wStreams[wi].Push(row)
			}
			if wi == numWorkers-1 {
				wi = -1 // cycle through workers
			}
			wi++
		}

		for i := 0; i < numWorkers; i++ {
			close(wStreams[i].Rows())
			wStreams[i].closed = true
		}

		wg.Wait()
		close(nDs.Rows())
		nDs.closed = true
	}()

	return
}

// NewCsvBytesChnl returns a channel yield chunk of bytes of csv
func (ds *Datastream) NewCsvBytesChnl(chunkRowSize int) (dataChn chan *[]byte) {
	dataChn = make(chan *[]byte, 100)

	go func() {
		defer close(dataChn)
		for {
			reader := ds.NewCsvReader(chunkRowSize, 0)
			data, err := ioutil.ReadAll(reader)
			if err != nil {
				ds.Context.CaptureErr(g.Error(err, "Error ioutil.ReadAll(reader)"))
				ds.Context.Cancel()
				return
			}
			dataChn <- &data
		}
	}()
	return dataChn
}

// NewCsvBufferReader creates a Reader with limit. If limit == 0, then read all rows.
func (ds *Datastream) NewCsvBufferReader(limit int, bytesLimit int64) *bytes.Reader {
	reader := ds.NewCsvReader(limit, bytesLimit)
	data, err := ioutil.ReadAll(reader) // puts data in memory
	if err != nil {
		ds.Context.CaptureErr(g.Error(err, "Error ioutil.ReadAll(reader)"))
	}
	return bytes.NewReader(data)
}

// NewCsvBufferReaderChnl provides a channel of readers as the limit is reached
// data is read in memory, whereas NewCsvReaderChnl does not hold in memory
func (ds *Datastream) NewCsvBufferReaderChnl(rowLimit int, bytesLimit int64) (readerChn chan *bytes.Reader) {

	readerChn = make(chan *bytes.Reader) // not buffered, will block if receiver isn't ready

	go func() {
		defer close(readerChn)
		for !ds.empty {
			readerChn <- ds.NewCsvBufferReader(rowLimit, bytesLimit)
		}
	}()

	return readerChn
}

// NewCsvReaderChnl provides a channel of readers as the limit is reached
// each channel flows as fast as the consumer consumes
func (ds *Datastream) NewCsvReaderChnl(rowLimit int, bytesLimit int64) (readerChn chan *io.PipeReader) {
	readerChn = make(chan *io.PipeReader, 100)

	pipeR, pipeW := io.Pipe()

	tbw := int64(0)

	mux := ds.Context.Mux
	df := ds.Df()
	if df != nil {
		mux = df.Context.Mux
	}
	_ = mux

	go func() {
		var w *csv.Writer
		defer close(readerChn)

		c := 0 // local counter

		nextPipe := func() error {

			pipeW.Close() // close the prior reader?
			tbw = 0       // reset

			// new reader
			c = 0
			pipeR, pipeW = io.Pipe()
			w = csv.NewWriter(pipeW)
			w.Comma = ','
			if ds.config.delimiter != "" {
				w.Comma = []rune(ds.config.delimiter)[0]
			}

			if ds.config.header {
				bw, err := w.Write(ds.GetFields(true, true))
				tbw = tbw + cast.ToInt64(bw)
				if err != nil {
					err = g.Error(err, "error writing header")
					ds.Context.CaptureErr(g.Error(err, "error writing header"))
					ds.Context.Cancel()
					pipeW.Close()
					return err
				}
			}
			readerChn <- pipeR
			return nil
		}

		for batch := range ds.BatchChan {

			err := nextPipe()
			if err != nil {
				return
			}

			// ensure that previous batch has same amount of columns
			if pBatch := batch.Previous; pBatch != nil {
				if len(pBatch.Columns) != len(batch.Columns) {
					err := g.Error("number of columns have changed across files")
					ds.Context.CaptureErr(err)
					ds.Context.Cancel()
					pipeW.Close()
					return
				}
			}

			for row0 := range batch.Rows {
				c++
				// convert to csv string
				row := make([]string, len(row0))
				if len(row0) > len(batch.Columns) {
					g.Warn("len(row) > len(ds.Columns)")
					g.Debug("%#v", row0)
					g.Debug("%#v", batch.Columns.Names())
				}
				for i, val := range row0 {
					row[i] = ds.Sp.CastToString(i, val, batch.Columns[i].Type)
				}
				mux.Lock()

				bw, err := w.Write(row)
				tbw = tbw + cast.ToInt64(bw)
				if err != nil {
					ds.Context.CaptureErr(g.Error(err, "error writing row"))
					ds.Context.Cancel()
					pipeW.Close()
					return
				}
				w.Flush()
				mux.Unlock()

				if (rowLimit > 0 && c >= rowLimit) || (bytesLimit > 0 && tbw >= bytesLimit) {
					err = nextPipe()
					if err != nil {
						return
					}
				}
			}

		}

		pipeW.Close()

	}()

	return readerChn
}

// NewJsonReaderChnl provides a channel of readers as the limit is reached
// each channel flows as fast as the consumer consumes
func (ds *Datastream) NewJsonReaderChnl(rowLimit int, bytesLimit int64) (readerChn chan *io.PipeReader) {
	readerChn = make(chan *io.PipeReader, 100)

	go func() {
		defer close(readerChn)

		c := 0 // local counter
		firstRec := true
		pipe := g.NewPipe()

		nextPipe := func() error {
			if c > 0 {
				pipe.Write([]byte("]")) // close array
			}
			pipe.Writer.Close() // close the prior reader?

			// new reader
			c = 0
			pipe = g.NewPipe()
			readerChn <- pipe.Reader

			_, err := pipe.Write([]byte("["))
			if err != nil {
				err = g.Error(err, "error writing row")
				ds.Context.CaptureErr(err)
				ds.Context.Cancel()
				pipe.Writer.Close()
				return err
			}
			firstRec = true
			return nil
		}

		w := json.NewEncoder(pipe)
		for batch := range ds.BatchChan {

			// open array
			err := nextPipe()
			if err != nil {
				ds.Context.CaptureErr(g.Error(err, "error writing row"))
				ds.Context.Cancel()
				pipe.Writer.Close()
				return
			}

			fields := batch.Columns.Names(true)
			for row0 := range batch.Rows {
				c++

				rec := g.M()
				for i, val := range row0 {
					rec[fields[i]] = val
				}

				if !firstRec {
					pipe.Write([]byte(",")) // comma in between records
				}

				err = w.Encode(rec)
				ds.Bytes = cast.ToUint64(pipe.BytesWritten)
				if err != nil {
					ds.Context.CaptureErr(g.Error(err, "error writing row"))
					ds.Context.Cancel()
					pipe.Writer.Close()
					return
				}
				firstRec = false

				if (rowLimit > 0 && c >= rowLimit) || (bytesLimit > 0 && pipe.BytesWritten >= bytesLimit) {
					err = nextPipe()
					if err != nil {
						ds.Context.CaptureErr(g.Error(err, "error writing row"))
						ds.Context.Cancel()
						pipe.Writer.Close()
						return
					}
				}
			}
		}
	}()

	return readerChn
}

// NewJsonLinesReaderChnl provides a channel of readers as the limit is reached
// each channel flows as fast as the consumer consumes
func (ds *Datastream) NewJsonLinesReaderChnl(rowLimit int, bytesLimit int64) (readerChn chan *io.PipeReader) {
	readerChn = make(chan *io.PipeReader, 100)

	pipe := g.NewPipe()

	readerChn <- pipe.Reader
	tbw := int64(0)

	go func() {
		defer close(readerChn)

		c := 0 // local counter

		for batch := range ds.BatchChan {
			fields := batch.Columns.Names(true)

			for row0 := range batch.Rows {
				c++

				rec := g.M()
				for i, val := range row0 {
					rec[fields[i]] = val
				}

				b, err := json.Marshal(rec)
				if err != nil {
					ds.Context.CaptureErr(g.Error(err, "error marshaling rec"))
					ds.Context.Cancel()
					pipe.Writer.Close()
					return
				}

				bw, err := pipe.Writer.Write(b)
				tbw = tbw + cast.ToInt64(bw)
				if err != nil {
					ds.Context.CaptureErr(g.Error(err, "error writing row"))
					ds.Context.Cancel()
					pipe.Writer.Close()
					return
				}

				if (rowLimit > 0 && c >= rowLimit) || (bytesLimit > 0 && tbw >= bytesLimit) {
					pipe.Writer.Close() // close the prior reader?
					tbw = 0             // reset

					// new reader
					c = 0
					pipe = g.NewPipe()
					readerChn <- pipe.Reader
				}
			}
		}
		pipe.Writer.Close()
	}()

	return readerChn
}

// NewCsvReader creates a Reader with limit. If limit == 0, then read all rows.
func (ds *Datastream) NewCsvReader(rowLimit int, bytesLimit int64) *io.PipeReader {
	pipeR, pipeW := io.Pipe()

	tbw := int64(0)
	go func() {
		defer pipeW.Close()

		// only process current batch
		var batch *Batch
		select {
		case batch = <-ds.BatchChan:
		default:
			batch = ds.CurrentBatch()
		}

		// ensure that previous batch has same amount of columns
		if pBatch := batch.Previous; pBatch != nil {
			if len(pBatch.Columns) != len(batch.Columns) {
				err := g.Error("number of columns have changed across files")
				ds.Context.CaptureErr(err)
				ds.Context.Cancel()
				pipeW.Close()
				return
			}
		}

		c := 0 // local counter
		w := csv.NewWriter(pipeW)
		w.Comma = ','
		if ds.config.delimiter != "" {
			w.Comma = []rune(ds.config.delimiter)[0]
		}

		if ds.config.header {
			bw, err := w.Write(batch.Columns.Names(true, true))
			tbw = tbw + cast.ToInt64(bw)
			if err != nil {
				ds.Context.CaptureErr(g.Error(err, "error writing header"))
				ds.Context.Cancel()
			}
		}

		for row0 := range batch.Rows {
			c++
			// convert to csv string
			row := make([]string, len(row0))
			for i, val := range row0 {
				row[i] = ds.Sp.CastToString(i, val, ds.Columns[i].Type)
			}
			bw, err := w.Write(row)
			tbw = tbw + cast.ToInt64(bw)
			if err != nil {
				ds.Context.CaptureErr(g.Error(err, "error writing row"))
				ds.Context.Cancel()
				break
			}
			w.Flush()

			if (rowLimit > 0 && c >= rowLimit) || (bytesLimit > 0 && tbw >= bytesLimit) {
				return // close reader if limit is reached
			}
		}

	}()

	return pipeR
}

func (it *Iterator) Ds() *Datastream {
	return it.ds
}

func (it *Iterator) close() {
	it.Closed = true
}

func (it *Iterator) next() bool {
	select {
	case <-it.Context.Ctx.Done():
		return false
	default:
		if it.Closed {
			return false
		}

		next := it.nextFunc(it)
		if next {
			it.Counter++

			// logic to improve perf but not checking if
			// df limit is reached each cycle
			if it.ds.df != nil && it.ds.df.Limit > 0 {
				it.limitCnt++

				if it.Counter > it.ds.df.Limit {
					return false
				} else if it.limitCnt >= 30 {
					// to check for limit reached each 30 rows
					it.limitCnt = 0
					if it.ds.df.Count() > it.ds.df.Limit {
						return false
					}
				}
			}
		}
		return next
	}
}
