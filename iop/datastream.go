package iop

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"github.com/flarco/g"
	"github.com/flarco/g/csv"
	"github.com/spf13/cast"
)

// Datastream is a stream of rows
type Datastream struct {
	Columns       Columns
	Rows          chan []interface{}
	Buffer        [][]interface{}
	Count         uint64
	Context       g.Context
	Ready         bool
	Bytes         int64
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
}

// Iterator is the row provider for a datastream
type Iterator struct {
	Row      []interface{}
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
		Row:      make([]interface{}, len(columns)),
		nextFunc: nextFunc,
		Context:  g.NewContext(ctx),
		ds:       ds,
	}
	return
}

// NewDatastreamContext return a new datastream
func NewDatastreamContext(ctx context.Context, columns Columns) (ds *Datastream) {
	ds = &Datastream{
		Rows:       MakeRowsChan(),
		Columns:    columns,
		Context:    g.NewContext(ctx),
		Sp:         NewStreamProcessor(),
		config:     &streamConfig{emptyAsNull: true, header: true, delimiter: ","},
		deferFuncs: []func(){},
		clones:     []*Datastream{},
	}
	ds.Sp.ds = ds

	return
}

// SetEmpty sets the ds.Rows channel as empty
func (ds *Datastream) SetEmpty() {
	ds.empty = true
	if ds.df != nil && ds.df.IsEmpty() {
		g.Trace("executing defer functions")
		ds.df.mux.Lock()
		defer ds.df.mux.Unlock()
		for i, f := range ds.df.deferFuncs {
			f()
			ds.df.deferFuncs[i] = func() { return } // in case it gets called again
		}
	}
}

// SetConfig sets the ds.config values
func (ds *Datastream) SetConfig(configMap map[string]string) {
	ds.Sp.SetConfig(configMap)
	ds.config = ds.Sp.config
}

// IsEmpty returns true is ds.Rows channel as empty
func (ds *Datastream) IsEmpty() bool {
	return ds.empty
}

// Push return the fields of the Data
func (ds *Datastream) Push(row []interface{}) {
	if !ds.Ready {
		ds.Ready = true
	} else if ds.closed {
		return
	}
	select {
	case <-ds.Context.Ctx.Done():
		ds.Close()
		return
	default:
		ds.Rows <- row
	}
	for _, cDs := range ds.clones {
		cDs.Push(row)
	}
	ds.Count++
}

// Clone returns a new datastream of the same source
func (ds *Datastream) Clone() *Datastream {
	cDs := NewDatastreamContext(ds.Context.Ctx, ds.Columns)
	cDs.Inferred = ds.Inferred
	cDs.config = ds.config
	cDs.Sp.config = ds.Sp.config
	cDs.Ready = true
	ds.clones = append(ds.clones, cDs)
	return cDs
}

// IsClosed is true is ds is closed
func (ds *Datastream) IsClosed() bool {
	return ds.closed
}

// WaitReady waits until datastream is ready
func (ds *Datastream) WaitReady() error {
loop:
	for {
		select {
		case <-ds.Context.Ctx.Done():
			break loop
		default:
		}

		if ds.Context.Err() != nil {
			return ds.Context.Err()
		}

		if ds.Ready {
			break loop
		}

		// likelyhood of lock lessens. Unsure why
		// It seems that ds.Columns collides
		time.Sleep(50 * time.Millisecond)
	}

	return ds.Context.Ctx.Err()
}

// Defer runs a given function as close of Datastream
func (ds *Datastream) Defer(f func()) {
	ds.deferFuncs = append(ds.deferFuncs, f)
	if ds.closed { // mutex?
		for _, f := range ds.deferFuncs {
			f()
		}
	}
}

// Close closes the datastream
func (ds *Datastream) Close() {
	if !ds.closed {
		close(ds.Rows)

		for _, f := range ds.deferFuncs {
			f()
		}

		if ds.Sp.unrecognizedDate != "" {
			g.Warn("unrecognized date format (%s)", ds.Sp.unrecognizedDate)
		}
		ds.Buffer = nil
	}
	if ds.it != nil {
		ds.it.close()
	}
	ds.closed = true
}

// GetFields return the fields of the Data
func (ds *Datastream) GetFields(toLower ...bool) []string {
	lower := false
	if len(toLower) > 0 {
		lower = toLower[0]
	}
	fields := make([]string, len(ds.Columns))

	for j, column := range ds.Columns {
		if lower {
			fields[j] = strings.ToLower(column.Name)
		} else {
			fields[j] = column.Name
		}
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

func (ds *Datastream) setFields(fields []string) {
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
	for {
		if ds.Ready {
			break
		}

		if ds.Err() != nil {
			return data, g.Error(ds.Err())
		}

		time.Sleep(50 * time.Millisecond)
	}

	data.Result = nil
	data.Columns = ds.Columns
	data.Rows = [][]interface{}{}
	limited := false

	for row := range ds.Rows {
		data.Rows = append(data.Rows, row)
		if limit > 0 && len(data.Rows) == limit {
			limited = true
			break
		}
	}

	if !limited {
		ds.SetEmpty()
	}
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

	if !ds.Inferred {
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
				row := ds.Sp.ProcessRow(ds.it.Row)
				ds.Buffer = append(ds.Buffer, row)
				if ds.it.Counter >= cast.ToUint64(SampleSize) {
					break loop
				}
			}
		}

		// infer types
		sampleData := NewDataset(ds.Columns)
		sampleData.Rows = ds.Buffer
		sampleData.NoTrace = ds.NoTrace
		sampleData.SafeInference = ds.SafeInference
		sampleData.Sp.dateLayouts = ds.Sp.dateLayouts
		sampleData.InferColumnTypes()
		ds.Columns = sampleData.Columns
	}

	if ds.it.Context.Err() != nil {
		err = g.Error(ds.it.Context.Err(), "error in getting rows")
		return
	}

	go func() {
		var err error
		defer ds.Close()

		ds.Ready = true

		for _, row := range ds.Buffer {
			row = ds.Sp.CastRow(row, ds.Columns)
			if ds.config.skipBlankLines && ds.Sp.rowBlankValCnt == len(row) {
				continue
			}
			ds.Push(row)
		}

		row := make([]interface{}, len(ds.Columns))
		rowPtrs := make([]interface{}, len(ds.Columns))
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
			row = ds.Sp.CastRow(ds.it.Row, ds.Columns)
			if ds.config.skipBlankLines && ds.Sp.rowBlankValCnt == len(row) {
				continue
			}

			select {
			case <-ds.Context.Ctx.Done():
				break loop
			case <-ds.it.Context.Ctx.Done():
				if ds.it.Context.Err() != nil {
					err = g.Error(ds.it.Context.Err())
					ds.Context.CaptureErr(err, "Failed to scan")
				}
				break loop
			default:
				ds.Push(row)
			}
		}
		if !ds.NoTrace {
			g.Trace("Got %d rows", ds.it.Counter)
		}
	}()

	return
}

// ConsumeJsonReader uses the provided reader to stream JSON
// This will put each JSON rec as one string value
// so payload can be processed downstream
func (ds *Datastream) ConsumeJsonReader(reader io.Reader) (err error) {
	reader2, err := Decompress(reader)
	if err != nil {
		return g.Error(err, "Could not decompress reader")
	}

	ds.SetFields([]string{"data"})
	ds.Columns[0].Type = "json"
	ds.Inferred = true
	decoder := json.NewDecoder(reader2)
	nextFunc := func(it *Iterator) bool {

		var rec interface{}
		err = decoder.Decode(&rec)
		if err == io.EOF {
			return false
		} else if err != nil {
			it.Context.CaptureErr(g.Error(err, "could not decode JSON body"))
			return false
		}
		row := []string{g.Marshal(rec)}
		it.Row = make([]interface{}, len(it.ds.Columns))
		var val interface{}
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
		Row:      make([]interface{}, len(ds.Columns)),
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

// ConsumeCsvReader uses the provided reader to stream rows
func (ds *Datastream) ConsumeCsvReader(reader io.Reader) (err error) {
	c := CSV{Reader: reader, NoHeader: !ds.config.header}

	r, err := c.getReader()
	if err != nil {
		err = g.Error(err, "could not get reader")
		ds.Context.CaptureErr(err)
		return err
	}

	row0, err := r.Read()
	if err == io.EOF {
		g.Warn("csv stream provided is empty")
		ds.Ready = true
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
			it.ds.AddBytes(c.bytes)
			c.File.Close()
			return false
		} else if err != nil {
			it.Context.CaptureErr(g.Error(err, "Error reading file"))
			return false
		}

		it.Row = make([]interface{}, len(it.ds.Columns))
		var val interface{}
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
		Row:      make([]interface{}, len(ds.Columns)),
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
		ds.Columns[i].Stats.IntCnt = 0
		ds.Columns[i].Stats.DecCnt = 0
		ds.Columns[i].Stats.BoolCnt = 0
		ds.Columns[i].Stats.DateCnt = 0
		ds.Columns[i].Stats.Checksum = 0
	}
}

// AddBytes add bytes as processed
func (ds *Datastream) AddBytes(b int64) {
	ds.Bytes = ds.Bytes + b
}

// Records return rows of maps
func (ds *Datastream) Records() <-chan map[string]interface{} {
	chnl := make(chan map[string]interface{}, 1000)
	ds.WaitReady()
	go func() {
		defer close(chnl)

		for row := range ds.Rows {
			// get records
			rec := map[string]interface{}{}

			for i, field := range ds.GetFields() {
				rec[strings.ToLower(field)] = row[i]
			}
			chnl <- rec
		}
	}()

	return chnl
}

// Chunk splits the datastream into chunk datastreams
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
		for row := range ds.Rows {
			nDs.Ready = true

			select {
			case <-nDs.Context.Ctx.Done():
				break loop
			default:
				nDs.Rows <- row
				nDs.Count++

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

	diffCols := false
	for i := range columns {
		if columns[i].Type != ds.Columns[i].Type {
			diffCols = true
			ds.Columns[i].Type = columns[i].Type
		} else if columns[i].Name != ds.Columns[i].Name {
			diffCols = true
		}
	}

	if !diffCols {
		return ds, nil
	}

	g.Trace("shaping ds...")
	nDs = NewDatastreamContext(ds.Context.Ctx, columns)
	nDs.Inferred = ds.Inferred
	nDs.config = ds.config
	nDs.Sp.config = ds.Sp.config

	go func() {
		defer nDs.Close()
		nDs.Ready = true

	loop:
		for row := range ds.Rows {
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
func (ds *Datastream) Map(newColumns Columns, transf func([]interface{}) []interface{}) (nDs *Datastream) {

	nDs = NewDatastreamContext(ds.Context.Ctx, newColumns)

	go func() {
		defer nDs.Close()

	loop:
		for row := range ds.Rows {
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
func (ds *Datastream) MapParallel(transf func([]interface{}) []interface{}, numWorkers int) (nDs *Datastream) {
	var wg sync.WaitGroup
	nDs = NewDatastreamContext(ds.Context.Ctx, ds.Columns)

	transform := func(wDs *Datastream, wg *sync.WaitGroup) {
		defer wg.Done()

	loop:
		for row := range wDs.Rows {
			select {
			case <-nDs.Context.Ctx.Done():
				break loop
			case <-wDs.Context.Ctx.Done():
				break loop
			default:
				nDs.Rows <- transf(row)
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
		for row := range ds.Rows {
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
			close(wStreams[i].Rows)
			wStreams[i].closed = true
		}

		wg.Wait()
		close(nDs.Rows)
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
	data, err := ioutil.ReadAll(reader)
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
		for !ds.IsEmpty() {
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

	readerChn <- pipeR
	tbw := int64(0)

	go func() {
		defer close(readerChn)
		defer func() { ds.AddBytes(tbw) }()

		c := 0 // local counter
		w := csv.NewWriter(pipeW)
		w.Comma = []rune(ds.config.delimiter)[0]

		if ds.config.header {
			bw, err := w.Write(ds.GetFields(true))
			tbw = tbw + cast.ToInt64(bw)
			if err != nil {
				ds.Context.CaptureErr(g.Error(err, "error writing header"))
				ds.Context.Cancel()
				pipeW.Close()
				return
			}
		}

		for row0 := range ds.Rows {
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
				pipeW.Close()
				return
			}
			w.Flush()

			if (rowLimit > 0 && c >= rowLimit) || (bytesLimit > 0 && tbw >= bytesLimit) {
				pipeW.Close() // close the prior reader?
				ds.AddBytes(tbw)
				tbw = 0 // reset

				// new reader
				c = 0
				pipeR, pipeW = io.Pipe()
				w = csv.NewWriter(pipeW)

				bw, err = w.Write(ds.GetFields())
				tbw = tbw + cast.ToInt64(bw)
				if err != nil {
					ds.Context.CaptureErr(g.Error(err, "error writing header"))
					ds.Context.Cancel()
					pipeW.Close()
					return
				}
				readerChn <- pipeR
			}
		}
		pipeW.Close()
	}()

	return readerChn
}

// NewCsvReader creates a Reader with limit. If limit == 0, then read all rows.
func (ds *Datastream) NewCsvReader(rowLimit int, bytesLimit int64) *io.PipeReader {
	pipeR, pipeW := io.Pipe()

	tbw := int64(0)
	go func() {
		defer pipeW.Close()
		defer func() { ds.AddBytes(tbw) }()

		c := 0 // local counter
		w := csv.NewWriter(pipeW)
		w.Comma = []rune(ds.config.delimiter)[0]

		// header row to lower case
		fields := []string{}
		for _, field := range ds.GetFields(true) {
			fields = append(fields, strings.ToLower(field))
		}

		if ds.config.header {
			bw, err := w.Write(fields)
			tbw = tbw + cast.ToInt64(bw)
			if err != nil {
				ds.Context.CaptureErr(g.Error(err, "error writing header"))
				ds.Context.Cancel()
			}
		}

		limited := false // need this to know if channel is emptied
		for row0 := range ds.Rows {
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
				limited = true
				break // close reader if limit is reached
			}
		}

		if !limited {
			ds.SetEmpty()
		}

	}()

	return pipeR
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

// Collect reads from one or more streams and return a dataset
func Collect(dss ...*Datastream) (data Dataset, err error) {
	var datas []Dataset

	if len(dss) == 0 {
		return
	}

	for i, ds := range dss {
		d, err := ds.Collect(0)
		if err != nil {
			return NewDataset(nil), g.Error(err, "Error collecting ds")
		}

		if i > 0 && len(datas[i-1].Columns) != len(d.Columns) {
			err := g.Error(
				"columns mismatch, %#v != %#v",
				datas[i-1].GetFields(), d.GetFields(),
			)
			return NewDataset(nil), g.Error(err, "Error appending ds")
		}
		datas = append(datas, d)
	}

	data.Result = nil
	data.Columns = datas[0].Columns
	data.Rows = [][]interface{}{}

	for _, d := range datas {
		data.Rows = append(data.Rows, d.Rows...)
	}

	return
}
