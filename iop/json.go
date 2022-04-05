package iop

import (
	"io"
	"sort"

	"github.com/flarco/g"
	"github.com/nqd/flat"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

type decoderLike interface {
	Decode(obj interface{}) error
}

type jsonStream struct {
	ColumnMap map[string]*Column

	ds      *Datastream
	sp      *StreamProcessor
	decoder decoderLike
	flatten bool
	buffer  chan []interface{}
}

func NewJSONStream(ds *Datastream, decoder decoderLike, flatten bool) *jsonStream {
	js := &jsonStream{
		ColumnMap: map[string]*Column{},
		ds:        ds,
		decoder:   decoder,
		flatten:   flatten,
		buffer:    make(chan []interface{}, 100000),
		sp:        NewStreamProcessor(),
	}

	return js
}

func (js *jsonStream) nextFunc(it *Iterator) bool {
	var recordsInterf []map[string]interface{}
	if it.Closed {
		return false
	}

	select {
	case row := <-js.buffer:
		it.Row = row
		return true
	default:
	}

	var payload interface{}
	err := js.decoder.Decode(&payload)
	if err == io.EOF {
		return false
	} else if err != nil {
		it.Context.CaptureErr(g.Error(err, "could not decode JSON body"))
		return false
	}

	switch payloadV := payload.(type) {
	case map[string]interface{}:
		// is one record
		recordsInterf = []map[string]interface{}{payloadV}
	case map[interface{}]interface{}:
		// is one record
		interf := map[string]interface{}{}
		for k, v := range payloadV {
			interf[cast.ToString(k)] = v
		}
		recordsInterf = []map[string]interface{}{interf}
	case []interface{}:
		recordsInterf = []map[string]interface{}{}
		recList := payloadV
		if len(recList) == 0 {
			return false
		}

		switch recList[0].(type) {
		case map[interface{}]interface{}:
			for _, rec := range recList {
				newRec := map[string]interface{}{}
				for k, v := range rec.(map[interface{}]interface{}) {
					newRec[cast.ToString(k)] = v
				}
				recordsInterf = append(recordsInterf, newRec)
			}
		case map[string]interface{}:
			for _, val := range recList {
				recordsInterf = append(recordsInterf, val.(map[string]interface{}))
			}
		default:
			// is array of single values
			for _, val := range recList {
				recordsInterf = append(recordsInterf, map[string]interface{}{"data": val})
			}
		}

	case []map[interface{}]interface{}:
		recordsInterf = []map[string]interface{}{}
		for _, rec := range payloadV {
			newRec := map[string]interface{}{}
			for k, v := range rec {
				newRec[cast.ToString(k)] = v
			}
			recordsInterf = append(recordsInterf, newRec)
		}
	case []map[string]interface{}:
		recordsInterf = payloadV
	default:
		err = g.Error("unhandled JSON interface type: %#v", payloadV)
		it.Context.CaptureErr(err)
		return false
	}

	// parse records
	js.parseRecords(recordsInterf)

	if err = it.Context.Err(); err != nil {
		err = g.Error(err, "error parsing records")
		it.Context.CaptureErr(err)
		return false
	}

	select {
	// wait for row
	case row := <-js.buffer:
		it.Row = row
		return true
	}
}

func (js *jsonStream) parseRecords(records []map[string]interface{}) {

	for _, rec := range records {
		newRec, _ := flat.Flatten(rec, &flat.Options{Delimiter: "__", Safe: true})
		keys := lo.Keys(newRec)
		sort.Strings(keys)

		row := make([]interface{}, len(js.ds.Columns))
		for _, colName := range keys {
			col, ok := js.ColumnMap[colName]
			if !ok {
				col = &Column{
					Name:     colName,
					Type:     "string",
					Position: len(js.ds.Columns) + 1,
				}
				js.ds.Columns = append(js.ds.Columns, *col)
				js.ColumnMap[colName] = col
				row = append(row, nil)
			}
			i := col.Position - 1
			row[i] = newRec[colName]
			if arr, ok := row[i].([]interface{}); ok {
				row[i] = g.Marshal(arr)
			}
		}
		js.buffer <- row
	}
	// g.Debug("JSON Stream -> Parsed %d records", len(records))
}
