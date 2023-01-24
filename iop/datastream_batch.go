package iop

import (
	"strings"

	"github.com/flarco/g"
	"github.com/samber/lo"
)

type Batch struct {
	id         int
	Columns    Columns
	Rows       chan []any
	Previous   *Batch
	ds         *Datastream
	closed     bool
	closeChan  chan struct{}
	transforms []func(row []any) []any
}

// NewBatch create new batch with fixed columns
// should be used each time column type changes, or columns are added
func (ds *Datastream) NewBatch(columns Columns) *Batch {
	batch := &Batch{
		id:         len(ds.Batches),
		Columns:    columns,
		Rows:       MakeRowsChan(),
		Previous:   ds.CurrentBatch(),
		ds:         ds,
		closeChan:  make(chan struct{}),
		transforms: []func(row []any) []any{},
	}

	if batch.Previous != nil && !batch.Previous.closed {
		batch.Previous.ds.Pause()
		batch.Previous.Close() // close previous batch
		batch.Previous.ds.Unpause()
	}
	ds.Batches = append(ds.Batches, batch)
	ds.BatchChan <- batch
	if !ds.NoTrace {
		g.DebugLow("new batch %s", batch.ID())
	}
	return batch
}

func (ds *Datastream) CurrentBatch() *Batch {
	if len(ds.Batches) > 0 {
		return ds.Batches[len(ds.Batches)-1]
	}
	return nil
}

func (b *Batch) ID() string {
	return g.F("%s-%d", b.ds.ID, b.id)
}

func (b *Batch) IsFirst() bool {
	return b.id == 0
}

func (b *Batch) Close() {
	if !b.closed {
		go func() { b.closeChan <- struct{}{} }()
		b.closed = true
		close(b.Rows)
		if !b.ds.NoTrace {
			g.DebugLow("closed %s", b.ID())
		}
	}
}

func (b *Batch) ColumnsChanged() bool {
	if pB := b.Previous; pB != nil {
		if len(pB.Columns) != len(b.Columns) {
			return true
		}
		for i := range b.Columns {
			if b.Columns[i].Type != pB.Columns[i].Type {
				return true
			} else if b.Columns[i].Name != pB.Columns[i].Name {
				return true
			}
		}
	}
	return false
}

func (b *Batch) Shape(columns Columns) (err error) {
	if len(columns) != len(b.Columns) {
		return g.Error("number of columns do not match")
	}

	// determine diff, and match order of target columns
	srcColNames := lo.Map(b.Columns, func(c Column, i int) string { return strings.ToLower(c.Name) })
	diffCols := false
	colMap := map[int]int{}
	for i, col := range columns {
		j := lo.IndexOf(srcColNames, strings.ToLower(col.Name))
		if j == -1 {
			return g.Error("column %s not found in source columns", col.Name)
		}
		colMap[j] = i
		if columns[i].Type != b.Columns[j].Type {
			diffCols = true
			b.Columns[j].Type = columns[i].Type
		} else if columns[i].Name != b.Columns[j].Name {
			diffCols = true
		} else if j != i {
			diffCols = true
		}
	}

	if !diffCols {
		return nil
	}

	mapRowCol := func(row []any) []any {
		// g.PP(colMap)
		// g.P(row)
		// g.Warn("len(row) = %d", len(row))
		// g.Warn("len(b.Columns) = %d", len(b.Columns))
		for len(row) < len(b.Columns) {
			row = append(row, nil)
		}
		newRow := make([]any, len(row))
		for o, t := range colMap {
			newRow[t] = row[o]
		}
		return newRow
	}

	b.ds.Pause()
	b.transforms = append(b.transforms, mapRowCol)
	b.ds.Unpause()

	return nil
}

func (b *Batch) Push(row []any) {
	if b.closed {
		return
	}
retry:
	newRow := row
	for _, f := range b.transforms {
		newRow = f(newRow) // allows transformations
	}
	select {
	case <-b.ds.Context.Ctx.Done():
		b.ds.Close()
		return
	case <-b.ds.pauseChan:
		<-b.ds.pauseChan // wait for unpause
		if b.closed {
			b.ds.it.Reprocess <- row
			return
		}
		goto retry
	case <-b.closeChan:
		b.ds.it.Reprocess <- row
	case <-b.ds.schemaChgChan:
		b.Close()
		b.ds.it.Reprocess <- row
	case b.Rows <- newRow:
		b.ds.bwRows <- newRow
	}
	b.ds.Count++
}
