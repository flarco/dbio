package iop

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/flarco/g"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"github.com/spf13/cast"
	pb "gopkg.in/cheggaaa/pb.v2"
)

// https://github.com/cheggaaa/pb/blob/master/v3/element.go
// calculates the RAM percent
var elementMem pb.ElementFunc = func(state *pb.State, args ...string) string {
	memRAM, err := mem.VirtualMemory()
	if err != nil {
		return ""
	}
	return g.F("| %d%% MEM", cast.ToInt(memRAM.UsedPercent))
}

// calculates the CPU percent
var elementCPU pb.ElementFunc = func(state *pb.State, args ...string) string {
	cpuPct, err := cpu.Percent(0, false)
	if err != nil || len(cpuPct) == 0 {
		return ""
	}
	return g.F("| %d%% CPU", cast.ToInt(cpuPct[0]))
}

var elementStatus pb.ElementFunc = func(state *pb.State, args ...string) string {
	status := cast.ToString(state.Get("status"))
	if status == "" {
		return ""
	}
	return g.F("| %s", status)
}

type argsHelper []string

func (args argsHelper) getOr(n int, value string) string {
	if len(args) > n {
		return args[n]
	}
	return value
}

func (args argsHelper) getNotEmptyOr(n int, value string) (v string) {
	if v = args.getOr(n, value); v == "" {
		return value
	}
	return
}

var elementCounters pb.ElementFunc = func(state *pb.State, args ...string) string {
	var f string
	if state.Total() > 0 {
		f = argsHelper(args).getNotEmptyOr(0, "%s / %s")
	} else {
		f = argsHelper(args).getNotEmptyOr(1, "%[1]s")
	}
	return fmt.Sprintf(
		f, humanize.Commaf(cast.ToFloat64(state.Value())),
		humanize.Commaf(cast.ToFloat64(state.Total())),
	)
}

var elementBytes pb.ElementFunc = func(state *pb.State, args ...string) string {
	bytes := cast.ToUint64(state.Get("bytes"))
	if bytes == 0 {
		return ""
	}
	return g.F("| %s", humanize.Bytes(bytes))
}

// NewPBar creates a new progress bar
func NewPBar(d time.Duration) *pb.ProgressBar {
	pbar := new(pb.ProgressBar)
	pb.RegisterElement("status", elementStatus, true)
	pb.RegisterElement("counters", elementCounters, true)
	// pb.RegisterElement("bytes", elementBytes, true)
	tmpl := `{{etime . "%s" | yellow }} {{counters . }} {{speed . "%s r/s" | green }} {{{ bytes . }} { status . }}`
	tmpl = `{{etime . "%s" | yellow }} {{counters . }} {{speed . "%s r/s" | green }} { status . }}`
	if g.IsDebugLow() {
		pb.RegisterElement("mem", elementMem, true)
		pb.RegisterElement("cpu", elementCPU, true)
		tmpl = `{{etime . "%s" | yellow }} {{counters . }} {{speed . "%s r/s" | green }} {{{ bytes . }} {{ mem . }} {{ cpu . }} {{ status . }}`
		tmpl = `{{etime . "%s" | yellow }} {{counters . }} {{speed . "%s r/s" | green }} {{{ mem . }} {{ cpu . }} {{ status . }}`
	}
	barTmpl := pb.ProgressBarTemplate(tmpl)
	pbar = barTmpl.New(0)
	pbar.SetRefreshRate(d)
	pbar.SetWidth(40)
	return pbar
}

// Dataflow is a collection of concurrent Datastreams
type Dataflow struct {
	Columns    []Column
	Buffer     [][]interface{}
	StreamCh   chan *Datastream
	Streams    []*Datastream
	Context    g.Context
	Limit      uint64
	deferFuncs []func()
	Ready      bool
	Inferred   bool
	FsURL      string
	closed     bool
	pbar       *pb.ProgressBar
	mux        sync.Mutex
}

// NewDataflow creates a new dataflow
func NewDataflow(limit ...int) (df *Dataflow) {

	Limit := uint64(0) // infinite
	if len(limit) > 0 && limit[0] != 0 {
		Limit = cast.ToUint64(limit[0])
	}

	df = &Dataflow{
		StreamCh:   make(chan *Datastream, 100),
		Streams:    []*Datastream{},
		Context:    g.NewContext(context.Background()),
		Limit:      Limit,
		deferFuncs: []func(){},
	}

	if ShowProgress {
		// progress bar ticker
		df.pbar = NewPBar(time.Second)
		ticker := time.NewTicker(1 * time.Second)
		go func() {
			for {
				select {
				case <-ticker.C:
					cnt := df.Count()
					if cnt > 1000 {
						df.pbar.Start()
						df.pbar.SetCurrent(cast.ToInt64(cnt))
						df.pbar.Set("bytes", df.Bytes())
					}
				default:
					time.Sleep(100 * time.Millisecond)
					if df.IsEmpty() || df.Err() != nil {
						df.pbar.SetCurrent(cast.ToInt64(df.Count()))
						df.pbar.Finish()
						return
					}
				}
			}
		}()
	}

	return
}

// Err return the error if any
func (df *Dataflow) Err() (err error) {
	return df.Context.Err()
}

// IsClosed is true is ds is closed
func (df *Dataflow) IsClosed() bool {
	return df.closed
}

// Defer runs a given function as close of Dataflow
func (df *Dataflow) Defer(f func()) {
	df.mux.Lock()
	defer df.mux.Unlock()
	df.deferFuncs = append(df.deferFuncs, f)
}

// Close closes the df
func (df *Dataflow) Close() {
	if !df.closed {
		close(df.StreamCh)
	}
	df.closed = true
}

// SetEmpty sets all underlying datastreams empty
func (df *Dataflow) SetEmpty() {
	for _, ds := range df.Streams {
		ds.SetEmpty()
	}
}

// IsEmpty returns true is ds.Rows of all channels as empty
func (df *Dataflow) IsEmpty() bool {
	df.mux.Lock()
	defer df.mux.Unlock()
	for _, ds := range df.Streams {
		if ds != nil && ds.Ready {
			if !ds.IsEmpty() {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

// SetColumns sets the columns
func (df *Dataflow) SetColumns(columns []Column) {
	df.Columns = columns
	// for i := range df.Streams {
	// 	df.Streams[i].Columns = columns
	// 	df.Streams[i].Inferred = true
	// }
}

// ResetStats resets the stats
func (df *Dataflow) ResetStats() {
	for i := range df.Columns {
		// set totals to 0
		df.Columns[i].Stats.TotalCnt = 0
		df.Columns[i].Stats.NullCnt = 0
		df.Columns[i].Stats.StringCnt = 0
		df.Columns[i].Stats.IntCnt = 0
		df.Columns[i].Stats.DecCnt = 0
		df.Columns[i].Stats.BoolCnt = 0
		df.Columns[i].Stats.DateCnt = 0
		df.Columns[i].Stats.Checksum = 0
	}
}

// SyncStats sync stream processor stats aggregated to the df.Columns
func (df *Dataflow) SyncStats() {
	df.ResetStats()

	for _, ds := range df.Streams {
		for i, colStat := range ds.Sp.colStats {
			if i+1 > len(df.Columns) {
				g.Debug("index %d is outside len of array (%d) in SyncStats", i, len(df.Columns))
				continue
			}
			df.Columns[i].Stats.TotalCnt = df.Columns[i].Stats.TotalCnt + colStat.TotalCnt
			df.Columns[i].Stats.NullCnt = df.Columns[i].Stats.NullCnt + colStat.NullCnt
			df.Columns[i].Stats.StringCnt = df.Columns[i].Stats.StringCnt + colStat.StringCnt
			df.Columns[i].Stats.IntCnt = df.Columns[i].Stats.IntCnt + colStat.IntCnt
			df.Columns[i].Stats.DecCnt = df.Columns[i].Stats.DecCnt + colStat.DecCnt
			df.Columns[i].Stats.BoolCnt = df.Columns[i].Stats.BoolCnt + colStat.BoolCnt
			df.Columns[i].Stats.DateCnt = df.Columns[i].Stats.DateCnt + colStat.DateCnt
			df.Columns[i].Stats.Checksum = df.Columns[i].Stats.Checksum + colStat.Checksum

			if colStat.Min < df.Columns[i].Stats.Min {
				df.Columns[i].Stats.Min = colStat.Min
			}
			if colStat.Max > df.Columns[i].Stats.Max {
				df.Columns[i].Stats.Max = colStat.Max
			}
			if colStat.MaxLen > df.Columns[i].Stats.MaxLen {
				df.Columns[i].Stats.MaxLen = colStat.MaxLen
			}
			if colStat.MaxDecLen > df.Columns[i].Stats.MaxDecLen {
				df.Columns[i].Stats.MaxDecLen = colStat.MaxDecLen
			}
		}
	}

	if !df.Inferred {
		df.Columns = InferFromStats(df.Columns, false, false)
		df.Inferred = true
	}
}

// Count returns the aggregate count
func (df *Dataflow) Count() (cnt uint64) {
	if df != nil && df.Ready {
		for _, ds := range df.Streams {
			if ds.Ready {
				cnt += ds.Count
			}
		}
	}
	return
}

// Bytes returns the aggregate bytes
func (df *Dataflow) Bytes() (bytes uint64) {
	if df != nil && df.Ready {
		for _, ds := range df.Streams {
			if ds.Ready {
				bytes += ds.Bytes
			}
		}
	}
	return
}

// Size is the number of streams
func (df *Dataflow) Size() int {
	return len(df.Streams)
}

// PushStreams waits until each datastream is ready, then adds them to the dataflow. Should be launched as a goroutine
func (df *Dataflow) PushStreams(dss ...*Datastream) {

	df.mux.Lock()
	df.Streams = append(df.Streams, dss...)
	df.mux.Unlock()

	pushed := map[int]bool{}
	pushCnt := 0
	for {
		for i, ds := range dss {
			if pushed[i] || df.closed {
				continue
			}

			if df.Err() != nil {
				df.Close()
				return
			}

			if ds.Err() != nil {
				df.Context.CaptureErr(ds.Err())
				df.Close()
				return
			}

			select {
			case <-df.Context.Ctx.Done():
				df.Close()
				return
			case <-ds.Context.Ctx.Done():
				df.Close()
				return
			default:
				// wait for first ds to start streaming.
				// columns/buffer need to be populated
				if ds.Ready {
					ds.df = df
					if df.Ready {
						// compare columns, if differences than error
						if err := CompareColumns(df.Columns, ds.Columns); err != nil {
							df.Context.CaptureErr(g.Error(err, "files columns don't match"))
							df.Close()
							return
						}
					} else {
						df.Columns = ds.Columns
						df.Buffer = ds.Buffer
						df.Inferred = ds.Inferred
						df.Ready = true
					}

					select {
					case df.StreamCh <- ds:
						pushed[i] = true
						pushCnt++
						g.Trace("pushed dss %d", i)
					default:
					}
				}
			}

			// likelyhood of lock lessens. Unsure why
			// It seems that ds.Columns collides
			time.Sleep(50 * time.Millisecond)
		}

		if pushCnt == len(dss) || df.closed {
			break
		}
	}
}

// WaitReady waits until dataflow is ready
func (df *Dataflow) WaitReady() error {
	// wait for first ds to start streaming.
	// columns need to be populated
	for {
		if df.Ready {
			break
		}

		if df.Context.Err() != nil {
			return df.Context.Err()
		}

		// likelyhood of lock lessens. Unsure why
		// It seems that df.Columns collides
		time.Sleep(50 * time.Millisecond)
	}

	return nil
}

// MergeDataflow merges the dataflow streams into one
func MergeDataflow(df *Dataflow) (ds *Datastream) {

	ds = NewDatastream(df.Columns)

	go func() {
		ds.Ready = true
		defer ds.Close()

	loop:
		for ds0 := range df.StreamCh {
			for row := range ds0.Rows {
				select {
				case <-df.Context.Ctx.Done():
					break loop
				case <-ds.Context.Ctx.Done():
					break loop
				default:
					ds.Push(row)
				}
			}
		}
	}()

	return ds
}
