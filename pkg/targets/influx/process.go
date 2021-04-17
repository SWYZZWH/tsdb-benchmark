package influx

import (
	"bytes"
	"context"
	"fmt"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
	"golang.org/x/time/rate"
	"time"

	"github.com/valyala/fasthttp"
)

const backingOffChanCap = 100

// allows for testing
var printFn = fmt.Printf

type processor struct {
	daemonURLs     []string
	loader         load.BenchmarkRunner
	consistency    string
	useGzip        bool
	backoff        time.Duration
	backingOffChan chan bool
	backingOffDone chan struct{}
	httpWriter     *HTTPWriter
	limiter        *rate.Limiter
}

// influxdb support multi-worker streess
func (p *processor) Init(numWorker int, _, _ bool) {
	//daemonURL := p.daemonURLs[numWorker%len(p.daemonURLs)]
	daemonURL := p.daemonURLs[0]
	cfg := HTTPWriterConfig{
		DebugInfo: fmt.Sprintf("worker #%d, dest url: %s", numWorker, daemonURL),
		Host:      daemonURL,
		Database:  p.loader.DatabaseName(),
	}
	w := NewHTTPWriter(cfg, p.consistency)
	p.initWithHTTPWriter(numWorker, w)
}

func (p *processor) initWithHTTPWriter(numWorker int, w *HTTPWriter) {
	p.backingOffChan = make(chan bool, backingOffChanCap)
	p.backingOffDone = make(chan struct{})
	p.httpWriter = w
	go p.processBackoffMessages(numWorker)
}

func (p *processor) Close(_ bool) {
	close(p.backingOffChan)
	<-p.backingOffDone
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)

	// Write the batch: try until backoff is not needed.
	if doLoad {
		var err error
		for {
			// qps limiter
			if p.limiter != nil {
				err = p.limiter.WaitN(context.Background(), int(batch.metrics))
				if err != nil {
					fatal("Error waitN: %s\n", err.Error())
				}
			}

			if p.useGzip {
				compressedBatch := bufPool.Get().(*bytes.Buffer)
				_, _ = fasthttp.WriteGzip(compressedBatch, batch.buf.Bytes())
				_, err = p.httpWriter.WriteLineProtocol(compressedBatch.Bytes(), true)
				// Return the compressed batch buffer to the pool.
				compressedBatch.Reset()
				bufPool.Put(compressedBatch)
			} else {
				_, err = p.httpWriter.WriteLineProtocol(batch.buf.Bytes(), false)
			}

			if err == errBackoff {
				p.backingOffChan <- true
				time.Sleep(p.backoff)
			} else {
				p.backingOffChan <- false
				break
			}
		}
		if err != nil {
			fatal("Error writing: %s\n", err.Error())
		}
	}
	metricCnt := batch.metrics
	rowCnt := batch.rows

	// Return the batch buffer to the pool.
	batch.buf.Reset()
	bufPool.Put(batch.buf)
	return metricCnt, uint64(rowCnt)
}

func (p *processor) processBackoffMessages(workerID int) {
	var totalBackoffSecs float64
	var start time.Time
	last := false
	for this := range p.backingOffChan {
		if this && !last {
			start = time.Now()
			last = true
		} else if !this && last {
			took := time.Now().Sub(start)
			_, _ = printFn("[worker %d] backoff took %.02fsec\n", workerID, took.Seconds())
			totalBackoffSecs += took.Seconds()
			last = false
			start = time.Now()
		}
	}
	_, _ = printFn("[worker %d] backoffs took a total of %fsec of runtime\n", workerID, totalBackoffSecs)
	p.backingOffDone <- struct{}{}
}
