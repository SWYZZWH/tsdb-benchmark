package victoriametrics

import (
	"bytes"
	"context"
	"github.com/timescale/tsbs/pkg/targets"
	"golang.org/x/time/rate"
	"log"
	"net/http"
	"time"
)

type processor struct {
	url     string
	vmURLs  []string
	limiter *rate.Limiter
}

func (p *processor) Init(workerNum int, doLoad, hashWorkers bool) {
	p.url = p.vmURLs[workerNum%len(p.vmURLs)]
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*batch)
	if !doLoad {
		return batch.metrics, batch.rows
	}
	mc, rc := p.do(batch)
	return mc, rc
}

func (p *processor) do(b *batch) (uint64, uint64) {
	for {
		if p.limiter != nil {
			err := p.limiter.WaitN(context.Background(), int(b.metrics))
			if err != nil {
				fatal(err.Error())
			}
		}

		r := bytes.NewReader(b.buf.Bytes())
		req, err := http.NewRequest("POST", p.url, r)
		if err != nil {
			log.Fatalf("error while creating new request: %s", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatalf("error while executing request: %s", err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusNoContent {
			b.buf.Reset()
			return b.metrics, b.rows
		}
		log.Printf("server returned HTTP status %d. Retrying", resp.StatusCode)
		time.Sleep(time.Millisecond * 10)
	}
}
