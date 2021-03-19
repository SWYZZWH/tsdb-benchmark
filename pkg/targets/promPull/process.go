package prom_pull

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/tsbs/pkg/targets"
	"golang.org/x/time/rate"
	"math/rand"
	"strconv"
	"strings"
	"sync"
)

var (
	//prom_pull_go_service   = "prom-test"
	prom_pull_test_service = "test"
)

func NewProcessor(bench *Benchmark) *processor {
	fmt.Println("New prom_pull processor...")
	return &processor{host: bench.opts.Host, port: bench.opts.Port, ds: bench.ds, metricMap: bench.metricsMap, limiter: bench.limiter}
}

type processor struct {
	host      string
	port      string
	ds        targets.DataSource
	metricMap *sync.Map
	limiter   *rate.Limiter
}

func (p *processor) Init(int, bool, bool) {
}

func (p *processor) Close() {

}

func (p *processor) ProcessBatch(b targets.Batch, _ bool) (uint64, uint64) {
	arr := b.(*hypertableArr)
	var metricCount uint64 = 0
	var rowCount uint64 = 0

	for metricName, rows := range arr.m {
		if len(rows) == 0 {
			continue
		}

		//parse tags
		tags := make([]string, 0)
		tagkvs := strings.Split(rows[0].tags, ",")[1:]
		for _, tagkv := range tagkvs {
			tags = append(tags, strings.Split(tagkv, "=")[0])
		}
		tags = append(tags, "random")

		newGauge := prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: metricName,
			}, tags,
		)
		metric, loaded := p.metricMap.LoadOrStore(metricName, newGauge)
		if !loaded {
			prometheus.MustRegister(newGauge)
		}

		gaugeMetric := metric.(*prometheus.GaugeVec)
		count := sendPoints(rows, gaugeMetric)
		metricCount += count
		rowCount += uint64(len(rows))
	}

	//clear batch
	arr.m = map[string][]*insertData{}
	arr.cnt = 0
	return metricCount, rowCount

}

func sendPoints(rows []*insertData, gauge *prometheus.GaugeVec) uint64 {
	for _, row := range rows {
		tags := map[string]string{}

		tagkvs := strings.Split(row.tags, ",")[1:]
		for _, tagkv := range tagkvs {
			tags["random"] = strconv.FormatInt(rand.Int63(), 10)
			tags[strings.Split(tagkv, "=")[0]] = strings.Split(tagkv, "=")[1]
		}

		(*gauge).With(tags).Add(1)
	}
	return uint64(len(rows))
}
