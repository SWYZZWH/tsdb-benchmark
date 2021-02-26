package open_telemetry

import (
	"context"
	"fmt"
	"github.com/spf13/cast"
	"github.com/timescale/tsbs/pkg/targets"
	"gitlab.alibaba-inc.com/monitor_service/prometheus_client_golang/prometheus/otel"
	"golang.org/x/time/rate"
	"log"
	"strings"
	"time"
)

var (
	otel_go_service   = "otel-test"
	otel_test_service = "test"
)

func NewProcessor(bench *Benchmark) *processor {
	fmt.Println("New Otel-go-client...")
	client := otel.NewClient(
		&otel.Config{
			Address: bench.opts.Host,
			Port:    bench.opts.Port,
			Service: otel_go_service,
			GlobalTag: map[string]string{
				"cluster": "na61"}})
	client.Init()
	return &processor{host: bench.opts.Host, port: bench.opts.Port, ds: bench.ds, client: client, limiter: bench.limiter}
}

type processor struct {
	host    string
	port    string
	ds      targets.DataSource
	client  *otel.Client
	limiter *rate.Limiter
}

func (p *processor) Init(int, bool, bool) {

}

func (p *processor) Close() {

}

func (p *processor) ProcessBatch(b targets.Batch, _ bool) (uint64, uint64) {
	arr := b.(*hypertableArr)
	var metricCount uint64 = 0
	var rowCount uint64 = 0
	fieldKeys := p.ds.Headers().FieldKeys

	var otelMatrix [][]*otel.Point
	for metricName, rows := range arr.m {
		matrix, count := transferRows2Points(rows, fieldKeys, metricName)
		otelMatrix = append(otelMatrix, matrix...)
		metricCount += count
		rowCount += uint64(len(rows))
	}

	p.sendMatrix(otelMatrix, int(rowCount))

	//clear batch
	arr.m = map[string][]*insertData{}
	arr.cnt = 0
	return metricCount, rowCount

}

func (p processor) sendMatrix(matrix [][]*otel.Point, batchSize int) {
	var flatMatrix []*otel.Point

	for _, row := range matrix {
		flatMatrix = append(flatMatrix, row...)
	}

	index := 0
	for ; index+batchSize <= len(flatMatrix); index += batchSize {
		p.sendBatch(flatMatrix[index : index+batchSize])
	}

	if index < len(flatMatrix) {
		p.sendBatch(flatMatrix[index:])
	}

}

func (p processor) sendBatch(batch []*otel.Point) {
	if p.limiter != nil {
		err := p.limiter.WaitN(context.Background(), len(batch))
		if err != nil {
			log.Fatal(err.Error())
		}
	}

	err := p.client.Send(batch)
	if err != nil {
		log.Fatalln(err.Error())
	}

}

// transfer rows i.e. []*insertData to []*model.Point
func transferRows2Points(rows []*insertData, fieldkeys map[string][]string, metricName string) ([][]*otel.Point, uint64) {
	rowlen := len(rows)
	matrix := make([][]*otel.Point, rowlen)
	var metricCount uint64 = 0
	for i, row := range rows {
		matrix[i] = transferRow2Point(row, fieldkeys, metricName)
		metricCount += uint64(len(matrix[i]))
	}
	return matrix, metricCount
}

func transferRow2Point(row *insertData, fieldkeys map[string][]string, metricName string) []*otel.Point {
	tags := map[string]string{}

	tagkvs := strings.Split(row.tags, ",")[1:]
	for _, tagkv := range tagkvs {
		tags[strings.Split(tagkv, "=")[0]] = strings.Split(tagkv, "=")[1]
	}

	fields := strings.Split(row.fields, ",")
	var series = make([]*otel.Point, len(fields)-1)
	for i, key := range fieldkeys[metricName] {
		series[i] = &otel.Point{Name: strings.Join([]string{metricName, key}, "_"), Tags: tags,
			Service: otel_test_service, TimeStamp: uint64(time.Now().UnixNano()), Value: cast.ToFloat64(fields[i+1]), Tenant: "default"}
	}

	return series
}
