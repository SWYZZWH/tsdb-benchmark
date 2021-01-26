package kmonitor

import (
	"fmt"
	"github.com/spf13/cast"
	"github.com/timescale/tsbs/pkg/targets"
	. "gitlab.alibaba-inc.com/monitor_service/kmonitor-client-go"
	"gitlab.alibaba-inc.com/monitor_service/kmonitor-client-go/model"
	"log"
	"strings"
)

const (
	kmon_go_service   = "kmonitor-go"
	kmon_test_service = "test"
)

func NewProcessor(bench *Benchmark) *processor {
	return &processor{host: bench.opts.Host, port: bench.opts.Port, ds: bench.ds, client: bench.client}
}

type processor struct {
	host   string
	port   string
	ds     targets.DataSource
	client *Client
}

func (p processor) Init(int, bool, bool) {
}

func (p processor) ProcessBatch(b targets.Batch, _ bool) (uint64, uint64) {
	arr := b.(*hypertableArr)
	var metricCount uint64 = 0
	var rowCount uint64 = 0
	fieldKeys := p.ds.Headers().FieldKeys
	for metricName, rows := range arr.m {
		fmt.Println("sending...")
		matrix, count := transferRows2Points(rows, fieldKeys, metricName)
		metricCount += count
		rowCount += uint64(len(rows))

		// send matrix
		// in fact, in order to take full advantage of bulk send ability for kmonitor client/agent
		// points need to gather by tags
		// now we just send points in a matrix one by one by single worker
		p.sendMatrix(matrix)
		fmt.Println("sended!")
	}

	//clear batch
	arr.m = map[string][]*insertData{}
	arr.cnt = 0
	return metricCount, rowCount
}

func (p processor) sendMatrix(matrix [][]model.Point) {
	for _, row := range matrix {
		for _, po := range row {
			err := p.client.Send(po)
			if err != nil {
				log.Fatalln(err.Error())
			}

		}
	}

}

// transfer rows i.e. []*insertData to []*model.Point
func transferRows2Points(rows []*insertData, fieldkeys map[string][]string, metricName string) ([][]model.Point, uint64) {
	rowlen := len(rows)
	matrix := make([][]model.Point, rowlen)
	var metricCount uint64 = 0
	for i, row := range rows {
		matrix[i] = transferRow2Point(row, fieldkeys, metricName)
		metricCount += uint64(len(matrix[i]))
	}
	return matrix, metricCount
}

func transferRow2Point(row *insertData, fieldkeys map[string][]string, metricName string) []model.Point {
	tags := map[string]string{}

	tagkvs := strings.Split(row.tags, ",")[1:]
	for _, tagkv := range tagkvs {
		tags[strings.Split(tagkv, "=")[0]] = strings.Split(tagkv, "=")[1]
	}

	fields := strings.Split(row.fields, ",")
	series := make([]model.Point, len(fields)-1)

	for i, key := range fieldkeys[metricName] {
		series[i] = model.Point{Name: strings.Join([]string{metricName, key}, "_"), Tags: tags,
			Service: kmon_test_service, TimeStamp: cast.ToInt64(fields[0]), Value: cast.ToFloat64(fields[i+1])}
	}

	return series
}
