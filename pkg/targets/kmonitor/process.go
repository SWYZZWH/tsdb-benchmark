package kmonitor

import (
	"fmt"
	"github.com/spf13/cast"
	"github.com/timescale/tsbs/pkg/targets"
	"gitlab.alibaba-inc.com/monitor_service/prometheus_client_golang/prometheus/kmonitor"
	"log"
	"strings"
	"time"
)

const (
	kmon_go_service   = "kmonitor-go"
	kmon_test_service = "test"
)

func NewProcessor(bench *Benchmark) *processor {
	fmt.Println("New Kmon-go-client...")
	client := kmonitor.NewClient(
		&kmonitor.Config{
			Address: bench.opts.Host,
			Port:    bench.opts.Port,
			Service: kmon_go_service,
			GlobalTag: map[string]string{
				"cluster": "na61"}})
	client.Init()
	return &processor{host: bench.opts.Host, port: bench.opts.Port, ds: bench.ds, client: client}
}

type processor struct {
	host   string
	port   string
	ds     targets.DataSource
	client *kmonitor.Client
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

func (p processor) sendMatrix(matrix [][]*kmonitor.Point) {
	for _, row := range matrix {
		err := p.client.Send(row)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

}

func TransferPointTimeUnit(p *kmonitor.Point, from_time_unit string, to_time_unit string) {
	support_time_unit := []string{"ps", "ns", "us", "ms", "s"} // short time unit first
	//if !slice.Contains(support_time_unit, from_time_unit) || !slice.Contains(support_time_unit, to_time_unit){
	//	log.Fatalf("can't transfer timeunit from %v to %v! not supported yet", from_time_unit, to_time_unit)
	//}
	from_index, to_index := -1, -1
	for i, time_unit := range support_time_unit {
		if from_time_unit == time_unit || to_time_unit == time_unit {
			if from_time_unit == time_unit {
				from_index = i
			} else {
				to_index = i
			}
		}
	}
	if from_index == -1 || to_index == -1 {
		log.Fatalf("can't transfer timeunit from %v to %v! not supported yet", from_time_unit, to_time_unit)
	}

	scaleUp := to_index - from_index
	for scaleUp != 0 {
		if scaleUp > 0 {
			p.TimeStamp /= 1000
			scaleUp--
		} else {
			p.TimeStamp *= 1000
			scaleUp++
		}
	}

}

// transfer rows i.e. []*insertData to []*model.Point
func transferRows2Points(rows []*insertData, fieldkeys map[string][]string, metricName string) ([][]*kmonitor.Point, uint64) {
	rowlen := len(rows)
	matrix := make([][]*kmonitor.Point, rowlen)
	var metricCount uint64 = 0
	for i, row := range rows {
		matrix[i] = transferRow2Point(row, fieldkeys, metricName)
		metricCount += uint64(len(matrix[i]))
	}
	return matrix, metricCount
}

func transferRow2Point(row *insertData, fieldkeys map[string][]string, metricName string) []*kmonitor.Point {
	tags := map[string]string{}

	tagkvs := strings.Split(row.tags, ",")[1:]
	for _, tagkv := range tagkvs {
		tags[strings.Split(tagkv, "=")[0]] = strings.Split(tagkv, "=")[1]
	}

	fields := strings.Split(row.fields, ",")
	series := make([]*kmonitor.Point, len(fields)-1)

	for i, key := range fieldkeys[metricName] {
		series[i] = &kmonitor.Point{Name: strings.Join([]string{metricName, key}, "_"), Tags: tags,
			Service: kmon_test_service, TimeStamp: time.Now().Unix() * 1000, Value: cast.ToFloat64(fields[i+1]), Tenant: "default"}
	}

	return series
}
