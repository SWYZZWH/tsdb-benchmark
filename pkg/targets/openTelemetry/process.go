package open_telemetry

import (
	"context"
	"fmt"
	"github.com/spf13/cast"
	"github.com/timescale/tsbs/pkg/targets"
	"gitlab.alibaba-inc.com/monitor_service/prometheus_client_golang/prometheus/kmonitor"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpgrpc"
	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/metric"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	proc "go.opentelemetry.io/otel/sdk/metric/processor/basic"
	"go.opentelemetry.io/otel/sdk/metric/selector/simple"
	"log"
	"strings"
	"time"
)

func NewProcessor(bench *Benchmark) *processor {
	return &processor{host: bench.opts.Host, port: bench.opts.Port, ds: bench.ds, registerMap: bench.registerMap}
}

type processor struct {
	host        string
	port        string
	ds          targets.DataSource
	cont        *controller.Controller
	ctx         *context.Context
	registerMap map[string]*metric.Float64ValueRecorder
}

func (p processor) Init(int, bool, bool) {

	fmt.Println("Open new OpenTelemetry-client...")

	ctx := context.Background()
	p.ctx = &ctx

	driver := otlpgrpc.NewDriver(
		otlpgrpc.WithInsecure(),
		otlpgrpc.WithEndpoint(p.host+":"+p.port),
		//otlpgrpc.WithDialOption(grpc.WithBlock()), // useful for testing
	)
	exp, err := otlp.NewExporter(ctx, driver)
	if err != nil {
		fmt.Println("Failed to start new exported!")
	}

	cont := controller.New(
		proc.New(
			simple.NewWithExactDistribution(),
			exp,
		),
		controller.WithPusher(exp),
		controller.WithCollectPeriod(2*time.Second),
	)
	err = cont.Start(ctx)
	if err != nil {
		fmt.Println("Failed to start new controller!")
	}
	p.cont = cont

}

func (p processor) Close() {
	cont := p.cont
	err := cont.Stop(context.Background())
	if err != nil {
		fmt.Println("Failed to close controller!")
	}
}

func (p processor) ProcessBatch(b targets.Batch, _ bool) (uint64, uint64) {
	arr := b.(*hypertableArr)
	var metricCount uint64 = 0
	var rowCount uint64 = 0
	fieldKeys := p.ds.Headers().FieldKeys
	for metricName, rows := range arr.m {
		fmt.Println("Sending...")
		metricCount += p.sendRows(p.ctx, rows, fieldKeys, metricName)
		rowCount += uint64(len(rows))
		fmt.Println("Sent!")
	}

	//clear batch
	arr.m = map[string][]*insertData{}
	arr.cnt = 0
	return metricCount, rowCount
}

func (p processor) sendRows(ctx *context.Context, rows []*insertData, fieldKeys map[string][]string, metricName string) uint64 {
	var count uint64 = 0
	for _, row := range rows {
		// send point one by one directly
		// use metric group to improve later
		meter := otel.Meter("meter")
		labels := transferTags(row.tags)
		fields := strings.Split(row.fields, ",")

		var count uint64 = 0

		var valueRecorder metric.Float64ValueRecorder
		if p.registerMap[metricName] == nil {
			valueRecorder = metric.Must(meter).NewFloat64ValueRecorder(metricName)
			p.registerMap[metricName] = &valueRecorder
		} else {
			valueRecorder = *p.registerMap[metricName]
		}

		for i := range fieldKeys[metricName] {
			valueRecorder.Record(*ctx, cast.ToFloat64(fields[i+1]), labels...)
			count++
		}
	}
	return count
}

func transferTags(tags string) []label.KeyValue {
	tagkvs := strings.Split(tags, ",")[1:]
	labels := make([]label.KeyValue, len(tagkvs))
	for i, tagkv := range tagkvs {
		labels[i] = label.String(strings.Split(tagkv, "=")[0], strings.Split(tagkv, "=")[1])
	}

	return labels
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
