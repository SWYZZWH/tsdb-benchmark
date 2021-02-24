package open_telemetry

import (
	"context"
	"fmt"
	"github.com/spf13/cast"
	"github.com/timescale/tsbs/pkg/targets"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpgrpc"
	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/metric"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	proc "go.opentelemetry.io/otel/sdk/metric/processor/basic"
	"go.opentelemetry.io/otel/sdk/metric/selector/simple"
	"google.golang.org/grpc"
	"math"
	"strings"
	"sync"
	"time"
)

var counter = 0

func NewProcessor(bench *Benchmark) *processor {
	return &processor{host: bench.opts.Host, port: bench.opts.Port, ds: bench.ds, registerMap: bench.registerMap}
}

type processor struct {
	host        string
	port        string
	ds          targets.DataSource
	cont        *controller.Controller
	ctx         *context.Context
	registerMap *sync.Map //map[string]*metric.Float64ValueRecorder
	meter       metric.Meter
}

func (p *processor) Init(int, bool, bool) {

	fmt.Println("Open new OpenTelemetry-client...")

	ctx := context.Background()
	p.ctx = &ctx

	driver := otlpgrpc.NewDriver(
		otlpgrpc.WithInsecure(),
		otlpgrpc.WithEndpoint(p.host+":"+p.port),
		otlpgrpc.WithDialOption(grpc.WithBlock(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(32*1024*1024))), // useful for testing
	)
	exp, err := otlp.NewExporter(ctx, driver)
	if err != nil {
		fmt.Println("Failed to start new exported!")
	}

	if err != nil {
		fmt.Println("failed to create resource")
	}

	cont := controller.New(
		proc.New(
			simple.NewWithExactDistribution(),
			exp,
		),
		controller.WithPusher(exp),
		controller.WithCollectPeriod(50*time.Millisecond),
		controller.WithPushTimeout(10*time.Second),
	)
	otel.SetMeterProvider(cont.MeterProvider())

	err = cont.Start(ctx)
	if err != nil {
		fmt.Println("Failed to start new controller!")
	}
	p.cont = cont
	p.meter = otel.Meter("meter")

}

func (p *processor) Close() {
	cont := p.cont
	err := cont.Stop(context.Background())
	if err != nil {
		fmt.Println("Failed to close controller!")
	}
}

func (p *processor) ProcessBatch(b targets.Batch, _ bool) (uint64, uint64) {
	arr := b.(*hypertableArr)
	var metricCount uint64 = 0
	var rowCount uint64 = 0
	fieldKeys := p.ds.Headers().FieldKeys
	for metricName, rows := range arr.m {
		metricCountAdd, rowCountAdd := p.sendRows(rows, fieldKeys, metricName)
		metricCount += metricCountAdd
		rowCountAdd += rowCountAdd
	}

	//clear batch
	arr.m = map[string][]*insertData{}
	arr.cnt = 0
	return metricCount, rowCount
}

func (p *processor) sendRows(rows []*insertData, fieldKeys map[string][]string, metricName string) (uint64, uint64) {
	var metricCount uint64 = 0
	var rowCount uint64 = 0

	for _, row := range rows {

		labels := transferTags(row.tags)
		fields := strings.Split(row.fields, ",")

		valueRecorder, _ := p.registerMap.LoadOrStore(metricName,
			metric.Must(p.meter).NewFloat64ValueRecorder(metricName, metric.WithDescription("")))

		//valueRecorder2 := metric.Must(p.meter).
		//	NewFloat64Counter(
		//		metricName,
		//		metric.WithDescription(""),
		//	)

		for i := range fieldKeys[metricName] {
			//if counter > 1000000{
			//	time.Sleep(20 * time.Second)
			//	counter = 0
			//}
			valueRecorder.(metric.Float64ValueRecorder).Record(*p.ctx, math.Abs(cast.ToFloat64(fields[i+1])), labels...)
			//valueRecorder2.Add(*p.ctx, math.Abs(cast.ToFloat64(fields[i+1])), labels...)
			time.Sleep(10 * time.Microsecond)
			metricCount++
			counter++
		}
		rowCount++
	}
	return metricCount, rowCount
}

func transferTags(tags string) []label.KeyValue {
	tagkvs := strings.Split(tags, ",")[1:]
	labels := make([]label.KeyValue, len(tagkvs))
	for i, tagkv := range tagkvs {
		labels[i] = label.String(strings.Split(tagkv, "=")[0], strings.Split(tagkv, "=")[1])
	}

	return labels
}
