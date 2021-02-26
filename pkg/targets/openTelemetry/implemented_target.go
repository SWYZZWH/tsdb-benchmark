package open_telemetry

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

func NewTarget() targets.ImplementedTarget {
	return &opentlTarget{}
}

type opentlTarget struct {
}

func (t *opentlTarget) TargetName() string {
	return constants.FormatOpenTelemetry
}

func (t *opentlTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *opentlTarget) Benchmark(
	_ string, dataSourceConfig *source.DataSourceConfig, v *viper.Viper,
) (targets.Benchmark, error) {
	promSpecificConfig, err := parseSpecificConfig(v)

	if err != nil {
		return nil, err
	}
	return NewBenchmark(promSpecificConfig, dataSourceConfig)
}

func (t *opentlTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"host", "10.101.193.170", "Hostname of daily Otel agent.")
	flagSet.String(flagPrefix+"port", "4848", "Port of daily Otel agent.")
	flagSet.Bool(flagPrefix+"use-qps-limiter", false, "Use qps limiter or not.")
	flagSet.Float64(flagPrefix+"limiter-max-qps", 5000*1000, "Limit max qps.")
	flagSet.Int(flagPrefix+"limiter-bucket-size", 1000, "qps limiter param, default is 1000")
}
