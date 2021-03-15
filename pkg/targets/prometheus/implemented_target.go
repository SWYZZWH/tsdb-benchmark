package prometheus

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

func NewTarget() targets.ImplementedTarget {
	return &prometheusTarget{}
}

type prometheusTarget struct {
}

func (t *prometheusTarget) TargetName() string {
	return constants.FormatPrometheus
}

func (t *prometheusTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *prometheusTarget) Benchmark(_ string, dataSourceConfig *source.DataSourceConfig, v *viper.Viper) (targets.Benchmark, error) {
	promSpecificConfig, err := parseSpecificConfig(v)
	if err != nil {
		return nil, err
	}
	return NewBenchmark(promSpecificConfig, dataSourceConfig)
}

func (t *prometheusTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"adapter-write-url", "http://localhost:9090/write", "Prometheus adapter url to send data to")
	flagSet.Bool(flagPrefix+"use-current-time", false, "Whether to replace the simulated timestamp with the current timestamp")
	flagSet.Bool(flagPrefix+"use-qps-limiter", false, "Use qps limiter or not.")
	flagSet.Float64(flagPrefix+"limiter-max-qps", 5000*1000, "Limit max qps.")
	flagSet.Int(flagPrefix+"limiter-bucket-size", 1000, "qps limiter param, default is 1000")
}
