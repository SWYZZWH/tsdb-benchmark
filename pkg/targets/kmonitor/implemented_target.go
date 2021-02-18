package kmonitor

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

func NewTarget() targets.ImplementedTarget {
	return &kmonTarget{}
}

type kmonTarget struct {
}

func (t *kmonTarget) TargetName() string {
	return constants.FormatKmon
}

func (t *kmonTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *kmonTarget) Benchmark(
	_ string, dataSourceConfig *source.DataSourceConfig, v *viper.Viper,
) (targets.Benchmark, error) {
	kmonSpecificConfig, err := parseSpecificConfig(v)
	if err != nil {
		return nil, err
	}
	return NewBenchmark(kmonSpecificConfig, dataSourceConfig)
}

func (t *kmonTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	//flagSet.String(flagPrefix+"adapter-write-url", "http://localhost:9201/write", "kmon adapter url to send data to")
	//flagSet.Bool(flagPrefix+"use-current-time", false, "Whether to replace the simulated timestamp with the current timestamp")
	flagSet.String(flagPrefix+"host", "10.101.193.170", "Hostname of daily Kmon agent.")
	flagSet.String(flagPrefix+"port", "4848", "Port of daily Kmon agent.")
}
