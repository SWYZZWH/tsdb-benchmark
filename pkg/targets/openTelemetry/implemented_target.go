package open_telemetry

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/inputs"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"sync"
)

func NewTarget() targets.ImplementedTarget {
	return &opentlTarget{}
}

type opentlTarget struct {
}

func (t *opentlTarget) TargetName() string {
	return constants.FormatKmon
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
	flagSet.String(flagPrefix+"host", "", "Hostname of daily Kmon agent.") // waited to be set ...
	flagSet.String(flagPrefix+"port", "", "Port of daily Kmon agent.")
}

func NewBenchmark(opts *SpecificConfig, dataSourceConfig *source.DataSourceConfig) (targets.Benchmark, error) {
	var ds targets.DataSource
	if dataSourceConfig.Type == source.FileDataSourceType {
		ds = newFileDataSource(dataSourceConfig.File.Location)
	} else {
		dataGenerator := &inputs.DataGenerator{}
		simulator, err := dataGenerator.CreateSimulator(dataSourceConfig.Simulator)
		if err != nil {
			return nil, err
		}
		ds = newSimulationDataSource(simulator)
	}

	newPool := &sync.Pool{New: func() interface{} { return &hypertableArr{} }}

	return &Benchmark{
		ds:          ds,
		opts:        opts,
		pool:        newPool,
		registerMap: new(sync.Map),
	}, nil
}
