package victoriametrics

import (
	"bufio"
	"bytes"
	"github.com/blagojts/viper"
	"github.com/timescale/tsbs/internal/inputs"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"golang.org/x/time/rate"
	"sync"
)

type SpecificConfig struct {
	ServerURLs          []string `yaml:"urls" mapstructure:"urls"`
	limiter_bucket_size int      `yaml:"limiter-bucket-size" mapstructure:"limiter-bucket-size"`
	limiter_max_qps     float64  `yaml:"limiter-max-qps" mapstructure:"limiter-max-qps"`
	use_qps_limiter     bool     `yaml:"use-qps-limiter" mapstructure:"use-qps-limiter"`
}

func parseSpecificConfig(v *viper.Viper) (*SpecificConfig, error) {
	var conf SpecificConfig
	conf.ServerURLs = v.GetStringSlice("urls")
	conf.limiter_bucket_size = v.GetInt("limiter-bucket-size")
	conf.limiter_max_qps = v.GetFloat64("limiter-max-qps")
	conf.use_qps_limiter = v.GetBool("use-qps-limiter")
	return &conf, nil
}

// loader.Benchmark interface implementation
type benchmark struct {
	serverURLs []string
	dataSource targets.DataSource
	limiter    *rate.Limiter
}

func NewBenchmark(vmSpecificConfig *SpecificConfig, dataSourceConfig *source.DataSourceConfig) (targets.Benchmark, error) {
	var dataSource targets.DataSource
	if dataSourceConfig.Type == source.FileDataSourceType {
		br := load.GetBufferedReader(dataSourceConfig.File.Location)
		dataSource = &fileDataSource{
			scanner: bufio.NewScanner(br),
		}
	} else {
		dataGenerator := &inputs.DataGenerator{}
		simulator, err := dataGenerator.CreateSimulator(dataSourceConfig.Simulator)
		if err != nil {
			return nil, err
		}
		dataSource = newSimulationDataSource(simulator)
	}

	var limiter *rate.Limiter = nil
	if vmSpecificConfig.use_qps_limiter {
		limiter = rate.NewLimiter(rate.Limit(vmSpecificConfig.limiter_max_qps), vmSpecificConfig.limiter_bucket_size)
	}

	return &benchmark{
		dataSource: dataSource,
		serverURLs: vmSpecificConfig.ServerURLs,
		limiter:    limiter,
	}, nil
}

func (b *benchmark) GetDataSource() targets.DataSource {
	return b.dataSource
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	bufPool := sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 16*1024*1024))
		},
	}
	return &factory{bufPool: &bufPool}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{vmURLs: b.serverURLs, limiter: b.limiter}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{}
}

type factory struct {
	bufPool *sync.Pool
}

func (f *factory) New() targets.Batch {
	return &batch{buf: f.bufPool.Get().(*bytes.Buffer)}
}
