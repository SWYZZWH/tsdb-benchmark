package kmonitor

import (
	"github.com/timescale/tsbs/internal/inputs"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"gitlab.alibaba-inc.com/monitor_service/prometheus_client_golang/prometheus/kmonitor"
	"golang.org/x/time/rate"
	"sync"
)

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

	var limiter *rate.Limiter = nil
	if opts.UseQpsLimiter {
		limiter = rate.NewLimiter(rate.Limit(opts.LimiterMaxQps), opts.LimiterBucketSize)
	}

	return &Benchmark{
		ds:      ds,
		opts:    opts,
		pool:    newPool,
		limiter: limiter,
	}, nil
}

// Benchmark implements targets.Benchmark interface
type Benchmark struct {
	opts    *SpecificConfig
	ds      targets.DataSource
	pool    *sync.Pool
	client  *kmonitor.Client
	limiter *rate.Limiter
}

func (self *Benchmark) GetDataSource() targets.DataSource {
	return self.ds
}

func (self *Benchmark) GetBatchFactory() targets.BatchFactory {
	//return &BatchFactory{batchPool:self.pool}
	return &factory{}
}

func (self *Benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	if maxPartitions > 1 {
		return newPointIndexer(maxPartitions)
	}
	return &targets.ConstantIndexer{}
}

func (self *Benchmark) GetProcessor() targets.Processor {
	return NewProcessor(self)
}

func (self *Benchmark) GetDBCreator() targets.DBCreator {
	return nil
}
