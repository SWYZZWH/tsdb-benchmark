package prom_pull

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/timescale/tsbs/internal/inputs"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"golang.org/x/time/rate"
	"log"
	"net/http"
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

	//new qps limiter
	var limiter *rate.Limiter = nil
	if opts.UseQpsLimiter {
		limiter = rate.NewLimiter(rate.Limit(opts.LimiterMaxQps), opts.LimiterBucketSize)
	}

	//expose metrics gathering api
	go func() {
		http.Handle("/metrics", promhttp.HandlerFor(
			prometheus.DefaultGatherer,
			promhttp.HandlerOpts{
				// Opt into OpenMetrics to support exemplars.
				EnableOpenMetrics: true,
			},
		))
		log.Fatal(http.ListenAndServe(":"+opts.Port, nil))
	}()

	return &Benchmark{
		ds:         ds,
		opts:       opts,
		limiter:    limiter,
		metricsMap: new(sync.Map),
	}, nil
}

// Benchmark implements targets.Benchmark interface
type Benchmark struct {
	opts       *SpecificConfig
	ds         targets.DataSource
	limiter    *rate.Limiter
	metricsMap *sync.Map
}

func (self *Benchmark) GetDataSource() targets.DataSource {
	return self.ds
}

func (self *Benchmark) GetBatchFactory() targets.BatchFactory {
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
