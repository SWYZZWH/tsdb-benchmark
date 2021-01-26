package kmonitor

import (
	"github.com/timescale/tsbs/internal/inputs"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
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

	return &Benchmark{
		ds:   ds,
		opts: opts,
		pool: newPool,
	}, nil
}

// Benchmark implements targets.Benchmark interface
type Benchmark struct {
	opts *SpecificConfig
	ds   targets.DataSource
	pool *sync.Pool
}

func (self *Benchmark) GetDataSource() targets.DataSource {
	return self.ds
}

//type BatchFactory struct {
//	batchPool *sync.Pool
//}
//
//func (self *BatchFactory) New() targets.Batch {
//	arr := self.batchPool.Get().(*hypertableArr)
//
//	return arr
//}

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
