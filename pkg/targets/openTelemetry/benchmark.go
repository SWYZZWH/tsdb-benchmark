package open_telemetry

import (
	"github.com/timescale/tsbs/pkg/targets"
	"sync"
)

// Benchmark implements targets.Benchmark interface
type Benchmark struct {
	opts        *SpecificConfig
	ds          targets.DataSource
	pool        *sync.Pool
	registerMap *sync.Map // type:[string]*metric.Float64ValueRecorder //metrics should be registered before send, cache registered points
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
