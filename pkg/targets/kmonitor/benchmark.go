package kmonitor

import (
	"fmt"
	"github.com/spf13/cast"
	"github.com/timescale/tsbs/internal/inputs"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	kmonitor_client_go "gitlab.alibaba-inc.com/monitor_service/kmonitor-client-go"
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
	opts   *SpecificConfig
	ds     targets.DataSource
	pool   *sync.Pool
	client *kmonitor_client_go.Client
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
	//initialize kmon-go-client here
	self.NewClient(self.client)
	return NewProcessor(self)
}

func (self *Benchmark) GetDBCreator() targets.DBCreator {
	return nil
}

func (self *Benchmark) NewClient(client *kmonitor_client_go.Client) {
	if client == nil {
		fmt.Println("New Kmon-go-client...")
		tags := make(map[string]string)
		config := kmonitor_client_go.Config{Address: self.opts.Host, Port: cast.ToInt(self.opts.Host),
			Service: kmon_go_service, GlobalTag: tags}
		client, _ := kmonitor_client_go.NewClient(config)
		client.Init()
		self.client = client
	}
}
