package influx

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/timescale/tsbs/internal/inputs"
	"github.com/timescale/tsbs/pkg/data/source"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/blagojts/viper"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
)

func NewBenchmark(dsConfig *source.DataSourceConfig) (*benchmark, error) {
	b := new(benchmark)
	b.init()
	b.dsConfig = dsConfig
	return b, nil
}

// Program option vars:

var consistencyChoices = map[string]struct{}{
	"any":    {},
	"one":    {},
	"quorum": {},
	"all":    {},
}

// allows for testing
var fatal = log.Fatalf
var bufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
	},
}

type benchmark struct {
	loader load.BenchmarkRunner
	config load.BenchmarkRunnerConfig
	//bufPool sync.Pool
	//target  targets.ImplementedTarget

	daemonURLs        []string
	replicationFactor int
	backoff           time.Duration
	useGzip           bool
	doAbortOnExist    bool
	consistency       string
	dsConfig          *source.DataSourceConfig
	ds                targets.DataSource
}

// Parse args:
func (b *benchmark) init() {

	err := utils.SetupConfigFile()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err := viper.Unmarshal(&b.config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	csvDaemonURLs := viper.GetString("urls")
	b.replicationFactor = viper.GetInt("replication-factor")
	b.consistency = viper.GetString("consistency")
	b.backoff = viper.GetDuration("backoff")
	b.useGzip = viper.GetBool("gzip")

	if _, ok := consistencyChoices[b.consistency]; !ok {
		log.Fatalf("invalid consistency settings")
	}

	b.daemonURLs = strings.Split(csvDaemonURLs, ",")
	if len(b.daemonURLs) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	b.config.HashWorkers = false
	b.loader = load.GetBenchmarkRunner(b.config)

}

func (b *benchmark) GetDataSource() targets.DataSource {
	var ds targets.DataSource
	if b.dsConfig.Type == source.FileDataSourceType {
		ds = &fileDataSource{scanner: bufio.NewScanner(load.GetBufferedReader(b.config.FileName))}
		return ds
	} else {
		dataGenerator := &inputs.DataGenerator{}
		simulator, err := dataGenerator.CreateSimulator(b.dsConfig.Simulator)
		if err != nil {
			return nil
		}
		ds = newSimulationDataSource(simulator)
	}
	if b.ds == nil {
		b.ds = ds
	}
	return ds
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{
		//bufPool: b.bufPool,
	}
}

func (b *benchmark) GetPointIndexer(_ uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{daemonURLs: b.daemonURLs,
		loader:      b.loader,
		consistency: b.consistency,
		useGzip:     b.useGzip,
		//bufPool:     &b.bufPool,
		backoff: b.backoff,
	}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{b.daemonURLs[0], b.loader, b.replicationFactor}
}
