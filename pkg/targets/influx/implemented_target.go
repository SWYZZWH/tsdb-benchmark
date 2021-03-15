package influx

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"time"
)

func NewTarget() targets.ImplementedTarget {
	return &influxTarget{}
}

type influxTarget struct {
}

func (t *influxTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"urls", "http://localhost:8086", "InfluxDB URLs, comma-separated. Will be used in a round-robin fashion.")
	flagSet.Bool(flagPrefix+"use-qps-limiter", false, "Use qps limiter or not.")
	flagSet.Float64(flagPrefix+"limiter-max-qps", 5000*1000, "Limit max qps.")
	flagSet.Int(flagPrefix+"limiter-bucket-size", 1000, "qps limiter param, default is 1000")
	flagSet.Int(flagPrefix+"replication-factor", 1, "Cluster replication factor (only applies to clustered databases).")
	flagSet.String(flagPrefix+"consistency", "all", "Write consistency. Must be one of: any, one, quorum, all.")
	flagSet.Duration(flagPrefix+"backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	flagSet.Bool(flagPrefix+"gzip", true, "Whether to gzip encode requests (default true).")
}

type SpecificConfig struct {
	urls                string        `yaml:"urls" mapstructure:"urls"`
	replication_factor  int           `yaml:"replication-factor" mapstructure:"replication-factor"`
	consistency         string        `yaml:"consistency" mapstructure:"consistency"`
	backoff             time.Duration `yaml:"backoff" mapstructure:"backoff"`
	gzip                bool          `yaml:"gzip" mapstructure:"gzip"`
	use_qps_limiter     bool          `yaml:"use-qps-limiter" mapstructure:"use-qps-limiter"`
	limiter_max_qps     float64       `yaml:"limiter-max-qps" mapstructure:"limiter-max-qps"`
	limiter_bucket_size int           `yaml:"limiter-bucket-size" mapstructure:"limiter-bucket-size"`
}

func (t *influxTarget) TargetName() string {
	return constants.FormatInflux
}

func (t *influxTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *influxTarget) Benchmark(s string, dsConfig *source.DataSourceConfig, viper *viper.Viper) (targets.Benchmark, error) {
	config := load.BenchmarkRunnerConfig{}
	config.AddToFlagSet(pflag.CommandLine)
	t.TargetSpecificFlags("", pflag.CommandLine)

	//influxSpecificConfig, err := parseSpecificConfig(viper)
	//if err != nil {
	//	return nil, err
	//}
	return NewBenchmark(dsConfig, viper)
}
