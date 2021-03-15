package open_telemetry

import "github.com/blagojts/viper"

type SpecificConfig struct {
	Host              string  `yaml:"host" mapstructure:"host"`
	Port              string  `yaml:"port" mapstructure:"port"`
	UseQpsLimiter     bool    `yaml:"use-qps-limiter" mapstructure:"use-qps-limiter"`
	LimiterMaxQps     float64 `yaml:"limiter-max-qps" mapstructure:"limiter-max-qps"`
	LimiterBucketSize int     `yaml:"limiter-bucket-size" mapstructure:"limiter-bucket-size"`
}

func parseSpecificConfig(v *viper.Viper) (*SpecificConfig, error) {
	var conf SpecificConfig
	if err := v.Unmarshal(&conf); err != nil {
		return nil, err
	}
	return &conf, nil
}
