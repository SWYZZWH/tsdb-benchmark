package prometheus

import "github.com/blagojts/viper"

type SpecificConfig struct {
	UseQpsLimiter     bool    `yaml:"use-qps-limiter" mapstructure:"use-qps-limiter"`
	LimiterMaxQps     float64 `yaml:"limiter-max-qps" mapstructure:"limiter-max-qps"`
	LimiterBucketSize int     `yaml:"limiter-bucket-size" mapstructure:"limiter-bucket-size"`
	AdapterWriteURL   string  `yaml:"adapter-write-url" mapstructure:"adapter-write-url"`
	UseCurrentTime    bool    `yaml:"use-current-time" mapstructure:"use-current-time"`
}

func parseSpecificConfig(v *viper.Viper) (*SpecificConfig, error) {
	var conf SpecificConfig
	if err := v.Unmarshal(&conf); err != nil {
		return nil, err
	}
	return &conf, nil
}
