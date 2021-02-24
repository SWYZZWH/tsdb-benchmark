package kmonitor

import "github.com/blagojts/viper"

type SpecificConfig struct {
	Host              string  `yaml:"host" mapstructure:"host"`
	Port              string  `yaml:"port" mapstructure:"port"`
	Qps               float64 `yaml:"qps" mapstructure:"qps"`
	LimiterBucketSize int     `yaml:"limiter-bucket-size" mapstructure:"limiter-Bucket-size"`
}

func parseSpecificConfig(v *viper.Viper) (*SpecificConfig, error) {
	var conf SpecificConfig
	if err := v.Unmarshal(&conf); err != nil {
		return nil, err
	}
	return &conf, nil
}
