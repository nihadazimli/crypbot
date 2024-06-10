package config

import (
	"log"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v2"
)

type Config struct {
	BybitCreds APICredentialsConfig `yaml:"bybit"`
}

type APICredentialsConfig struct {
	APIKey    string `yaml:"APIKey"`
	APISecret string `yaml:"APISecret"`
	WSAddress string `yaml:"WSAddress"`
}

var cfg *Config

func GetConfig() *Config {
	if cfg == nil {
		var err error
		cfg, err = loadConfigFromYaml()
		if err != nil {
			log.Fatal(err)
		}
	}

	return cfg
}

func loadConfigFromYaml() (*Config, error) {
	configPath := filepath.Join("configs", "config.yaml")

	f, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var config Config
	decoder := yaml.NewDecoder(f)
	if err = decoder.Decode(&config); err != nil {
		return nil, err
	}

	return &config, nil
}
