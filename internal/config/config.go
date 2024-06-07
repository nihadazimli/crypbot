package config

import (
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

func LoadConfig() (*Config, error) {
	configPath := filepath.Join("configs", "config.yaml")

	f, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var cfg Config
	decoder := yaml.NewDecoder(f)
	if err = decoder.Decode(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
