package config

import (
	"log"
	"os"
	"path/filepath"
	"strconv"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Broker          BrokerConfig          `yaml:"bybit"`
	Symbols         []string              `yaml:"symbols"`
	MarketConstants MarketConstantsConfig `yaml:"marketConstants"`
	KlineTopics     []string              //Derived from symbols
}

type BrokerConfig struct {
	API       APIConfig       `yaml:"api"`
	WebSocket WebSocketConfig `yaml:"webSocket"`
}

type APIConfig struct {
	Host         string             `yaml:"host"`
	HostTestnet  string             `yaml:"hostTestnet"`
	Key          string             `yaml:"apiKey"`
	Secret       string             `yaml:"apiSecret"`
	Timeout      int                `yaml:"timeout"`
	RequestPaths RequestPathsConfig `yaml:"requestPaths"`
}

type WebSocketConfig struct {
	Address string `yaml:"address"`
}

type RequestPathsConfig struct {
	Kline string `yaml:"kline"`
}

type MarketConstantsConfig struct {
	KlineInterval int    `yaml:"klineInterval"`
	KLineCategory string `yaml:"klineCategory"`
}

var cfg *Config

func GetConfig() *Config {
	if cfg == nil {
		var err error
		cfg, err = loadFromYaml()
		if err != nil {
			log.Fatal(err)
		}
	}

	return cfg
}

func loadFromYaml() (*Config, error) {
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

	// Derive topics from symbols
	config.KlineTopics = make([]string, len(config.Symbols))
	for i, symbol := range config.Symbols {
		config.KlineTopics[i] = createKlineTopics(cfg.MarketConstants.KlineInterval, symbol)
	}

	return &config, nil
}

func createKlineTopics(klineInterval int, symbol string) string {
	return "kline." + strconv.Itoa(klineInterval) + "." + symbol
}
