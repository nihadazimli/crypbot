package config

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Broker          BrokerConfig          `yaml:"bybit"`
	Symbols         []string              `yaml:"symbols"`
	MarketConstants MarketConstantsConfig `yaml:"market_constants"`
	Trader          TraderConfig          `yaml:"trader"`
	KlineTopics     []string              // Derived from symbols, not directly from YAML
}

type BrokerConfig struct {
	API       APIConfig       `yaml:"api"`
	WebSocket WebSocketConfig `yaml:"web_socket"`
}

type APIConfig struct {
	Host         string             `yaml:"host"`
	HostTestnet  string             `yaml:"host_testnet"`
	Key          string             `yaml:"api_key"`
	Secret       string             `yaml:"api_secret"`
	Timeout      int                `yaml:"timeout"`
	RequestPaths RequestPathsConfig `yaml:"request_paths"`
}

type WebSocketConfig struct {
	Address string `yaml:"address"`
}

type RequestPathsConfig struct {
	Kline string `yaml:"kline"`
}

type MarketConstantsConfig struct {
	KlineInterval int    `yaml:"kline_interval"`
	KlineCategory string `yaml:"kline_category"`
	RSIWindowSize int    `yaml:"rsi_window_size"`
}

type TraderConfig struct {
	RSIBuyValue  float64 `yaml:"rsi_buy_value"`
	RSISellValue float64 `yaml:"rsi_sell_value"`
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
	config.initializeKlineTopics()

	err = config.validate()
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func createKlineTopics(klineInterval int, symbol string) string {
	return "kline." + strconv.Itoa(klineInterval) + "." + symbol
}

func (c *Config) initializeKlineTopics() {
	c.KlineTopics = make([]string, len(c.Symbols))
	for i, symbol := range c.Symbols {
		c.KlineTopics[i] = createKlineTopics(c.MarketConstants.KlineInterval, symbol)
	}
}

func (c *Config) validate() error {
	if len(c.Symbols) == 0 {
		return errors.New("no symbols configured")
	}
	return nil
}
