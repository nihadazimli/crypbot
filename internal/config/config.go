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
	Defaults        Defaults              `yaml:"defaults"`
	KlineTopics     []string              `yaml:"-"`
}

type BrokerConfig struct {
	API       APIConfig       `yaml:"api"`
	WebSocket WebSocketConfig `yaml:"web_socket"`
}

type APIConfig struct {
	Host         string             `yaml:"host"`
	HostTestnet  string             `yaml:"host_testnet"`
	Key          string             `yaml:"key"`
	Secret       string             `yaml:"secret"`
	Timeout      int                `yaml:"timeout"`
	RequestPaths RequestPathsConfig `yaml:"request_paths"`
}

type WebSocketConfig struct {
	Address string `yaml:"address"`
}

type RequestPathsConfig struct {
	Kline         string `yaml:"kline"`
	WalletBalance string `yaml:"wallet_balance"`
}

type MarketConstantsConfig struct {
	KlineInterval     int    `yaml:"kline_interval"`
	KlineCategory     string `yaml:"kline_category"`
	MetricsWindowSize int    `yaml:"metrics_window_size"`
}

type TraderConfig struct {
	RSIBuyValue           float64 `yaml:"rsi_buy_value"`
	RSISellValue          float64 `yaml:"rsi_sell_value"`
	MedianVolumeThreshold float64 `yaml:"median_volume_threshold"`
	SellCheckInterval     int     `yaml:"sell_check_interval"`
	BuyCheckInterval      int     `yaml:"buy_check_interval"`
	BalanceToLeave        float64 `yaml:"balance_to_leave"`
}

type Defaults struct {
	MissingDataValue float64 `yaml:"missing_data_value"`
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
	if err := decoder.Decode(&config); err != nil {
		return nil, err
	}

	// Derive topics from symbols
	config.initializeKlineTopics()

	if err := config.validate(); err != nil {
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
