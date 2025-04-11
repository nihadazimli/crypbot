// Package config includes configs used accross the application
package config

import (
	"errors"
	"log"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Exchange        ExchangeConfig        `yaml:"bybit"`
	Symbols         []string              `yaml:"symbols"`
	MarketConstants MarketConstantsConfig `yaml:"market_constants"`
	Trader          TraderConfig          `yaml:"trader"`
	Defaults        Defaults              `yaml:"defaults"`
	KlineTopics     []string              `yaml:"-"`
}

type ExchangeConfig struct {
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
	AddressPublic           string `yaml:"address_public"`
	AddressPrivate          string `yaml:"address_private"`
	PingInterval            int    `yaml:"ping_interval"`
	MonitorInterval         int    `yaml:"monitor_interval"`
	MaxSymbolsPerConnection int    `yaml:"max_symbols_per_connection"`
}

type RequestPathsConfig struct {
	Kline         string `yaml:"kline"`
	WalletBalance string `yaml:"wallet_balance"`
}

type MarketConstantsConfig struct {
	CloseKlineTimeoutMinutes    int    `yaml:"close_kline_timeout_minutes"`
	KlineInterval               int    `yaml:"kline_interval"`
	KlineCategory               string `yaml:"kline_category"`
	MetricsWindowSize           int    `yaml:"metrics_window_size"`
	ClosePriceMAInterval        int    `yaml:"close_price_ma_interval"`
	ClosePriceMAWindowSizeLong  int    `yaml:"close_price_ma_window_size_long"`
	ClosePriceMAWindowSizeShort int    `yaml:"close_price_ma_window_size_short"`
}

type TraderConfig struct {
	RSIBuyValue           float64 `yaml:"rsi_buy_value"`
	RSISellValue          float64 `yaml:"rsi_sell_value"`
	MedianVolumeThreshold float64 `yaml:"median_volume_threshold"`
	SellCheckInterval     int     `yaml:"sell_check_interval"`
	BuyCheckInterval      int     `yaml:"buy_check_interval"`
	BalanceToLeave        float64 `yaml:"balance_to_leave"`
	StopLossRatio         float64 `yaml:"stop_loss_ratio"`
}

type Defaults struct {
	MissingDataValue         float64 `yaml:"missing_data_value"`
	OrderStatusCacheDuration int     `yaml:"order_status_cache_duration"`
}

var cfg *Config

func GetConfig() *Config {
	if cfg == nil {
		isProd := os.Getenv("ENVIRONMENT") == "production"
		var err error
		cfg, err = loadFromYaml(isProd)
		if err != nil {
			log.Fatal(err)
		}
	}

	return cfg
}

func (c *Config) SetSymbols(symbols []string) {
	c.Symbols = symbols
}

func loadFromYaml(isProd bool) (*Config, error) {
	var configPath string
	if isProd {
		log.Printf("Loading production config file\n")
		configPath = filepath.Join("config", "config.yaml")
	} else {
		configPath = filepath.Join("config", "config_test.yaml")
	}

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

	if err := config.validate(); err != nil {
		return nil, err
	}
	return &config, nil
}

func (c *Config) validate() error {
	if len(c.Symbols) == 0 {
		return errors.New("no symbols configured")
	}
	return nil
}
