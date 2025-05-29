// Package config includes configs used across the application
package config

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/nihadazimli/crypbot/internal/model"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Exchange        ExchangeConfig        `yaml:"bybit"`
	Symbols         []string              `yaml:"symbols"`
	Strategies      StrategiesConfig      `yaml:"strategies"`
	MarketConstants MarketConstantsConfig `yaml:"market_constants"`
	Trader          TraderConfig          `yaml:"trader"`
	Defaults        Defaults              `yaml:"defaults"`
	KlineTopics     []string              `yaml:"-"`
}
type StrategiesConfig struct {
	RSI       RSIConfig       `yaml:"rsi"`
	Bollinger BollingerConfig `yaml:"bollinger"`
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
	Timeout      time.Duration      `yaml:"timeout"`
	RequestPaths RequestPathsConfig `yaml:"request_paths"`
}

type WebSocketConfig struct {
	AddressPublic           string        `yaml:"address_public"`
	AddressPrivate          string        `yaml:"address_private"`
	PingIntervalDuration    time.Duration `yaml:"ping_interval_duration"`
	MonitorIntervalDuration time.Duration `yaml:"monitor_interval_duration"`
	MaxSymbolsPerConnection int           `yaml:"max_symbols_per_connection"`
}

type RequestPathsConfig struct {
	Kline         string `yaml:"kline"`
	WalletBalance string `yaml:"wallet_balance"`
}

type MarketConstantsConfig struct {
	CloseKlineTimeout         time.Duration `yaml:"close_kline_timeout_minutes"`
	KlineInterval             int           `yaml:"kline_interval"`
	KlineCategory             string        `yaml:"kline_category"`
	MetricsWindowSize         int           `yaml:"metrics_window_size"`
	ClosePriceMAInterval      int           `yaml:"close_price_ma_interval"`
	ClosePriceMA100WindowSize int           `yaml:"close_price_ma100_window_size"`
	ClosePriceMA50WindowSize  int           `yaml:"close_price_ma50_window_size"`
}

type TraderConfig struct {
	RSIBuyValue           float64       `yaml:"rsi_buy_value"`
	RSISellValue          float64       `yaml:"rsi_sell_value"`
	MedianVolumeThreshold float64       `yaml:"median_volume_threshold"`
	SellCheckInterval     time.Duration `yaml:"sell_check_interval"`
	BuyCheckInterval      time.Duration `yaml:"buy_check_interval"`
	BalanceToLeave        float64       `yaml:"balance_to_leave"`
	StopLossRatio         float64       `yaml:"stop_loss_ratio"`
}

type Defaults struct {
	MissingDataValue         float64       `yaml:"missing_data_value"`
	OrderStatusCacheDuration time.Duration `yaml:"order_status_cache_duration"`
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

func GetRSIConfig() (*RSIConfig, error) {
	config := GetConfig()

	if !config.Strategies.RSI.Enabled {
		return nil, fmt.Errorf("RSI is not enabled")
	}

	if err := config.Strategies.RSI.Trader.Validate(); err != nil {
		return nil, fmt.Errorf("RSI config validation failed: %w", err)
	}

	return &config.Strategies.RSI, nil

}

func GetBollingerConfig() (*BollingerConfig, error) {
	config := GetConfig()

	if !config.Strategies.Bollinger.Enabled {
		return nil, fmt.Errorf("Bollinger strategy is disabled")
	}

	// Validate the configuration
	if err := config.Strategies.Bollinger.Trader.Validate(); err != nil {
		return nil, fmt.Errorf("Bollinger config validation failed: %w", err)
	}

	return &config.Strategies.Bollinger, nil
}

func GetEnabledStrategies() map[model.StrategyID]interface{} {
	config := GetConfig()
	enabled := make(map[model.StrategyID]interface{})

	strategiesValue := reflect.ValueOf(config.Strategies)
	strategiesType := reflect.TypeOf(config.Strategies)

	// Iterate through all fields in StrategiesConfig
	for i := 0; i < strategiesValue.NumField(); i++ {
		field := strategiesValue.Field(i)
		fieldType := strategiesType.Field(i)

		if !field.CanInterface() {
			continue
		}

		strategy := field.Interface()

		strategyValue := reflect.ValueOf(strategy)
		if strategyValue.Kind() == reflect.Struct {
			enabledField := strategyValue.FieldByName("Enabled")
			if enabledField.IsValid() && enabledField.Kind() == reflect.Bool && enabledField.Bool() {
				strategyName := strings.ToLower(fieldType.Name)
				enabled[model.ParseStrategyID(strategyName)] = strategy
				log.Printf("Found enabled strategy: %s", strategyName)
			}
		}
	}

	return enabled
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
		log.Printf("Loading local config file\n")
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
