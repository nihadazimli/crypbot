package config

import (
	"fmt"
	"time"

	"github.com/nihadazimli/crypbot/internal/model"
)

type CredentialsConfig struct {
	APIKey       string `yaml:"api_key"`
	APISecret    string `yaml:"api_secret"`
	SubAccountID string `yaml:"sub_account_id,omitempty"`
}
type CommonTraderConfig struct {
	OrderCheckInterval    time.Duration `yaml:"order_check_interval"`
	BalanceToLeave        float64       `yaml:"balance_to_leave"`
	StopLossRatio         float64       `yaml:"stop_loss_ratio"`
	MedianVolumeThreshold float64       `yaml:"median_volume_threshold"`
	APITimeoutDuration    time.Duration `yaml:"api_timeout_duration"`
}

type RSIConfig struct {
	Enabled     bool              `yaml:"enabled"`
	Credentials CredentialsConfig `yaml:"credentials"`
	Trader      RSITraderConfig   `yaml:"trader"`
}

type RSITraderConfig struct {
	CommonTraderConfig `yaml:",inline"`

	// RSI-specific parameters
	BuyThreshold           float64       `yaml:"buy_threshold"`
	SellThreshold          float64       `yaml:"sell_threshold"`
	CloseKlineCycleTimeout time.Duration `yaml:"close_kline_cycle_timeout"`
}

func (r RSITraderConfig) Validate() error {
	if r.OrderCheckInterval < time.Second {
		return fmt.Errorf("order_check_interval too short: %v", r.OrderCheckInterval)
	}
	if r.BuyThreshold <= 0 || r.BuyThreshold >= 100 {
		return fmt.Errorf("buy_threshold must be between 0 and 100")
	}
	if r.SellThreshold <= 0 || r.SellThreshold >= 100 {
		return fmt.Errorf("sell_threshold must be between 0 and 100")
	}
	if r.BuyThreshold >= r.SellThreshold {
		return fmt.Errorf("buy_threshold must be less than sell_threshold")
	}
	return nil
}

type BollingerConfig struct {
	SubAccountID string                `yaml:"sub_account_id"` // Note: different field name
	Enabled      bool                  `yaml:"enabled"`
	Credentials  CredentialsConfig     `yaml:"credentials"`
	Trader       BollingerTraderConfig `yaml:"trader"`
}

type BollingerTraderConfig struct {
	CommonTraderConfig `yaml:",inline"`

	// Bollinger-specific parameters
	MinTradeAmount               float64 `yaml:"min_trade_amount"`
	BalanceVerificationThreshold float64 `yaml:"balance_verification_threshold"`
	BuyThreshold                 float64 `yaml:"buy_threshold"`
	SellThreshold                float64 `yaml:"sell_threshold"`
}

func (b BollingerTraderConfig) Validate() error {
	if b.OrderCheckInterval < time.Second {
		return fmt.Errorf("order_check_interval too short: %v", b.OrderCheckInterval)
	}
	if b.BuyThreshold >= 0 {
		return fmt.Errorf("buy_threshold must be negative")
	}
	if b.SellThreshold >= 0 {
		return fmt.Errorf("sell_threshold must be negative")
	}
	return nil
}

type DummyConfig struct {
	Enabled     bool              `yaml:"enabled"`
	Credentials CredentialsConfig `yaml:"credentials"`
	Trader      DummyTraderConfig `yaml:"trader"`
}

type DummyTraderConfig struct {
	CommonTraderConfig `yaml:",inline"`
}

func GetCommonTraderConfig(strategyID model.StrategyID) (*CommonTraderConfig, error) {
	switch strategyID {
	case model.StrategyRSI:
		rsiConfig, err := GetRSIConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to get RSI config: %w", err)
		}
		return &rsiConfig.Trader.CommonTraderConfig, nil

	case model.StrategyBollinger:
		bollingerConfig, err := GetBollingerConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to get Bollinger config: %w", err)
		}
		return &bollingerConfig.Trader.CommonTraderConfig, nil

	case model.StrategyDummy:
		dummyConfig, err := GetDummyConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to get Dummy config: %w", err)
		}
		return &dummyConfig.Trader.CommonTraderConfig, nil

	default:
		return nil, fmt.Errorf("unknown strategy ID: %s", strategyID)
	}
}
