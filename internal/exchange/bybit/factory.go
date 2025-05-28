package bybit

import (
	"fmt"
	"sync"

	"github.com/nihadazimli/crypbot/internal/exchange"

	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/model"
	"github.com/nihadazimli/crypbot/pkg/websocket"
	bybit "github.com/wuhewuhe/bybit.go.api"
)

var (
	marketClient *MarketClient
	marketOnce   sync.Once
)

func NewMarketDataClient() *MarketClient {
	marketOnce.Do(func() {
		marketClient = newMarketDataClientInstance()
	})
	return marketClient
}

func newMarketDataClientInstance() *MarketClient {
	cfg := config.GetConfig()

	m := &MarketClient{
		cfg: cfg,
		BybitClient: bybit.NewBybitHttpClient(
			cfg.Exchange.API.Key,
			cfg.Exchange.API.Secret,
			bybit.WithBaseURL(cfg.Exchange.API.Host),
		),
	}

	return m

}

func NewTradingClientForStrategy(strategyID string) (exchange.TradingClient, error) {
	var credentials config.CredentialsConfig

	switch strategyID {
	case "rsi":
		rsiConfig, err := config.GetRSIConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to get RSI config: %w", err)
		}
		credentials = rsiConfig.Credentials

	case "bollinger":
		bollingerConfig, err := config.GetBollingerConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to get Bollinger config: %w", err)
		}
		credentials = bollingerConfig.Credentials

	default:
		return nil, fmt.Errorf("unknown strategy ID: %s", strategyID)
	}

	return newTradingClient(strategyID, credentials), nil
}

func newTradingClient(strategyID string, credentials config.CredentialsConfig) *TradingClient {
	cfg := config.GetConfig()

	tc := &TradingClient{
		strategyID: strategyID,
		BybitClient: bybit.NewBybitHttpClient(
			credentials.APIKey,
			credentials.APISecret,
			bybit.WithBaseURL(cfg.Exchange.API.Host),
		),
		cfg:                  cfg,
		precision:            make(map[string]model.Precision),
		activeOrders:         make(map[string]ActiveOrderTracker),
		activeOrdersToFollow: make(map[string]struct{}),
	}
	tc.orderWebsocket = websocket.NewWebSocketClientPrivate(
		tc.cfg.Exchange.WebSocket.AddressPrivate,
		credentials.APIKey,
		credentials.APISecret,
		tc.cfg.Exchange.WebSocket.PingIntervalDuration,
		tc.handlePrivateWSMessages,
	)
	tc.monitorActiveOrders(cfg.Exchange.WebSocket.MonitorIntervalDuration)
	return tc
}
