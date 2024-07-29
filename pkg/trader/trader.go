package trader

import (
	"context"
	"fmt"
	"github.com/nihadazimli/crypbot/internal/broker_api"
	"github.com/nihadazimli/crypbot/internal/config"
	"github.com/nihadazimli/crypbot/pkg/financial"
	"log"
	"strconv"
	"time"
)

type Client struct {
	buyReady  chan string
	positions map[string]bool
	cfg       *config.Config
	broker    broker_api.BrokerAPI
	financial financial.EngineInterface
}

type OrderParams struct {
	Symbol      string
	LimitPrice  float64
	OrderAmount float64
	StopLimit   float64
}

// Start prepares and returns a new trader client for use in production.
func Start(buyReady chan string, market broker_api.BrokerAPI, financial financial.EngineInterface) *Client {
	cfg := config.GetConfig()

	return NewTraderClient(buyReady, cfg, market, financial)
}

func NewTraderClient(buyReady chan string, cfg *config.Config, market broker_api.BrokerAPI, financial financial.EngineInterface) *Client {
	return &Client{
		buyReady:  buyReady,
		cfg:       cfg,
		broker:    market,
		financial: financial,
		positions: make(map[string]bool),
	}
}

func (tc *Client) ProcessBuySignal(ctx context.Context) {
	for symbol := range tc.buyReady {
		if tc.buyConditionsMet(symbol) {

			orderParams, err := tc.calculateOrderDetails(ctx, symbol)
			if err != nil {
				log.Printf("Error calculating order details: %v", err)
				continue
			}
			success := tc.placeBuyOrder(ctx, orderParams)
			if success {
				tc.ProcessSell(ctx, symbol)
			}
		}
	}
}

func (tc *Client) placeBuyOrder(ctx context.Context, orderParams OrderParams) bool {

	params := map[string]interface{}{
		"category":  "spot",
		"symbol":    orderParams.Symbol,
		"side":      "Buy",
		"orderType": "Limit",
		"qty":       orderParams.OrderAmount,
		"price":     orderParams.LimitPrice,
		"stopLoss":  orderParams.StopLimit,
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
	defer cancel()

	orderResult, err := tc.broker.PlaceOrder(ctxWithTimeout, params)
	if err != nil {
		log.Println("Failed to place buy order:", err)
		return false
	}
	fmt.Println("Buy order placed successfully:", orderResult.Result.OrderId)
	tc.positions[orderParams.Symbol] = true
	return true
}

func (tc *Client) ProcessSell(ctx context.Context, coinSymbol string) {
	//if !(slices.Contains(tc.positions, coinSymbol)) {
	//
	//}
	//
	//for tc.financial.RSIClosed[coinSymbol] < 40 || tc.financial.TickerDataMap[coinSymbol] != stop_loss {
	//
	//}
	if tc.positions[coinSymbol] {
		fmt.Println("Process sell: ", coinSymbol)
		delete(tc.positions, coinSymbol)
	}
}

func (tc *Client) sell(coinKlineSymbol string) {

}

func (tc *Client) buyConditionsMet(symbol string) bool {

	rsiClosed, rsiOk := tc.financial.GetRSIClosed(symbol)
	if !rsiOk {
		log.Printf("No RSI data available for %s", symbol)
		return false
	}

	volumeMedian, volumeOk := tc.financial.GetVolumeValueMedian(symbol)
	if !volumeOk {
		log.Printf("No volume data available for %s", symbol)
		return false
	}

	if rsiClosed > tc.cfg.Trader.RSIBuyValue {
		log.Printf("RSIClosed too big")
		return false
	}

	if tc.positions[symbol] {
		log.Printf("Already has position for %s", symbol)
		return false
	}

	if volumeMedian < tc.cfg.Trader.MedianVolumeThreshold {
		log.Printf("Median Volume below threshold")
		return false
	}

	return true
}

func (tc *Client) getBestBidPrice(symbol string) float64 {
	params := map[string]interface{}{
		"abc": 1,
	}
	res, err := tc.broker.GetOrderBook(context.Background(), params)
	if err != nil {
		return 0
	} //get order book data
	//out of 50 get the latest
	//return the latest
	result, err := strconv.ParseFloat(res, 64)
	if err != nil {
		return 0
	}
	return result
}

func (tc *Client) calculateOrderDetails(ctx context.Context, symbol string) (OrderParams, error) {
	bestBidPrice := tc.getBestBidPrice(symbol)
	currentPrice, ok := tc.financial.GetCurrentPrice(symbol)
	if !ok {
		return OrderParams{}, fmt.Errorf("cannot get current price for %s", symbol)
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
	defer cancel()

	accountBalance, err := tc.broker.GetAccountBalance(ctxWithTimeout)
	if err != nil {
		return OrderParams{}, fmt.Errorf("cannot get account balance for %s: %v", symbol, err)
	}

	var limitPrice float64
	if bestBidPrice >= currentPrice {
		limitPrice = bestBidPrice
	} else {
		limitPrice = (bestBidPrice + currentPrice) / 2
	}
	orderAmount := accountBalance
	stopLimit := 0.95 * limitPrice

	return OrderParams{
		Symbol:      symbol,
		LimitPrice:  limitPrice,
		OrderAmount: orderAmount,
		StopLimit:   stopLimit,
	}, nil

}

func (tc *Client) getAPITimeoutDuration() time.Duration {
	return time.Duration(tc.cfg.Broker.API.Timeout) * time.Second
}
