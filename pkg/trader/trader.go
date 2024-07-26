package trader

import (
	"context"
	"fmt"
	"github.com/nihadazimli/crypbot/internal/config"
	"github.com/nihadazimli/crypbot/pkg/financial"
	bybit "github.com/wuhewuhe/bybit.go.api"
	"log"
)

type Client struct {
	buyReady    chan string
	positions   map[string]bool
	cfg         *config.Config
	bybitClient *bybit.Client
}

// CreateTraderClient prepares and returns a new trader client for use in production.
func CreateTraderClient(buyReady chan string) *Client {
	cfg := config.GetConfig()
	client := bybit.NewBybitHttpClient(cfg.Broker.API.Key, cfg.Broker.API.Secret, bybit.WithBaseURL(bybit.TESTNET))

	return NewTraderClient(buyReady, cfg, client)
}

func NewTraderClient(buyReady chan string, cfg *config.Config, client *bybit.Client) *Client {
	return &Client{
		buyReady:    buyReady,
		cfg:         cfg,
		bybitClient: client,
		positions:   make(map[string]bool),
	}
}

func (tc *Client) ProcessBuySignal(fc *financial.Engine) {
	for symbol := range tc.buyReady {
		if tc.buyConditionsMet(fc, symbol) {
			price := tc.getBestBidPrice(symbol)

			tc.placeBuyOrder(symbol, 1, price)
			tc.ProcessSell(fc, symbol, 1)
		}

	}
}
func (tc *Client) placeBuyOrder(symbol string, qty float64, price float64) {

	params := map[string]interface{}{
		"category":    "spot",
		"symbol":      symbol,
		"side":        "Buy",
		"positionIdx": 0,
		"orderType":   "Limit",
		"qty":         qty,   //"0.001",
		"price":       price, //"10000",
		"timeInForce": "GTC",
	}
	orderResult, err := tc.bybitClient.NewTradeService(params).PlaceOrder(context.Background())
	if err != nil {
		log.Println("Failed to place buy order:", err)
		return
	}
	fmt.Println("Order placed: ", bybit.PrettyPrint(orderResult))
	tc.positions[symbol] = true
}

func (tc *Client) ProcessSell(fc *financial.Engine, coinSymbol string, stop_loss float64) {
	//if !(slices.Contains(tc.positions, coinSymbol)) {
	//
	//}
	//
	//for fc.RSIClosed[coinSymbol] < 40 || fc.TickerDataMap[coinSymbol] != stop_loss {
	//
	//}
	//
	if tc.positions[coinSymbol] {
		fmt.Println("Process sell: ", coinSymbol)
		delete(tc.positions, coinSymbol)
	}
}

func (tc *Client) sell(coinKlineSymbol string) {

}

func (tc *Client) buyConditionsMet(fc *financial.Engine, symbol string) bool {

	rsiClosed, ok := fc.GetRSIClosed(symbol)
	if !ok {
		log.Printf("No RSI data available for %s", symbol)
		return false
	}

	volumeMedian, ok := fc.GetVolumeValueMedian(symbol)
	if !ok {
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
	tc.bybitClient.NewOrderBookService() //get order book data
	//out of 50 get the latest
	//return the latest
	return 0.0
}
