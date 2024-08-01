package trader

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nihadazimli/crypbot/internal/broker_api"
	"github.com/nihadazimli/crypbot/internal/config"
	"github.com/nihadazimli/crypbot/internal/numutils"
	"github.com/nihadazimli/crypbot/pkg/financial"
)

type Trader struct {
	buyReady  chan string
	sellReady chan string
	positions map[string]bool
	cfg       *config.Config
	broker    broker_api.BrokerAPI
	financial financial.EngineInterface
}

// Start prepares and returns a new trader client for use in production.
func Start(buyReady chan string, sellReady chan string, market broker_api.BrokerAPI, financial financial.EngineInterface) *Trader {
	cfg := config.GetConfig()

	return NewTrader(buyReady, sellReady, cfg, market, financial)
}

func NewTrader(buyReady chan string, sellReady chan string, cfg *config.Config, market broker_api.BrokerAPI, financial financial.EngineInterface) *Trader {
	return &Trader{
		buyReady:  buyReady,
		sellReady: sellReady,
		cfg:       cfg,
		broker:    market,
		financial: financial,
		positions: make(map[string]bool),
	}
}

func (tc *Trader) ProcessBuySignal(ctx context.Context) {
	for symbol := range tc.buyReady {
		if tc.buyConditionsMet(symbol) {

			orderParams, err := tc.calculateBuyOrderDetails(ctx, symbol)
			if err != nil {
				log.Printf("Error calculating order details: %v", err)
				continue
			}
			success := tc.placeBuyOrder(ctx, orderParams)
			if success {
				tc.sellReady <- symbol
			}
		}
	}
}

func (tc *Trader) placeBuyOrder(ctx context.Context, orderParams broker_api.BuyOrderRequestParams) bool {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
	defer cancel()

	orderResult, err := tc.broker.PlaceBuyOrder(ctxWithTimeout, orderParams)
	if err != nil {
		log.Println("Failed to place buy order:", err)
		return false
	}
	fmt.Println("Buy order placed successfully:", orderResult.Details.OrderId)
	tc.positions[orderParams.Symbol] = true
	return true
}

func (tc *Trader) ProcessSellSignal(ctx context.Context) {
	for symbol := range tc.sellReady {
		fmt.Println("Sell signal received for", symbol)

		ticker := time.NewTicker(tc.getSellCheckIntervalDuration())
		for {
			select {
			case <-ctx.Done():
				log.Println("Stopping sell checker")
				return
			case <-ticker.C:
				rsiLive, ok := tc.financial.GetRSILive(symbol)
				if !ok {
					log.Println("No RSI live for symbol:", symbol)
					break
				}

				if rsiLive >= 40 {
					log.Printf("RSI hit the sell value. %s:%.2f", symbol, rsiLive)
					if success := tc.executeSellOrder(ctx, symbol); success {
						log.Printf("Successfully executed sell order for %s", symbol)
						return

					} else {
						log.Printf("Failed to execute sell order for %s", symbol)
					}
				}

			}
		}
	}

}

func (tc *Trader) executeSellOrder(ctx context.Context, symbol string) bool {
	bestAskPrice, err := tc.getBestAskPrice(ctx, symbol)
	if err != nil {
		log.Println("Failed to get Best Ask Price for", symbol, " Err:", err)
		return false
	}

	currentPrice, ok := tc.financial.GetCurrentPrice(symbol)
	if !ok {
		log.Println("Failed to get current price", symbol)
		return false
	}
	fmt.Println(bestAskPrice, currentPrice)
	return true
	//1. Get best ask price
	//2. Get the current price (closedPrice in kline)
	//3. Get coin balance that we are currently holding
	//4. If best ask <= current price, then place an order such that:
	//⁃ limit price = best ask
	//⁃ order amount = coin balance
	//5. If best bid < closed price, then place an order such that:
	//⁃ Limit price = (best ask + current price)/2
	//	⁃ Round UP limit price to the same precision as best bid
	//⁃ Order amount = coin balance
	//6. Check if the order is filled at the next candle close.
	//⁃ i.e. if RSI > 40 at 00:15, check if the order is filled at 00:30.
	//7. If yes, done
	//8. If no, repeat the steps 1 to 6.
	//9. If again not filled, place a market order with order size = coin balance
}

func (tc *Trader) buyConditionsMet(symbol string) bool {

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

func (tc *Trader) calculateBuyOrderDetails(ctx context.Context, symbol string) (broker_api.BuyOrderRequestParams, error) {
	bestBidPrice, err := tc.getBestBidPrice(ctx, symbol)
	if err != nil {
		return broker_api.BuyOrderRequestParams{}, err
	}
	currentPrice, ok := tc.financial.GetCurrentPrice(symbol)
	if !ok {
		return broker_api.BuyOrderRequestParams{}, fmt.Errorf("cannot get current price for %s", symbol)
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
	defer cancel()

	accountBalance, err := tc.broker.GetAccountBalance(ctxWithTimeout)
	if err != nil {
		return broker_api.BuyOrderRequestParams{}, fmt.Errorf("cannot get account balance for %s: %v", symbol, err)
	}

	var limitPrice float64
	if bestBidPrice >= currentPrice {
		limitPrice = bestBidPrice
	} else {
		averagePrice := (bestBidPrice + currentPrice) / 2.0
		limitPrice = numutils.RoundDownToSamePrecision(averagePrice, bestBidPrice)
	}
	orderAmount := accountBalance
	stopLimit := 0.95 * limitPrice

	return broker_api.BuyOrderRequestParams{
		Symbol:      symbol,
		LimitPrice:  limitPrice,
		OrderAmount: orderAmount,
		StopLimit:   stopLimit,
	}, nil

}

func (tc *Trader) getAPITimeoutDuration() time.Duration {
	return time.Duration(tc.cfg.Broker.API.Timeout) * time.Second
}

func (tc *Trader) getSellCheckIntervalDuration() time.Duration {
	return time.Duration(tc.cfg.Trader.SellCheckInterval) * time.Second
}

func (tc *Trader) getOrderBook(ctx context.Context, symbol string, limit int) (broker_api.OrderBookResponse, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
	defer cancel()

	params := broker_api.OrderBookRequestParams{
		Symbol: symbol,
		Limit:  limit,
	}
	orderBook, err := tc.broker.GetOrderBook(ctxWithTimeout, params)

	if err != nil {
		return broker_api.OrderBookResponse{}, fmt.Errorf("error getting order book for %s: %v", symbol, err)
	}
	return orderBook, nil
}

func (tc *Trader) getBestBidPrice(ctx context.Context, symbol string) (float64, error) {

	orderBook, err := tc.getOrderBook(ctx, symbol, 1) // we only need top value (i.e highest bid)
	if err != nil {
		return 0, err
	}

	bestBidPrice := orderBook.Bid[0].Price
	return bestBidPrice, nil
}

func (tc *Trader) getBestAskPrice(ctx context.Context, symbol string) (float64, error) {
	orderBook, err := tc.getOrderBook(ctx, symbol, 1) // we only need top value (i.e. lowest ask)
	if err != nil {
		return 0, err
	}

	bestAskPrice := orderBook.Ask[0].Price
	return bestAskPrice, nil
}
