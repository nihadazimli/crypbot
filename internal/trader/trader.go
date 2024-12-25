// Package trader is handling all trade operations according to signals
package trader

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/financial"

	"github.com/nihadazimli/crypbot/internal/broker_api"
	"github.com/nihadazimli/crypbot/pkg/numutils"
)

type Trader struct {
	buyReady     chan string
	sellReady    chan string
	activeOrders map[string]float64
	cfg          *config.Config
	broker       broker_api.BrokerAPI
	financial    financial.EngineInterface
}

// Start prepares and returns a new trader client for use in production.
func Start(
	buyReady chan string,
	sellReady chan string,
	market broker_api.BrokerAPI,
	financial financial.EngineInterface,
) *Trader {
	cfg := config.GetConfig()

	return NewTrader(buyReady, sellReady, cfg, market, financial)
}

func NewTrader(
	buyReady chan string,
	sellReady chan string,
	cfg *config.Config,
	market broker_api.BrokerAPI,
	financial financial.EngineInterface,
) *Trader {
	return &Trader{
		buyReady:     buyReady,
		sellReady:    sellReady,
		cfg:          cfg,
		broker:       market,
		financial:    financial,
		activeOrders: make(map[string]float64),
	}
}

func (tc *Trader) ProcessBuySignal(ctx context.Context) {
	for symbol := range tc.buyReady {
		tc.handleBuyOrder(ctx, symbol)
	}
}

func (tc *Trader) handleBuyOrder(ctx context.Context, symbol string) {

	if !tc.buyConditionsMet(symbol) {
		log.Printf("Buy conditions are not met for %s! :(\n", symbol)
		return
	}
	err := tc.broker.StartOrderStream()
	defer func() {
		err := tc.broker.StopOrderStream()
		if err != nil {
			log.Printf("Buy Order - Order stream cannot be stopped! Err:%v\n", err)
		}
	}()

	if err != nil {
		log.Printf("Cannot start listening to order stream. Err: %v\n", err)
		return
	}

	order, err := tc.placeFastBuyOrder(ctx, symbol)

	if err != nil {
		log.Printf("Cannot place buy order for %s. Err: %v\n", symbol, err)
		return
	}

	ticker := time.NewTicker(tc.getBuyCheckIntervalDuration())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Context cancelled while monitoring buy order. Symbol: %s\n", symbol)
		case <-ticker.C:
			orderStatus, err := tc.broker.GetOrderStatus(ctx, order.Details.OrderId)
			if err != nil {
				log.Printf(
					"Order not found for symbol %s. OrderId: %s\n",
					symbol,
					order.Details.OrderId,
				)
				continue
			}

			if orderStatus == broker_api.Filled || orderStatus == broker_api.PartiallyFilledCancelled {
				log.Printf("Order filled for %s\n", symbol)
				tc.activeOrders[symbol] = order.Details.OrderAmount
				tc.sellReady <- symbol
				return
			}

			if !tc.buyConditionsMet(symbol) {
				log.Printf("Buy Conditions are not met for %s! Cancelling current order! :(\n", symbol)
				params := broker_api.CancelOrderRequestParams{
					OrderId: order.Details.OrderId,
					Symbol:  symbol,
				}
				cancelOrder, err := tc.broker.CancelOrder(ctx, params)
				if err != nil {
					log.Printf("Cannot cancel order! PANIC!. Err: %v\n", err)
					return
				}
				//TODO: Check from websocket if order is cancelled
				log.Printf("Order %s cancelled successfully!\n", cancelOrder.OrderId)
				return
			}

			// For the rest of the states we need to create buy order
			orderParams, err := tc.CalculateBuyOrderDetails(ctx, symbol)
			if err != nil {
				log.Printf("Buy details can't be calculated. Err: %s\n", err)
				continue
			}

			if orderStatus == broker_api.PartiallyFilled {
				params := broker_api.OpenOrderRequestParams{OrderId: order.Details.OrderId}
				remainingQty, err := tc.broker.GetRemainingQtyForPartialOrder(ctx, params)
				if err != nil {
					log.Printf(
						"Cant get Remaining qty for buy order %s. Error: %v\n",
						order.Details.OrderId,
						err,
					)
					continue
				}
				log.Printf("Remaining qty for %s is %v", symbol, remainingQty)
				//orderParams.OrderAmount = remainingQty
			}

			params := broker_api.AmendOrderRequestParams{
				Symbol:   symbol,
				OrderId:  order.Details.OrderId,
				Price:    &orderParams.LimitPrice,
				Quantity: &orderParams.OrderAmount,
				StopLoss: &orderParams.StopLimit,
			}
			order, err := tc.broker.AmendOrder(ctx, params)
			if err != nil {
				if errors.Is(err, broker_api.ErrQuantityTooLow) {
					cancelParams := broker_api.CancelOrderRequestParams{OrderId: order.OrderId, Symbol: symbol}
					_, err := tc.broker.CancelOrder(ctx, cancelParams)
					if err != nil {
						log.Printf("Trader - Cannot cancel order! Err: %v\n", err)
					}
					log.Printf(
						"Tradar - Partially Filled order id %s cancelled due to low remaining qtyt\n",
						order.OrderId,
					)
				} else {
					log.Printf("Trader - Cannot Amend Order. Unhandled err: %s\n", err)
				}
				continue
			}

			log.Printf("Order Id %s is amended successfully\n", order.OrderId)

		}
	}
}

func (tc *Trader) placeFastSellOrder(
	ctx context.Context,
	symbol string,
) (broker_api.SellOrderResult, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
	defer cancel()

	orderParams, err := tc.CalculateSellOrderDetails(ctx, symbol)
	if err != nil {
		log.Printf("Error calculating sell order details: %v\n", err)
	}

	orderResult, err := tc.broker.PlaceSellOrder(ctxWithTimeout, orderParams)
	if err != nil {
		return orderResult, err
	}
	log.Printf("Sell order placed successfully:%v\n", orderResult.Details.OrderId)
	return orderResult, nil
}

func (tc *Trader) placeFastBuyOrder(
	ctx context.Context,
	symbol string,
) (broker_api.BuyOrderResult, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
	defer cancel()

	orderParams, err := tc.CalculateBuyOrderDetails(ctx, symbol)
	if err != nil {
		return broker_api.BuyOrderResult{}, fmt.Errorf("Error calculating buy order details: %v\n", err)
	}

	orderResult, err := tc.broker.PlaceBuyOrder(ctxWithTimeout, orderParams)
	if err != nil {
		return orderResult, err
	}
	log.Println("Buy order placed successfully:", orderResult.Details.OrderId)
	return orderResult, nil
}

func (tc *Trader) ProcessSellSignal(ctx context.Context) {
	for symbol := range tc.sellReady {
		log.Println("Sell signal received for", symbol)

		go func(symbol string) {

			ticker := time.NewTicker(tc.getSellCheckIntervalDuration())
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					log.Println("Stopping sell signal checker")
					return
				case <-ticker.C:
					rsiLive, ok := tc.financial.GetRSILive(symbol)
					if !ok {
						log.Printf("No RSI live for symbol:%s\n", symbol)
						return
					}

					if rsiLive >= tc.getRSISellValue() {
						log.Printf("RSI hit the sell value. %s:%.2f\n", symbol, rsiLive)
						err := tc.broker.CancelAllTPSLOrders(ctx, symbol)
						if err != nil {
							log.Printf(
								"Cannot cancel order for %s, will retry after %v duration. Err:%v",
								symbol,
								tc.getSellCheckIntervalDuration(),
								err,
							)
							continue
						}
						if success := tc.executeSellOrder(ctx, symbol); success {
							log.Printf("Successfully executed sell order for %s\n", symbol)
							delete(tc.activeOrders, symbol)
							return
						} else {
							log.Printf("Failed to execute sell order for %s\n", symbol)
						}
					}

				}
			}
		}(symbol)
	}

}

func (tc *Trader) executeSellOrder(ctx context.Context, symbol string) bool {

	err := tc.broker.StartOrderStream()
	if err != nil {
		return false
	}
	defer func() {
		err := tc.broker.StopOrderStream()
		if err != nil {
			log.Printf("Order stream cannot be stopped! Err:%v\n", err)
		}
	}()
	order, err := tc.placeFastSellOrder(ctx, symbol)
	if err != nil {
		log.Printf("Cannot Place sell order.\nSymbol: %s\nError:%v\n", symbol, err)
	}

	sellTicker := time.NewTicker(tc.getSellOrderCheckDuration())
	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping executing sell order")
		case <-sellTicker.C:
			orderStatus, err := tc.broker.GetOrderStatus(ctx, order.Details.OrderId)

			if err != nil {
				log.Printf(
					"Order not found for symbol %s. OrderId: %s\n",
					symbol,
					order.Details.OrderId,
				)
				continue
			}

			if orderStatus == broker_api.Filled {
				log.Printf("Sell order filled for %s\n", symbol)
				return true
			}

			orderParams, err := tc.CalculateSellOrderDetails(ctx, symbol)
			if err != nil {
				log.Printf("Error calculating sell order details: %v\n", err)
				continue
			}

			if orderStatus == broker_api.PartiallyFilled {
				params := broker_api.OpenOrderRequestParams{OrderId: order.Details.OrderId}
				remainingQty, err := tc.broker.GetRemainingQtyForPartialOrder(ctx, params)
				if err != nil {
					log.Printf(
						"Can't get Remaining qty for sell order %s. Error: %v\n",
						order.Details.OrderId,
						err,
					)
					continue
				}
				log.Printf("Remaining qty for %s is %v", symbol, remainingQty)
				//orderParams.OrderAmount = remainingQty
			}
			params := broker_api.AmendOrderRequestParams{
				Symbol:   symbol,
				OrderId:  order.Details.OrderId,
				Price:    &orderParams.LimitPrice,
				Quantity: &orderParams.OrderAmount,
			}
			order, err := tc.broker.AmendOrder(ctx, params)
			if err != nil {
				log.Printf("Cannot Amend Order. Err: %s\n", err)
				continue
			}
			log.Printf("Order Id %s is amended successfully\n", order.OrderId)

		}
	}
}

func (tc *Trader) buyConditionsMet(symbol string) bool {

	if len(tc.activeOrders) > 0 {
		log.Printf("Already has a position. Active orders: %v", tc.activeOrders)
		return false
	}
	rsiLive, rsiOk := tc.financial.GetRSILive(symbol)
	if !rsiOk {
		log.Println("No RSI data available")
		return false
	}

	volumeMedian, volumeOk := tc.financial.GetVolumeValueMedian(symbol)
	if !volumeOk {
		log.Println("No volume data available")
		return false
	}

	movingAverages, err := tc.financial.GetMovingAverage(symbol)
	if err != nil {
		log.Printf("Could not retrieve MA - Err: %s\n", err)
		return false
	}

	if movingAverages.MA50 < movingAverages.MA100 {
		log.Printf("Moving average short is smaller than moving average long. MA values: %v\n", movingAverages)
		return false
	}

	if rsiLive > tc.cfg.Trader.RSIBuyValue {
		log.Println("RSILive too big")
		return false
	}
	_, exists := tc.activeOrders[symbol]
	if exists {
		log.Println("Already has position for given symbol")
		return false
	}

	if volumeMedian < tc.cfg.Trader.MedianVolumeThreshold {
		log.Println("Median Volume below threshold")
		return false
	}

	return true
}

func (tc *Trader) CalculateSellOrderDetails(
	ctx context.Context,
	symbol string,
) (broker_api.SellOrderRequestParams, error) {
	bestAskPrice, err := tc.getBestAskPrice(ctx, symbol)
	if err != nil {
		log.Println("Failed to get Best Ask Price for", symbol, " Err:", err)
		return broker_api.SellOrderRequestParams{}, fmt.Errorf(
			"failed to get Best Ask Price for %s Err: %v",
			symbol,
			err,
		)
	}

	currentPrice, ok := tc.financial.GetCurrentPrice(symbol)
	if !ok {
		return broker_api.SellOrderRequestParams{}, fmt.Errorf(
			"failed to get current price for %s",
			symbol,
		)
	}

	var limitPrice float64
	if bestAskPrice <= currentPrice {
		limitPrice = bestAskPrice
	} else {
		averagePrice := (bestAskPrice + currentPrice) / 2
		limitPrice = numutils.RoundUpToPrecision(averagePrice, bestAskPrice)
	}
	orderAmount, err := tc.broker.GetSymbolBalance(ctx, symbol)
	if err != nil {
		return broker_api.SellOrderRequestParams{}, fmt.Errorf(
			"failed to get balance for %s symbol",
			symbol,
		)
	}
	//TODO: When orderAmount is zero check through Broker as well. Or generate a logic to construct it.
	return broker_api.SellOrderRequestParams{
		Symbol:      symbol,
		OrderAmount: orderAmount,
		LimitPrice:  limitPrice,
	}, err

}

func (tc *Trader) CalculateBuyOrderDetails(
	ctx context.Context,
	symbol string,
) (broker_api.BuyOrderRequestParams, error) {
	bestBidPrice, err := tc.getBestBidPrice(ctx, symbol)
	if err != nil {
		return broker_api.BuyOrderRequestParams{}, err
	}
	currentPrice, ok := tc.financial.GetCurrentPrice(symbol)
	if !ok {
		return broker_api.BuyOrderRequestParams{}, fmt.Errorf(
			"cannot get current price for %s",
			symbol,
		)
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
	defer cancel()

	accountBalance, err := tc.broker.GetAccountBalance(ctxWithTimeout)
	if err != nil {
		return broker_api.BuyOrderRequestParams{}, fmt.Errorf(
			"cannot get account balance for %s: %v",
			symbol,
			err,
		)
	}

	var limitPrice float64
	if bestBidPrice >= currentPrice {
		limitPrice = bestBidPrice
	} else {
		averagePrice := (bestBidPrice + currentPrice) / 2.0
		limitPrice = numutils.RoundDownToPrecision(averagePrice, bestBidPrice)
	}
	orderAmount := (accountBalance.Total - tc.cfg.Trader.BalanceToLeave) / limitPrice
	stopLimit := tc.cfg.Trader.StopLossRatio * limitPrice

	return broker_api.BuyOrderRequestParams{
		Symbol:      symbol,
		LimitPrice:  limitPrice,
		OrderAmount: orderAmount,
		StopLimit:   stopLimit,
	}, nil

}

func (tc *Trader) getSellOrderCheckDuration() time.Duration {
	return time.Duration(tc.cfg.Trader.SellCheckInterval) * time.Second
}

func (tc *Trader) getAPITimeoutDuration() time.Duration {
	return time.Duration(tc.cfg.Broker.API.Timeout) * time.Second
}

func (tc *Trader) getBuyCheckIntervalDuration() time.Duration {
	return time.Duration(tc.cfg.Trader.BuyCheckInterval) * time.Second
}

func (tc *Trader) getSellCheckIntervalDuration() time.Duration {
	return time.Duration(tc.cfg.Trader.SellCheckInterval) * time.Second
}

func (tc *Trader) getRSISellValue() float64 {
	return tc.cfg.Trader.RSISellValue
}

func (tc *Trader) getOrderBook(
	ctx context.Context,
	symbol string,
	limit int,
) (broker_api.OrderBookResult, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
	defer cancel()

	params := broker_api.OrderBookRequestParams{
		Symbol: symbol,
		Limit:  limit,
	}
	orderBook, err := tc.broker.GetOrderBook(ctxWithTimeout, params)

	if err != nil {
		return broker_api.OrderBookResult{}, fmt.Errorf(
			"error getting order book for %s: %v",
			symbol,
			err,
		)
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
