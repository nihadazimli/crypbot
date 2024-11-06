// Package trader is handling all trade operations according to signals
package trader

import (
	"context"
	"fmt"
	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/financial"
	"log"
	"time"

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
		log.Printf("Buy Conditions are not met! :(")
		return
	}

	order, err := tc.placeFastBuyOrder(ctx, symbol)

	if err != nil {
		log.Printf("Cannot Place buy order.\nSymbol: %s\nError:%v", symbol, err)
	}

	ticker := time.NewTicker(tc.getBuyCheckIntervalDuration())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Context cancelled while monitoring buy order. Symbol: %s", symbol)
		case <-ticker.C:
			orderStatus := tc.broker.GetOrderStatus(ctx, order.Details.OrderId)

			if orderStatus == broker_api.NotFound {
				log.Printf(
					"Order not found for symbol %s. OrderId: %s",
					symbol,
					order.Details.OrderId,
				)
				continue
			} else if orderStatus == broker_api.Filled {
				log.Printf("Order filled for %s", symbol)
				tc.sellReady <- symbol
				return
			}

			if !tc.buyConditionsMet(symbol) {
				log.Printf("Buy Conditions are not met!\nCancelling current order! :(")
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
				log.Printf("Buy details can't be calculated. Err: %s", err)
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
				orderParams.OrderAmount = remainingQty
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
				log.Printf("Cannot Amend Order. Err: %s", err)
				continue
			}

			log.Printf("Order Id %s is amended successfully", order.OrderId)

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
		log.Printf("Error calculating sell order details: %v", err)
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
	fmt.Println("Buy order placed successfully:", orderResult.Details.OrderId)
	return orderResult, nil
}

func (tc *Trader) ProcessSellSignal(ctx context.Context) {
	for symbol := range tc.sellReady {
		fmt.Println("Sell signal received for", symbol)

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
							fmt.Printf(
								"Cannot cancel order for %s, will retry after %v duration. Err:%v",
								symbol,
								tc.getSellCheckIntervalDuration(),
								err,
							)
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
			orderStatus := tc.broker.GetOrderStatus(ctx, order.Details.OrderId)

			if orderStatus == broker_api.NotFound {
				log.Printf(
					"Order not found for symbol %s. OrderId: %s\n",
					symbol,
					order.Details.OrderId,
				)
				continue
			} else if orderStatus == broker_api.Filled {
				log.Printf("Sell order filled for %s", symbol)
				tc.sellReady <- symbol
				return true
			}

			orderParams, err := tc.CalculateSellOrderDetails(ctx, symbol)
			if err != nil {
				fmt.Printf("Error calculating sell order details: %v", err)
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
				orderParams.OrderAmount = remainingQty
			}
			params := broker_api.AmendOrderRequestParams{
				Symbol:   symbol,
				OrderId:  order.Details.OrderId,
				Price:    &orderParams.LimitPrice,
				Quantity: &orderParams.OrderAmount,
			}
			order, err := tc.broker.AmendOrder(ctx, params)
			if err != nil {
				log.Printf("Cannot Amend Order. Err: %s", err)
				continue
			}
			log.Printf("Order Id %s is amended successfully", order.OrderId)

		}
	}
}

func (tc *Trader) buyConditionsMet(symbol string) bool {
	rsiLive, rsiOk := tc.financial.GetRSILive(symbol)
	if !rsiOk {
		log.Printf("No RSI data available for %s", symbol)
		return false
	}

	volumeMedian, volumeOk := tc.financial.GetVolumeValueMedian(symbol)
	if !volumeOk {
		log.Printf("No volume data available for %s", symbol)
		return false
	}

	if rsiLive > tc.cfg.Trader.RSIBuyValue {
		log.Printf("RSILive too big")
		return false
	}
	_, exists := tc.activeOrders[symbol]
	if exists {
		log.Printf("Already has position for %s", symbol)
		return false
	}

	if volumeMedian < tc.cfg.Trader.MedianVolumeThreshold {
		log.Printf("Median Volume below threshold")
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
