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

type BuyConditionResult int

const (
	BuyConditionMet BuyConditionResult = iota
	BuyConditionWait
	BuyConditionCancel
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
	ticker := time.NewTicker(tc.getBuyCheckIntervalDuration())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Buy Order - Context cancelled while waiting for buy conditions. Symbol: %s\n", symbol)
			return
		case <-ticker.C:
			condition := tc.checkBuyConditions(symbol)

			switch condition {
			case BuyConditionMet:
				if err := tc.placeBuyOrderAndMonitor(ctx, symbol); err != nil {
					log.Printf("Buy Order - Failed to place order for %s. Err: %v\n", symbol, err)
				}
				return
			case BuyConditionCancel:
				return
			case BuyConditionWait:
				continue
			}
		}
	}

}

func (tc *Trader) placeBuyOrderAndMonitor(ctx context.Context, symbol string) error {
	err := tc.broker.StartOrderStream()
	if err != nil {
		return fmt.Errorf("cannot start listenting to order stream. Err: %v", err)
	}
	defer tc.broker.StopOrderStream()

	order, err := tc.placeFastBuyOrder(ctx, symbol)

	if err != nil {
		return fmt.Errorf("cannot place order. %v", err)
	}

	return tc.monitorBuyOrder(ctx, order, symbol)
}

func (tc *Trader) monitorBuyOrder(ctx context.Context, initialOrder broker_api.BuyOrderResult, symbol string) error {
	ticker := time.NewTicker(tc.getBuyCheckIntervalDuration())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("monitorBuyOrder - context cancelled while monitoring buy for %s", symbol)
		case <-ticker.C:
			orderStatus, err := tc.broker.GetOrderStatus(ctx, initialOrder.Details.OrderId)
			if err != nil {
				return fmt.Errorf("monitorBuyOrder - not found initialOrder id %s. Err: %v", initialOrder.Details.OrderId, err)
			}

			switch orderStatus {
			case broker_api.Filled, broker_api.PartiallyFilledCancelled:
				log.Printf("Order filled for %s\n", symbol)
				tc.triggerSell(symbol)
				return nil
			case broker_api.Open, broker_api.PartiallyFilled:
				condition := tc.checkBuyConditions(symbol)

				switch condition {
				case BuyConditionMet:
					orderParams, err := tc.CalculateBuyOrderDetails(ctx, symbol)
					if err != nil {
						log.Printf("Buy details can't be calculated. Err: %s\n", err)
						continue
					}
					params := broker_api.AmendOrderRequestParams{
						Symbol:   symbol,
						OrderId:  initialOrder.Details.OrderId,
						Price:    &orderParams.LimitPrice,
						Quantity: &orderParams.OrderAmount,
						StopLoss: &orderParams.StopLimit,
					}
					amendedOrder, err := tc.broker.AmendOrder(ctx, params)
					if err != nil {
						if errors.Is(err, broker_api.ErrQuantityTooLow) {
							cancelParams := broker_api.CancelOrderRequestParams{OrderId: amendedOrder.OrderId, Symbol: symbol}
							_, err = tc.broker.CancelOrder(ctx, cancelParams)
							if err != nil {
								return fmt.Errorf("monitor buy order - cannot cancel low qty order. Err:%v", err)
							}
							log.Printf(
								"Monitor buy order - Partially Filled order %s cancelled due to low remaining qty\n",
								amendedOrder.OrderId,
							)
						} else {
							return fmt.Errorf("monitor buy order - Cannot Amend Order. Unhandled err: %v", err)

						}
						err = tc.gracefullyFinishBuyOrder(ctx, symbol)
						if err != nil {
							return fmt.Errorf("monitor buy order - %v", err)
						}
						return nil
					}
				case BuyConditionWait:
					continue
				case BuyConditionCancel:
					cancelParams := broker_api.CancelOrderRequestParams{OrderId: initialOrder.Details.OrderId, Symbol: symbol}
					_, err = tc.broker.CancelOrder(ctx, cancelParams)
					if err != nil {
						return fmt.Errorf("monitor buy order - cannot cancel conditions failed order. Err:%v", err)
					}
					err = tc.gracefullyFinishBuyOrder(ctx, symbol)
					if err != nil {
						return fmt.Errorf("monitor buy order - %v", err)
					}
					return nil
				}

			case broker_api.Invalid, broker_api.Canceled:
				err = tc.gracefullyFinishBuyOrder(ctx, symbol)
				if err != nil {
					return fmt.Errorf("monitorBuyOrder - %v", err)
				}
				return nil
			default:
				log.Printf("Order Status: %v This status is not handled\n", orderStatus)
				err = tc.gracefullyFinishBuyOrder(ctx, symbol)
				if err != nil {
					return fmt.Errorf("monitorBuyOrder - %v", err)
				}
				return nil
			}
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
					rsiClosed, ok := tc.financial.GetRSIClosed(symbol)
					if !ok {
						log.Printf("No RSI closed for symbol:%s\n", symbol)
						return
					}

					if rsiClosed >= tc.getRSISellValue() {
						log.Printf("RSI hit the sell value. %s:%.2f\n", symbol, rsiClosed)
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

func (tc *Trader) checkBuyConditions(symbol string) BuyConditionResult {
	return BuyConditionMet
	if len(tc.activeOrders) > 0 {
		log.Printf("Already has a position. Active orders: %v", tc.activeOrders)
		return BuyConditionCancel
	}
	_, exists := tc.activeOrders[symbol]
	if exists {
		log.Println("Already has position for given symbol")
		return BuyConditionCancel
	}

	rsiLive, rsiOk := tc.financial.GetRSILive(symbol)
	if !rsiOk {
		log.Println("No RSI data available")
		// QUESTION: Should I send construct signal to financial for missing coins? Isn't it coupling these 2 packages?
		return BuyConditionCancel
	}

	rsiClosed, rsiOk := tc.financial.GetRSIClosed(symbol)
	if !rsiOk {
		log.Println("No RSI data available")
		// QUESTION: Should I send construct signal to financial for missing coins? Isn't it coupling these 2 packages?
		return BuyConditionCancel
	}

	volumeMedian, volumeOk := tc.financial.GetVolumeValueMedian(symbol)
	if !volumeOk {
		log.Println("No volume data available")
		return BuyConditionCancel
	}

	movingAverages, err := tc.financial.GetMovingAverage(symbol)
	if err != nil {
		log.Printf("Could not retrieve MA - Err: %s\n", err)
		return BuyConditionCancel
	}

	if movingAverages.MA50 < movingAverages.MA100 {
		log.Printf("Moving average short (%.2f) is smaller than moving average long (%.2f)\n", movingAverages.MA50, movingAverages.MA100)
		return BuyConditionCancel
	}

	if rsiClosed > tc.cfg.Trader.RSIBuyValue {
		log.Println("RSIClose too big")
		return BuyConditionCancel
	}

	if volumeMedian < tc.cfg.Trader.MedianVolumeThreshold {
		log.Println("Median Volume below threshold")
		return BuyConditionCancel
	}

	if rsiLive > tc.cfg.Trader.RSIBuyValue {
		log.Println("RSILive too big")
		return BuyConditionWait
	}

	return BuyConditionMet
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
	coinBalance, err := tc.broker.GetSymbolBalance(ctx, symbol)
	if err != nil {
		return broker_api.SellOrderRequestParams{}, fmt.Errorf(
			"failed to get balance for %s symbol",
			symbol,
		)
	}
	//TODO: When orderAmount is zero check through Broker as well. Or generate a logic to construct it.
	return broker_api.SellOrderRequestParams{
		Symbol:      symbol,
		OrderAmount: coinBalance.WalletBalance,
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

func (tc *Trader) gracefullyFinishBuyOrder(ctx context.Context, symbol string) error {
	coinBalance, err := tc.broker.GetSymbolBalance(ctx, symbol)
	if err != nil {
		return err
	}

	if coinBalance.UsdValue > 1 {
		tc.triggerSell(symbol)
	}
	return nil
}

func (tc *Trader) triggerSell(symbol string) {
	tc.activeOrders[symbol] = 1
	tc.sellReady <- symbol
}
