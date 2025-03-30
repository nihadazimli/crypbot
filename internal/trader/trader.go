// Package trader is handling all trade operations according to signals
package trader

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nihadazimli/crypbot/internal/model"

	"github.com/nihadazimli/crypbot/internal/order"

	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/financial"

	"github.com/nihadazimli/crypbot/internal/exchange"
	"github.com/nihadazimli/crypbot/pkg/numutils"
)

type OrderDetails struct {
	Price    float64
	Quantity float64
	StopLoss float64
}

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
	exchange     exchange.Exchange
	order        order.OrderManager
	financial    financial.EngineInterface
}

func NewTrader(
	buyReady chan string,
	sellReady chan string,
	market exchange.Exchange,
	financial financial.EngineInterface,
	order order.OrderManager,
) *Trader {
	cfg := config.GetConfig()

	return &Trader{
		buyReady:     buyReady,
		sellReady:    sellReady,
		cfg:          cfg,
		exchange:     market,
		financial:    financial,
		order:        order,
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
func (tc *Trader) BuyCoin(ctx context.Context, symbol string) error {
	err := tc.placeBuyOrderAndMonitor(ctx, symbol)
	if err != nil {
		return err
	}
	return nil
}

func (tc *Trader) SellCoin(ctx context.Context, symbol string, sellWithCheck bool) error {

	log.Printf("Starting selling manually %s. Checks conditions set to %v", symbol, sellWithCheck)
	if sellWithCheck {
		go tc.ProcessSellSignal(ctx)
		tc.triggerSell(symbol)
	} else {
		err := tc.exchange.CancelAllTPSLOrders(ctx, symbol)
		if err != nil {
			log.Printf("Error cancelling tpsl orders. Err: %v\n", err)
		}
		result := tc.executeSellOrder(ctx, symbol)
		if result != true {
			return fmt.Errorf("cant sell coin")
		}
	}
	return nil
}
func (tc *Trader) placeBuyOrderAndMonitor(ctx context.Context, symbol string) error {
	createCtx, createCancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
	createdOrder, err := tc.createLimitOrder(createCtx, symbol, model.OrderSideBuy)
	createCancel()

	if err != nil {
		return fmt.Errorf("placeBuyOrderAndMonitor - %w", err)
	}
	log.Printf("Buy order placed successfully. Order Params: %+v\n", createdOrder)

	ticker := time.NewTicker(tc.getBuyCheckIntervalDuration())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("monitorBuyOrder - context cancelled while monitoring buy for %s", symbol)
		case <-ticker.C:
			createdOrder, err = tc.order.GetById(ctx, createdOrder.ID)
			if err != nil {
				return fmt.Errorf(
					"monitorBuyOrder - not found order id %s. Err: %v",
					createdOrder.ID,
					err,
				)
			}

			switch createdOrder.Status {
			case model.OrderFilled, model.OrderPartiallyFilledCancelled:
				log.Printf("Order filled for %s\n", symbol)
				tc.triggerSell(symbol)
				return nil
			case model.OrderOpen, model.OrderPartiallyFilled:
				condition := tc.checkBuyConditions(symbol)

				switch condition {
				case BuyConditionMet:
					updateCtx, updateCancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
					updatedOrder, err := tc.updateLimitOrder(updateCtx, symbol, createdOrder.ID, model.OrderSideBuy)
					updateCancel()

					if err != nil {
						var oerr *order.Error
						if errors.As(err, &oerr) {
							switch oerr.Type {
							case order.ErrInvalidParams:
								err = tc.order.Cancel(ctx, updatedOrder.ID)
								if err != nil {
									return fmt.Errorf("monitor buy order - cannot cancel low qty order. Err:%v", err)
								}
								log.Printf(
									"Monitor buy order - order %s with status %s cancelled due to low remaining qty\n",
									updatedOrder.ID, updatedOrder.Status,
								)
							case order.ErrOrderUnchangedParams:
								continue
							case order.ErrOrderAtFinalState:
								log.Printf(
									"Monitor buy order - Probably Filled order %s does not exist anymore\n",
									oerr.OrderID,
								)
							case order.ErrOrderNotFound:
								log.Printf(
									"Monitor buy order - Order cannot be found. Weird! Order Id: %s,Error order id: %s err: %v\n",
									createdOrder.ID,
									oerr.OrderID,
									oerr.Err,
								)
								gracefulErr := tc.gracefullyFinishOrder(ctx, symbol, false)
								if gracefulErr != nil {
									return fmt.Errorf("monitor buy order - %v", err)
								}
								return err
							default:
								gracefulErr := tc.gracefullyFinishOrder(ctx, symbol, false)
								if gracefulErr != nil {
									return fmt.Errorf("monitor buy order - %v", err)
								}
								return fmt.Errorf(
									"monitor buy order - Cannot Amend Order id %v. Unhandled err: %v",
									updatedOrder.ID,
									err,
								)
							}
						} else {
							if errors.Is(err, BuyDetailsErrCannotCalculate) {
								log.Printf("Buy details can't be calculated. Err: %s\n", err)
								continue
							} else {
								return fmt.Errorf("error does not match order error. Err: %e", err)
							}
						}
						return nil
					}

				case BuyConditionWait:
					continue
				case BuyConditionCancel:
					err = tc.order.Cancel(ctx, createdOrder.ID)
					if err != nil {
						return fmt.Errorf("monitor buy order - cannot cancel conditions failed order. Err:%v", err)
					}
					err = tc.gracefullyFinishOrder(ctx, symbol, false)
					if err != nil {
						return fmt.Errorf("monitor buy order - %v", err)
					}
					return nil
				}

			case model.OrderInvalid, model.OrderCanceled:
				err = tc.gracefullyFinishOrder(ctx, symbol, false)
				if err != nil {
					return fmt.Errorf("monitorBuyOrder - %v", err)
				}
				return nil
			default:
				log.Printf("Order Status: %v This status is not handled\n", createdOrder)
				err = tc.gracefullyFinishOrder(ctx, symbol, false)
				if err != nil {
					return fmt.Errorf("monitorBuyOrder - %v", err)
				}
				return nil
			}
		}
	}
}

func (tc *Trader) updateLimitOrder(ctx context.Context, symbol string, orderId string, side model.OrderSide) (*order.Order, error) {
	var updateParams order.UpdateParams
	if side == model.OrderSideBuy {
		updatedBuyDetails, err := tc.CalculateBuyOrderDetails(ctx, symbol, model.OrderTypeLimit)
		if err != nil {
			return nil, fmt.Errorf(
				"updateLimitOrder - failed to calculate buy details: %w",
				BuyDetailsErrCannotCalculate,
			)
		}
		updateParams = order.UpdateParams{
			OrderID:  orderId,
			Symbol:   symbol,
			Price:    &updatedBuyDetails.Price,
			Quantity: &updatedBuyDetails.Quantity,
			StopLoss: &updatedBuyDetails.StopLoss,
		}
	} else if side == model.OrderSideSell {
		updatedSellDetails, err := tc.CalculateSellOrderDetails(ctx, symbol, model.OrderTypeLimit)
		if err != nil {
			return nil, fmt.Errorf(
				"updateLimitOrder - failed to calculate sell details: %w",
				SellDetailsErrCannotCalculate,
			)
		}
		updateParams = order.UpdateParams{
			OrderID:  orderId,
			Symbol:   symbol,
			Price:    &updatedSellDetails.Price,
			Quantity: &updatedSellDetails.Quantity,
		}
	}

	updatedOrder, err := tc.order.Update(ctx, updateParams)
	if err != nil {
		return nil, fmt.Errorf("updateLimitOrder - %w", err)
	}
	return updatedOrder, nil
}

func (tc *Trader) createLimitOrder(ctx context.Context, symbol string, side model.OrderSide) (*order.Order, error) {

	var createParams order.CreateParams
	if side == model.OrderSideBuy {
		orderDetails, err := tc.CalculateBuyOrderDetails(ctx, symbol, model.OrderTypeLimit)
		if err != nil {
			return nil, fmt.Errorf("createLimitOrder - Error calculating buy order details: %v\n", err)
		}
		createParams = order.CreateParams{
			Symbol:   symbol,
			Side:     model.OrderSideBuy,
			Type:     model.OrderTypeLimit,
			Price:    &orderDetails.Price,
			Quantity: orderDetails.Quantity,
			StopLoss: &orderDetails.StopLoss,
		}
	} else if side == model.OrderSideSell {
		orderDetails, err := tc.CalculateSellOrderDetails(ctx, symbol, model.OrderTypeLimit)
		if err != nil {
			return nil, fmt.Errorf("createLimitOrder - Error calculating sell order details: %v\n", err)
		}
		createParams = order.CreateParams{
			Symbol:   symbol,
			Side:     model.OrderSideSell,
			Type:     model.OrderTypeLimit,
			Price:    &orderDetails.Price,
			Quantity: orderDetails.Quantity,
		}

	}
	createdOrder, err := tc.order.Create(ctx, createParams)
	if err != nil {
		return nil, err
	}

	return createdOrder, nil

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
						err := tc.exchange.CancelAllTPSLOrders(ctx, symbol)
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
	orderCtx, orderCancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
	sellOrder, err := tc.createLimitOrder(orderCtx, symbol, model.OrderSideSell)
	orderCancel()
	if err != nil {
		log.Printf("Cannot Place sell order.\nSymbol: %s\nError:%v\n", symbol, err)
		return false
	}

	sellTicker := time.NewTicker(tc.getSellOrderCheckDuration())
	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping executing sell order")
			return false
		case <-sellTicker.C:
			statusCtx, statusCancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
			currOrder, err := tc.order.GetById(statusCtx, sellOrder.ID)
			statusCancel()

			if err != nil {
				log.Printf(
					"Order not found for symbol %s. OrderId: %s\n",
					symbol,
					currOrder.ID,
				)
				return false
			}

			if currOrder.Status == model.OrderFilled {
				log.Printf("Sell order filled for %s\n", symbol)
				return true
			} else if currOrder.Status == model.OrderPartiallyFilled {
				log.Printf("Remaining qty for %s is %v\n", symbol, currOrder.RemainingQty)
			}
			updateCtx, updateCancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
			currOrder, err = tc.updateLimitOrder(updateCtx, symbol, currOrder.ID, model.OrderSideSell)
			updateCancel()

			if err != nil {
				var oerr *order.Error
				if errors.As(err, &oerr) {
					switch oerr.Type {
					case order.ErrOrderUnchangedParams:
						continue
					case order.ErrOrderNotFound:
						log.Printf("Execute sell order - %v\nCalling graceful sell for %s\n", err, symbol)
						err = tc.gracefullyFinishOrder(ctx, symbol, true)
						if err != nil {
							log.Printf("executeSellOrder - Graceful sell for %s could not start. Err:%v\n", symbol, err)
							return false
						}
						return true
					default:
						log.Printf("Execute sell order - Cannot Amend Order %s. Unhandled err: %v", sellOrder.ID, err)
						err = tc.gracefullyFinishOrder(ctx, symbol, true)
						if err != nil {
							log.Printf("executeSellOrder - Graceful sell for %s could not start. Err:%v\n", symbol, err)
						}
						return false
					}
				} else {
					if errors.Is(err, SellDetailsErrCannotCalculate) {
						log.Printf("executeSellOrder -  %v\n", err)
						continue
					} else {
						return false
					}
				}
			}
		}

	}
}

func (tc *Trader) checkBuyConditions(symbol string) BuyConditionResult {
	if len(tc.activeOrders) > 0 {
		log.Printf("trader - checkBuyConditions - Already has a position. Active orders: %v\n", tc.activeOrders)
		return BuyConditionCancel
	}
	_, exists := tc.activeOrders[symbol]
	if exists {
		log.Printf("trader - checkBuyConditions - Already has position for given symbol for %s\n", symbol)
		return BuyConditionCancel
	}

	rsiLive, rsiOk := tc.financial.GetRSILive(symbol)
	if !rsiOk {
		log.Printf("trader - checkBuyConditions - No RSI closed data available for %s\n", symbol)
		// QUESTION: Should I send construct signal to financial for missing coins? Isn't it coupling these 2 packages?
		return BuyConditionCancel
	}

	rsiClosed, rsiOk := tc.financial.GetRSIClosed(symbol)
	if !rsiOk {
		log.Printf("trader - checkBuyConditions - No RSI live data available for %s\n", symbol)
		// QUESTION: Should I send construct signal to financial for missing coins? Isn't it coupling these 2 packages?
		return BuyConditionCancel
	}

	volumeMedian, volumeOk := tc.financial.GetVolumeValueMedian(symbol)
	if !volumeOk {
		log.Printf("trader - checkBuyConditions - No volume data available for %s\n", symbol)
		return BuyConditionCancel
	}

	movingAverages, err := tc.financial.GetMovingAverages(symbol)
	if err != nil {
		log.Printf("trader - checkBuyConditions - Could not retrieve MA - Err: %s\n", err)
		return BuyConditionCancel
	}

	if movingAverages.MA50 < movingAverages.MA100 {
		log.Printf(
			"trader - checkBuyConditions - MA short (%.2f) is smaller than MA long (%.2f) for %s\n",
			movingAverages.MA50,
			movingAverages.MA100,
			symbol,
		)
		return BuyConditionCancel
	}

	if rsiClosed > tc.cfg.Trader.RSIBuyValue {
		log.Printf("trader - checkBuyConditions - RSIClose is bigger than threshold for %s\n", symbol)
		return BuyConditionCancel
	}

	if volumeMedian < tc.cfg.Trader.MedianVolumeThreshold {
		log.Printf("trader - checkBuyConditions - Median Volume below threshold for %s\n", symbol)
		return BuyConditionCancel
	}

	if rsiLive > tc.cfg.Trader.RSIBuyValue {
		log.Printf(
			"RSI live is bigger than threshold value of %v for %s. RSI Live:%.2f\n",
			tc.cfg.Trader.RSIBuyValue,
			symbol,
			rsiLive,
		)
		return BuyConditionWait
	}

	return BuyConditionMet
}

func (tc *Trader) CalculateSellOrderDetails(
	ctx context.Context,
	symbol string,
	orderType model.OrderType,
) (OrderDetails, error) {
	bestAskPrice, err := tc.getBestAskPrice(ctx, symbol)
	if err != nil {
		log.Println("Failed to get Best Ask Price for", symbol, " Err:", err)
		return OrderDetails{}, fmt.Errorf(
			"failed to get Best Ask Price for %s Err: %v",
			symbol,
			err,
		)
	}

	currentPrice, ok := tc.financial.GetCurrentPrice(symbol)
	if !ok {
		return OrderDetails{}, fmt.Errorf(
			"failed to get current price for %s",
			symbol,
		)
	}

	coinBalance, err := tc.exchange.GetSymbolBalance(ctx, symbol)
	if err != nil {
		return OrderDetails{}, fmt.Errorf(
			"failed to get balance for %s symbol",
			symbol,
		)
	}
	orderDetails := OrderDetails{
		Quantity: coinBalance.WalletBalance,
	}

	if orderType == model.OrderTypeLimit {
		var limitPrice float64
		if bestAskPrice <= currentPrice {
			limitPrice = bestAskPrice
		} else {
			averagePrice := (bestAskPrice + currentPrice) / 2
			limitPrice = numutils.RoundUpToPrecision(averagePrice, bestAskPrice)
		}
		orderDetails.Price = limitPrice
	}

	return orderDetails, nil

}

func (tc *Trader) CalculateBuyOrderDetails(
	ctx context.Context,
	symbol string,
	orderType model.OrderType,
) (OrderDetails, error) {

	currentPrice, ok := tc.financial.GetCurrentPrice(symbol)
	if !ok {
		return OrderDetails{}, fmt.Errorf(
			"cannot get current price for %s",
			symbol,
		)
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
	defer cancel()

	accountBalance, err := tc.exchange.GetAccountBalance(ctxWithTimeout)
	if err != nil {
		return OrderDetails{}, fmt.Errorf(
			"cannot get account balance for %s: %v",
			symbol,
			err,
		)
	}
	stopLimit := tc.cfg.Trader.StopLossRatio * currentPrice

	var limitPrice float64
	var orderAmount float64

	switch orderType {
	case model.OrderTypeLimit:
		bestBidPrice, err := tc.getBestBidPrice(ctx, symbol)
		if err != nil {
			return OrderDetails{}, err
		}

		calcLimitPrice := bestBidPrice
		if bestBidPrice < currentPrice {
			averagePrice := (bestBidPrice + currentPrice) / 2.0
			calcLimitPrice = numutils.RoundDownToPrecision(averagePrice, bestBidPrice)
		}
		limitPrice = calcLimitPrice
		orderAmount = (accountBalance.Total - tc.cfg.Trader.BalanceToLeave) / calcLimitPrice
		return OrderDetails{
			Price:    limitPrice,
			Quantity: orderAmount,
			StopLoss: stopLimit,
		}, nil
	case model.OrderTypeMarket:
		orderAmount = accountBalance.Total - tc.cfg.Trader.BalanceToLeave
		return OrderDetails{
			Quantity: orderAmount,
			StopLoss: stopLimit,
		}, nil
	}

	return OrderDetails{}, fmt.Errorf("incorrect order type")
}

func (tc *Trader) getSellOrderCheckDuration() time.Duration {
	return time.Duration(tc.cfg.Trader.SellCheckInterval) * time.Second
}

func (tc *Trader) getAPITimeoutDuration() time.Duration {
	return time.Duration(tc.cfg.Exchange.API.Timeout) * time.Second
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
) (exchange.OrderBookResult, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
	defer cancel()

	params := exchange.OrderBookRequestParams{
		Symbol: symbol,
		Limit:  limit,
	}
	orderBook, err := tc.exchange.GetOrderBook(ctxWithTimeout, params)

	if err != nil {
		return exchange.OrderBookResult{}, fmt.Errorf(
			"error getting order book for %s: %v",
			symbol,
			err,
		)
	}
	return orderBook, nil
}

func (tc *Trader) getBestBidPrice(ctx context.Context, symbol string) (float64, error) {

	orderBook, err := tc.getOrderBook(ctx, symbol, 1) // we only need top value that is the highest bid
	if err != nil {
		return 0, err
	}

	bestBidPrice := orderBook.Bid[0].Price
	return bestBidPrice, nil
}

func (tc *Trader) getBestAskPrice(ctx context.Context, symbol string) (float64, error) {
	orderBook, err := tc.getOrderBook(ctx, symbol, 1) // we only need top value that is the lowest ask
	if err != nil {
		return 0, err
	}

	bestAskPrice := orderBook.Ask[0].Price
	return bestAskPrice, nil
}

func (tc *Trader) gracefullyFinishOrder(ctx context.Context, symbol string, sellWithCheck bool) error {
	log.Printf("Gracefully Finish for %s called. Checking RSI set to %v\n", symbol, sellWithCheck)

	coinBalance, err := tc.exchange.GetSymbolBalance(ctx, symbol)
	if err != nil {
		return err
	}

	if coinBalance.UsdValue > 1 {
		tc.activeOrders[symbol] = 1
		if sellWithCheck {
			tc.sellReady <- symbol
		} else {
			tc.executeSellOrder(ctx, symbol)
		}
	} else {
		log.Printf("Balance for %s is less than 1 dollar\n", symbol)
	}
	return nil
}

func (tc *Trader) triggerSell(symbol string) {
	tc.activeOrders[symbol] = 1
	tc.sellReady <- symbol
}
