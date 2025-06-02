// Package trader is handling all trade operations according to signals
package trader

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/exchange"
	"github.com/nihadazimli/crypbot/internal/model"
	"github.com/nihadazimli/crypbot/internal/order"
	"github.com/nihadazimli/crypbot/pkg/numutils"
)

type OrderDetails struct {
	Price    float64
	Quantity float64
	StopLoss float64
}

type FinancialService interface {
	GetRSIClosed(symbol string) (float64, bool)
	GetRSILive(symbol string) (float64, bool)
	GetVolumeValueMedian(symbol string) (float64, bool)
	GetMovingAverages(symbol string) (model.MovingAverage, error)
	GetCurrentPrice(symbol string) (float64, bool)
}

type Trader struct {
	cfg       *config.Config
	exchange  exchange.TradingClient
	order     order.OrderManager
	financial FinancialService
}

func NewTrader(
	market exchange.TradingClient,
	financial FinancialService,
	order order.OrderManager,
) *Trader {
	cfg := config.GetConfig()

	return &Trader{
		cfg:       cfg,
		exchange:  market,
		financial: financial,
		order:     order,
	}
}

func (tc *Trader) ExecuteBuy(
	ctx context.Context,
	symbol string,
	orderType model.OrderType,
	checker model.ConditionChecker,
) error {
	var err error
	if orderType == model.OrderTypeLimit {
		err = tc.placeBuyOrderAndMonitor(ctx, symbol, checker)
	} else if orderType == model.OrderTypeMarket {
		log.Printf("Market order is not implemented yet")
	}
	return err
}

func (tc *Trader) ExecuteSell(ctx context.Context, symbol string, orderType model.OrderType, checker model.ConditionChecker) error {
	var err error
	if orderType == model.OrderTypeLimit {
		success := tc.executeSellOrder(ctx, symbol, checker)
		if success {
			return nil
		} else {
			return fmt.Errorf("can't finish selling")
		}
	} else if orderType == model.OrderTypeMarket {
		log.Printf("Market order is not implemented yet")
	}
	return err
}

func (tc *Trader) placeBuyOrderAndMonitor(ctx context.Context, symbol string, checker model.ConditionChecker) error {
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
				return nil
			case model.OrderOpen, model.OrderPartiallyFilled:
				condition := checker(ctx, symbol)

				switch condition {
				case model.ConditionMet:
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
								return nil
							case order.ErrOrderUnchangedParams:
								continue
							case order.ErrOrderAtFinalState:
								log.Printf(
									"Monitor buy order - Probably Filled order %s does not exist anymore\n",
									oerr.OrderID,
								)
								continue
							case order.ErrOrderNotFound:
								log.Printf(
									"Monitor buy order - Order cannot be found. Weird! Order Id: %s,Error order id: %s err: %v\n",
									createdOrder.ID,
									oerr.OrderID,
									oerr.Err,
								)
								gracefulErr := tc.gracefullyFinishOrder(ctx, symbol)
								if gracefulErr != nil {
									return fmt.Errorf("monitor buy order - %v", err)
								}
								return err
							default:
								gracefulErr := tc.gracefullyFinishOrder(ctx, symbol)
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
					}

				case model.ConditionWait:
					continue
				case model.ConditionCancel:
					err = tc.order.Cancel(ctx, createdOrder.ID)
					if err != nil {
						return fmt.Errorf("monitor buy order - cannot cancel conditions failed order. Err:%v", err)
					}
					err = tc.gracefullyFinishOrder(ctx, symbol)
					if err != nil {
						return fmt.Errorf("monitor buy order - %v", err)
					}
					return nil
				}

			case model.OrderInvalid, model.OrderCanceled:
				err = tc.gracefullyFinishOrder(ctx, symbol)
				if err != nil {
					return fmt.Errorf("monitorBuyOrder - %v", err)
				}
				return nil
			default:
				log.Printf("Order Status: %v This status is not handled\n", createdOrder)
				err = tc.gracefullyFinishOrder(ctx, symbol)
				if err != nil {
					return fmt.Errorf("monitorBuyOrder - %v", err)
				}
				return nil
			}
		}
	}
}

func (tc *Trader) updateLimitOrder(
	ctx context.Context,
	symbol string,
	orderId string,
	side model.OrderSide,
) (*order.Order, error) {
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
			OrderID: orderId,
			Symbol:  symbol,
			Price:   &updatedBuyDetails.Price,
			//Quantity: &updatedBuyDetails.Quantity,
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
			OrderID: orderId,
			Symbol:  symbol,
			Price:   &updatedSellDetails.Price,
			//Quantity: &updatedSellDetails.Quantity,
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

func (tc *Trader) executeSellOrder(ctx context.Context, symbol string, checker model.ConditionChecker) bool {
	err := tc.exchange.CancelAllTPSLOrders(ctx, symbol)
	if err != nil {
		log.Printf(
			"Trader - executeSellOrder - Cannot cancel order for %s Err:%v",
			symbol,
			err,
		)
	}
	orderCtx, orderCancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
	sellOrder, err := tc.createLimitOrder(orderCtx, symbol, model.OrderSideSell)
	orderCancel()
	if err != nil {
		log.Printf("Trader - executeSellOrder - Can't place sell order for %s. error:%v\n", symbol, err)
		return false
	}

	sellTicker := time.NewTicker(tc.getSellOrderCheckDuration())
	for {
		select {
		case <-ctx.Done():
			log.Println("Trader - executeSellOrder - Stopping executing sell order")
			return false
		case <-sellTicker.C:
			statusCtx, statusCancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
			currOrder, err := tc.order.GetById(statusCtx, sellOrder.ID)
			statusCancel()

			if err != nil {
				log.Printf(
					"Trader - executeSellOrder - Order not found for symbol %s. OrderId: %s\n",
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

			condition := checker(ctx, symbol)

			if condition == model.ConditionWait {
				continue
			} else if condition == model.ConditionCancel {
				err = tc.order.Cancel(ctx, currOrder.ID)
				if err != nil {
					log.Printf("executeSellOrder - cannot cancel conditions failed order. Err:%v", err)
					return false
				}
				err = tc.gracefullyFinishOrder(ctx, symbol)
				if err != nil {
					log.Printf("executeSellOrder - %v", err)
					return false
				}
				return true
			}
			// Code continues to here only when model.ConditionMet is returned from checker

			updateCtx, updateCancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
			currOrder, err = tc.updateLimitOrder(updateCtx, symbol, currOrder.ID, model.OrderSideSell)
			updateCancel()

			if err != nil {
				var oerr *order.Error
				if errors.As(err, &oerr) {
					switch oerr.Type {
					case order.ErrOrderUnchangedParams:
						continue
					case order.ErrOrderAtFinalState:
						log.Printf(
							"Execute sell order - Probably Filled order %s does not exist anymore\n",
							oerr.OrderID,
						)
						continue
					case order.ErrOrderNotFound:
						log.Printf("Execute sell order - Calling graceful sell for %s. Err: %v\n", symbol, err)
						err = tc.gracefullyFinishOrder(ctx, symbol)
						if err != nil {
							log.Printf("executeSellOrder - Graceful sell for %s could not start. Err:%v\n", symbol, err)
							return false
						}
						return true
					default:
						log.Printf("Execute sell order - Cannot Amend Order %s. Unhandled err: %v", sellOrder.ID, err)
						err = tc.gracefullyFinishOrder(ctx, symbol)
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
	return tc.cfg.Trader.SellCheckInterval
}

func (tc *Trader) getAPITimeoutDuration() time.Duration {
	return tc.cfg.Exchange.API.Timeout
}

func (tc *Trader) getBuyCheckIntervalDuration() time.Duration {
	return tc.cfg.Trader.BuyCheckInterval
}

func (tc *Trader) getSellCheckIntervalDuration() time.Duration {
	return tc.cfg.Trader.SellCheckInterval
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

func (tc *Trader) gracefullyFinishOrder(ctx context.Context, symbol string) error {
	log.Printf("Gracefully Finish for %s called. Checking RSI set to %v\n", symbol)

	coinBalance, err := tc.exchange.GetSymbolBalance(ctx, symbol)
	if err != nil {
		return err
	}

	if coinBalance.UsdValue > 1 {
		if success := tc.executeSellOrder(ctx, symbol); success {
			log.Printf("Successfully executed sell order for %s\n", symbol)
		} else {
			log.Printf("Failed to execute sell order for %s\n", symbol)
		}
	} else {
		log.Printf("Balance for %s is less than 1 dollar\n", symbol)
	}
	return nil
}
