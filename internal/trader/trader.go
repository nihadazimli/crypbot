// Package trader is handling all trade operations according to signals
package trader

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nihadazimli/crypbot/internal/strategy"

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
	cfg       *config.CommonTraderConfig
	exchange  exchange.TradingClient
	order     order.OrderManager
	financial FinancialService
}

func NewTrader(
	cfg *config.CommonTraderConfig,
	market exchange.TradingClient,
	financial FinancialService,
	order order.OrderManager,
) *Trader {
	return &Trader{
		cfg:       cfg,
		exchange:  market,
		financial: financial,
		order:     order,
	}
}

func (tc *Trader) ExecuteOrder(
	ctx context.Context,
	symbol string,
	orderType model.OrderType,
	side model.OrderSide,
	checker model.ConditionChecker,
) (*strategy.ExecutionResult, error) {
	switch orderType {
	case model.OrderTypeLimit:
		return tc.placeOrderAndMonitor(ctx, symbol, side, checker)
	case model.OrderTypeMarket:
		return nil, fmt.Errorf("market order not implemented for %s", symbol)
	default:
		return nil, fmt.Errorf("unsupported order type %s for %s", orderType, symbol)
	}
}

func (tc *Trader) placeOrderAndMonitor(
	ctx context.Context,
	symbol string,
	side model.OrderSide,
	checker model.ConditionChecker,
) (*strategy.ExecutionResult, error) {
	if side == model.OrderSideSell {
		err := tc.exchange.CancelAllTPSLOrders(ctx, symbol)
		if err != nil {
			log.Printf(
				"placeOrderAndMonitor -  cannot cancel TPSL order for %s Err:%v",
				symbol,
				err,
			)
		}
	}
	createCtx, createCancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
	createdOrder, err := tc.createLimitOrder(createCtx, symbol, side)
	createCancel()
	if err != nil {
		return nil, fmt.Errorf("placeOrderAndMonitor - %w", err)
	}

	log.Printf("%s order placed successfully for %s. Order: %+v", side, symbol, createdOrder)

	ticker := time.NewTicker(tc.getOrderCheckInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return tc.handleContextCancellation(symbol, createdOrder)
		case <-ticker.C:
			result, shouldContinue, err := tc.processOrderUpdate(ctx, symbol, side, createdOrder.ID, checker)
			if !shouldContinue {
				return result, err
			}
		}
	}
}

func (tc *Trader) processOrderUpdate(
	ctx context.Context,
	symbol string,
	side model.OrderSide,
	orderID string,
	checker model.ConditionChecker,
) (*strategy.ExecutionResult, bool, error) {
	currentOrder, err := tc.order.GetById(ctx, orderID)
	if err != nil {
		return &strategy.ExecutionResult{
				Symbol:       symbol,
				Status:       model.OrderInvalid,
				Side:         side,
				RequestedQty: 0,
				FilledQty:    0,
				AvgPrice:     0,
			}, false, fmt.Errorf(
				"monitorBuyOrder - not found order id %s. Err: %v",
				orderID,
				err,
			)
	}
	switch currentOrder.Status {
	case model.OrderFilled, model.OrderPartiallyFilledCancelled:
		return tc.handleOrderFilled(symbol, side, currentOrder)

	case model.OrderOpen, model.OrderPartiallyFilled:
		return tc.handleOrderPending(ctx, symbol, side, currentOrder, checker)

	case model.OrderInvalid, model.OrderCanceled:
		err = tc.gracefullyFinishOrder(ctx, symbol)
		return &strategy.ExecutionResult{
			Symbol:       symbol,
			Side:         side,
			Status:       currentOrder.Status,
			RequestedQty: currentOrder.Quantity,
			FilledQty:    currentOrder.FilledQty,
			AvgPrice:     currentOrder.AvgPrice,
		}, false, err
	default:
		log.Printf("Order Status: %v This status is not handled\n", currentOrder.Status)
		err = tc.gracefullyFinishOrder(ctx, symbol)
		return &strategy.ExecutionResult{
			Symbol:       symbol,
			Status:       currentOrder.Status,
			RequestedQty: currentOrder.Quantity,
			FilledQty:    currentOrder.FilledQty,
			AvgPrice:     currentOrder.AvgPrice,
		}, false, err

	}
}

func (tc *Trader) handleOrderFilled(
	symbol string,
	side model.OrderSide,
	currentOrder *order.Order,
) (*strategy.ExecutionResult, bool, error) {
	log.Printf("Order filled for %s\n", symbol)
	return &strategy.ExecutionResult{
		Symbol:       symbol,
		Side:         side,
		Status:       currentOrder.Status,
		RequestedQty: currentOrder.Quantity,
		FilledQty:    currentOrder.FilledQty,
		AvgPrice:     currentOrder.AvgPrice,
	}, false, nil
}

func (tc *Trader) handleOrderPending(
	ctx context.Context,
	symbol string,
	side model.OrderSide,
	currentOrder *order.Order,
	checker model.ConditionChecker,
) (*strategy.ExecutionResult, bool, error) {
	condition := checker(ctx, symbol)

	switch condition {
	case model.ConditionMet:
		return tc.handleConditionMet(ctx, symbol, side, currentOrder)
	case model.ConditionWait:
		return nil, true, nil
	case model.ConditionCancel:
		return tc.handleConditionCancel(ctx, symbol, side, currentOrder)
	default:
		return &strategy.ExecutionResult{
			Symbol: symbol,
			Side:   side,
			Status: model.OrderInvalid,
		}, false, fmt.Errorf("unknown condition result for %s %s", side, symbol)
	}
}

func (tc *Trader) handleConditionMet(
	ctx context.Context,
	symbol string,
	side model.OrderSide,
	currentOrder *order.Order,
) (*strategy.ExecutionResult, bool, error) {
	updateCtx, updateCancel := context.WithTimeout(ctx, tc.getAPITimeoutDuration())
	updatedOrder, err := tc.updateLimitOrder(updateCtx, symbol, currentOrder.ID, side)
	updateCancel()

	if err != nil {
		var oerr *order.Error
		if errors.As(err, &oerr) {
			switch oerr.Type {
			case order.ErrInvalidParams:
				execResult := &strategy.ExecutionResult{
					Symbol:       symbol,
					Side:         side,
					RequestedQty: currentOrder.Quantity,
					FilledQty:    currentOrder.FilledQty,
					AvgPrice:     currentOrder.AvgPrice,
				}
				err = tc.order.Cancel(ctx, updatedOrder.ID)
				if err != nil {
					execResult.Status = model.OrderInvalid
					return execResult, false, fmt.Errorf(
						"monitor order - cannot cancel low qty order. Err:%v",
						err,
					)
				}
				execResult.Status = model.OrderCanceled
				log.Printf(
					"Monitor order - order %s with status %s cancelled due to low remaining qty\n",
					updatedOrder.ID, updatedOrder.Status,
				)
				return execResult, false, err
			case order.ErrOrderUnchangedParams:
				return nil, true, nil
			case order.ErrOrderAtFinalState:
				log.Printf(
					"Monitor order - Probably Filled order %s does not exist anymore\n",
					oerr.OrderID,
				)
				return nil, true, nil
			case order.ErrOrderNotFound:
				execResult := &strategy.ExecutionResult{
					Status:       model.OrderInvalid,
					Side:         side,
					Symbol:       symbol,
					RequestedQty: currentOrder.Quantity,
					FilledQty:    currentOrder.FilledQty,
					AvgPrice:     currentOrder.AvgPrice,
				}
				log.Printf(
					"Monitor order - Order cannot be found. Weird! Order Id: %s,Error order id: %s err: %v\n",
					currentOrder.ID,
					oerr.OrderID,
					oerr.Err,
				)
				gracefulErr := tc.gracefullyFinishOrder(ctx, symbol)
				if gracefulErr != nil {
					return execResult, false, fmt.Errorf("monitor order - %v", err)
				}
				return execResult, false, err
			default:
				execResult := &strategy.ExecutionResult{
					Status:       model.OrderInvalid,
					Side:         side,
					Symbol:       symbol,
					RequestedQty: currentOrder.Quantity,
					FilledQty:    currentOrder.FilledQty,
					AvgPrice:     currentOrder.AvgPrice,
				}
				gracefulErr := tc.gracefullyFinishOrder(ctx, symbol)
				if gracefulErr != nil {
					return execResult, false, fmt.Errorf("monitor order - %v", err)
				}
				return execResult, false, fmt.Errorf(
					"monitor order - Cannot Amend Order id %v. Unhandled err: %v",
					updatedOrder.ID,
					err,
				)
			}
		} else {
			if errors.Is(err, BuyDetailsErrCannotCalculate) || errors.Is(err, SellDetailsErrCannotCalculate) {
				log.Printf("Order details can't be calculated. Err: %s\n", err)
				return nil, true, nil
			} else {
				return nil, false, fmt.Errorf("error does not match order error. Err: %e", err)
			}
		}

	}
	return nil, true, nil
}

func (tc *Trader) handleConditionCancel(
	ctx context.Context,
	symbol string,
	side model.OrderSide,
	currentOrder *order.Order,
) (*strategy.ExecutionResult, bool, error) {
	execResult := &strategy.ExecutionResult{
		Symbol:       symbol,
		Side:         side,
		RequestedQty: currentOrder.Quantity,
		FilledQty:    currentOrder.FilledQty,
		AvgPrice:     currentOrder.AvgPrice,
	}
	err := tc.order.Cancel(ctx, currentOrder.ID)
	if err != nil {
		execResult.Status = model.OrderInvalid
		return execResult, false, fmt.Errorf(
			"monitor buy order - cannot cancel conditions failed order. Err:%v",
			err,
		)
	}
	err = tc.gracefullyFinishOrder(ctx, symbol)
	if err != nil {
		return execResult, false, fmt.Errorf("monitor buy order - %v", err)
	}
	execResult.Status = model.OrderCanceled
	return execResult, false, err
}

func (tc *Trader) handleContextCancellation(
	symbol string,
	createdOrder *order.Order,
) (*strategy.ExecutionResult, error) {
	latestCtx, cancel := context.WithTimeout(context.Background(), tc.getAPITimeoutDuration())
	latestOrder, err := tc.order.GetById(latestCtx, createdOrder.ID)
	cancel()

	orderToCancel := createdOrder
	if err == nil {
		orderToCancel = latestOrder // Use latest data if available
	}

	execResult := &strategy.ExecutionResult{
		Symbol:       symbol,
		Side:         createdOrder.Side,
		RequestedQty: orderToCancel.Quantity,
		FilledQty:    orderToCancel.FilledQty,
		AvgPrice:     orderToCancel.AvgPrice,
	}

	cancelCtx, cancelCancel := context.WithTimeout(context.Background(), tc.getAPITimeoutDuration())
	cancelErr := tc.order.Cancel(cancelCtx, orderToCancel.ID)
	cancelCancel()

	if cancelErr != nil {
		execResult.Status = model.OrderInvalid
		return execResult, fmt.Errorf("context cancelled and failed to cancel order %s: %v", symbol, cancelErr)
	}
	execResult.Status = model.OrderCanceled
	return execResult, fmt.Errorf("context cancelled while monitoring buy for %s", symbol)
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
	stopLimit := tc.cfg.StopLossRatio * currentPrice

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
		orderAmount = (accountBalance.Total - tc.cfg.BalanceToLeave) / calcLimitPrice
		return OrderDetails{
			Price:    limitPrice,
			Quantity: orderAmount,
			StopLoss: stopLimit,
		}, nil
	case model.OrderTypeMarket:
		orderAmount = accountBalance.Total - tc.cfg.BalanceToLeave
		return OrderDetails{
			Quantity: orderAmount,
			StopLoss: stopLimit,
		}, nil
	}

	return OrderDetails{}, fmt.Errorf("incorrect order type")
}

func (tc *Trader) getAPITimeoutDuration() time.Duration {
	return tc.cfg.APITimeoutDuration
}

func (tc *Trader) getOrderCheckInterval() time.Duration {
	return tc.cfg.OrderCheckInterval
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
		if _, err := tc.ExecuteOrder(ctx, symbol, model.OrderTypeMarket, model.OrderSideSell, func(ctx context.Context, symbol string) model.OrderConditionResult {
			return model.ConditionMet
		}); err == nil {
			log.Printf("Successfully executed sell order for %s\n", symbol)
		} else {
			log.Printf("Failed to execute sell order for %s\n", symbol)
		}
	} else {
		log.Printf("Balance for %s is less than 1 dollar\n", symbol)
	}
	return nil
}
