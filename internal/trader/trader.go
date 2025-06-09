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
	"github.com/nihadazimli/crypbot/internal/exchange/bybit"
	"github.com/nihadazimli/crypbot/internal/model"
	"github.com/nihadazimli/crypbot/internal/order"
	"github.com/nihadazimli/crypbot/internal/strategy"
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
	financial FinancialService,
	strategyID model.StrategyID,
) (*Trader, error) {

	tradingClient, err := bybit.NewTradingClientForStrategy(strategyID)
	if err != nil {
		return nil, err
	}
	orderManager := order.NewOrderManager(tradingClient)

	commonTraderConfig, err := config.GetCommonTraderConfig(strategyID)
	if err != nil {
		return nil, err
	}
	return &Trader{
		cfg:       commonTraderConfig,
		exchange:  tradingClient,
		financial: financial,
		order:     orderManager,
	}, nil
}

func (tc *Trader) GetBalance(ctx context.Context) (*strategy.BalanceInfo, error) {
	accountBalance, err := tc.exchange.GetAccountBalance(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetBalance - %v", err)
	}

	availableForTrading := accountBalance.Total - tc.cfg.BalanceToLeave
	if availableForTrading < 0 {
		availableForTrading = 0
	}

	return &strategy.BalanceInfo{
		TotalBalance:     accountBalance.Total,
		AvailableBalance: availableForTrading,
	}, nil
}

func (tc *Trader) GetSymbolBalance(ctx context.Context, symbol string) (*strategy.PositionBalance, error) {
	symbolBalance, err := tc.exchange.GetSymbolBalance(ctx, symbol)
	if err != nil {
		return nil, fmt.Errorf("GetSymbolBalance - %v", err)
	}
	return &strategy.PositionBalance{
		Symbol:        symbol,
		TotalQuantity: symbolBalance.WalletBalance,
		UsdValue:      symbolBalance.UsdValue,
	}, nil

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
			return tc.handleContextCancellation(symbol, side, createdOrder.ID)
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
				Symbol: symbol,
				Status: model.OrderInvalid,
				Side:   side,
			}, false, fmt.Errorf(
				"monitor order - not found order id %s. Err: %v",
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
			Side:         side,
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
	condition := checker(symbol)

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
				return nil, false, fmt.Errorf("error does not match order error. Err: %v", err)
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
			"monitor order - cannot cancel conditions failed order. Err:%v",
			err,
		)
	}
	err = tc.gracefullyFinishOrder(ctx, symbol)
	if err != nil {
		return execResult, false, fmt.Errorf("monitor order - %v", err)
	}
	execResult.Status = model.OrderCanceled
	return execResult, false, err
}

func (tc *Trader) handleContextCancellation(
	symbol string,
	side model.OrderSide,
	orderID string,
) (*strategy.ExecutionResult, error) {
	// Get latest order status
	latestCtx, cancel := context.WithTimeout(context.Background(), tc.getAPITimeoutDuration())
	latestOrder, err := tc.order.GetById(latestCtx, orderID)
	cancel()

	execResult := &strategy.ExecutionResult{
		Symbol: symbol,
		Side:   side,
	}

	if err == nil {
		execResult.RequestedQty = latestOrder.Quantity
		execResult.FilledQty = latestOrder.FilledQty
		execResult.AvgPrice = latestOrder.AvgPrice
	}

	// Try to cancel
	cancelCtx, cancelCancel := context.WithTimeout(context.Background(), tc.getAPITimeoutDuration())
	cancelErr := tc.order.Cancel(cancelCtx, orderID)
	cancelCancel()

	if cancelErr != nil {
		execResult.Status = model.OrderInvalid
		return execResult, fmt.Errorf(
			"context cancelled and failed to cancel %s order for %s: %v",
			side,
			symbol,
			cancelErr,
		)
	}

	execResult.Status = model.OrderCanceled
	return execResult, fmt.Errorf("context cancelled while monitoring %s for %s", side, symbol) // ✅ Dynamic side
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
				"updateLimitOrder - %w. %v",
				BuyDetailsErrCannotCalculate,
				err,
			)
		}
		updateParams = order.UpdateParams{
			OrderID:  orderId,
			Symbol:   symbol,
			Price:    &updatedBuyDetails.Price,
			StopLoss: &updatedBuyDetails.StopLoss,
		}
	} else if side == model.OrderSideSell {
		updatedSellDetails, err := tc.CalculateSellOrderDetails(ctx, symbol, model.OrderTypeLimit)
		if err != nil {
			return nil, fmt.Errorf(
				"updateLimitOrder - %w. %v",
				SellDetailsErrCannotCalculate,
				err,
			)
		}
		updateParams = order.UpdateParams{
			OrderID: orderId,
			Symbol:  symbol,
			Price:   &updatedSellDetails.Price,
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
	log.Printf("Gracefully Finish for %s called\n", symbol)

	coinBalance, err := tc.exchange.GetSymbolBalance(ctx, symbol)
	if err != nil {
		return err
	}

	if coinBalance.UsdValue > 1 {
		if _, err := tc.ExecuteOrder(ctx, symbol, model.OrderTypeLimit, model.OrderSideSell, func(symbol string) model.OrderConditionResult {
			return model.ConditionMet
		}); err == nil {
			log.Printf("Successfully executed sell order for %s\n", symbol)
		} else {
			log.Printf("Failed to execute sell order for %s: %v\n", symbol, err)
		}
	} else {
		log.Printf("Balance for %s is less than 1 dollar\n", symbol)
	}
	return nil
}
