// Package order is managing order lifecycle. It adds abstraction between trader and exchange to make
package order

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/exchange"
	"github.com/nihadazimli/crypbot/internal/model"
)

type Manager struct {
	cfg      *config.Config
	exchange exchange.Exchange
}

func NewOrderManager(exchange exchange.Exchange) *Manager {
	cfg := config.GetConfig()

	return &Manager{
		cfg:      cfg,
		exchange: exchange,
	}
}

func (m *Manager) Create(ctx context.Context, params CreateParams) (*Order, error) {
	var order *Order
	var err error

	switch params.Side {
	case model.OrderSideBuy:
		order, err = m.createBuyOrder(ctx, params)
	case model.OrderSideSell:
		order, err = m.createSellOrder(ctx, params)
	default:
		return nil, fmt.Errorf("order - Create - unsupported order side: %s", params.Side)
	}

	if err != nil {
		return nil, fmt.Errorf("order - Create - failed to create order: %w", err)
	}

	return order, nil
}

func (m *Manager) Update(ctx context.Context, params UpdateParams) (*Order, error) {
	order, err := m.GetById(ctx, params.OrderID)

	if err != nil {
		return nil, &Error{
			OrderID: params.OrderID,
			Type:    ErrOrderNotFound,
			Err:     err,
		}
	}

	if order.Status.IsFinished() {
		return nil, &Error{
			OrderID: params.OrderID,
			Type:    ErrOrderAtFinalState,
			Err:     fmt.Errorf("order is in final state. Status: %s", order.Status),
		}
	}

	// Try to amend order
	amendParams := exchange.AmendOrderRequestParams{
		Symbol:   params.Symbol,
		OrderId:  params.OrderID,
		Price:    params.Price,
		Quantity: params.Quantity,
		StopLoss: params.StopLoss,
	}
	_, err = m.exchange.AmendOrder(ctx, amendParams)
	if err != nil {
		switch {
		case errors.Is(err, exchange.AmendErrInsufficientBalance):
			return order, &Error{
				OrderID: params.OrderID,
				Type:    ErrOrderInsufficientBalance,
				Err:     err,
			}
		case errors.Is(err, exchange.AmendErrUnchangedParams):
			return order, &Error{
				OrderID: params.OrderID,
				Type:    ErrOrderUnchangedParams,
				Err:     err,
			}
		case errors.Is(err, exchange.AmendErrQuantityTooLow):
			return order, &Error{
				OrderID: params.OrderID,
				Type:    ErrInvalidParams,
				Err:     fmt.Errorf("quantity is below minimum allowed: %w", err),
			}
		case errors.Is(err, exchange.AmendErrOrderDoesNotExist):
			updateOrder, statusErr := m.GetById(ctx, params.OrderID)
			if statusErr != nil {
				return nil, &Error{
					OrderID: params.OrderID,
					Type:    ErrOrderNotFound,
					Err:     err,
				}
			}
			return updateOrder, &Error{
				OrderID: params.OrderID,
				Type:    ErrOrderAtFinalState,
				Err:     fmt.Errorf("order status changed during update to %s state", updateOrder.Status),
			}
		default:
			return nil, &Error{
				OrderID: params.OrderID,
				Type:    ErrUnexpected,
				Err:     fmt.Errorf("failed to amend order: %w", err),
			}
		}
	}

	updatedOrder, err := m.GetById(ctx, params.OrderID)
	if err != nil {
		return nil, err
	}
	return updatedOrder, nil
}

func (m *Manager) Cancel(ctx context.Context, orderID string) error {
	order, err := m.GetById(ctx, orderID)
	if err != nil {
		return &Error{
			OrderID: orderID,
			Type:    ErrOrderNotFound,
			Err:     fmt.Errorf("cancel - order not found: %w", err),
		}
	}

	if order.Status.IsFinished() {
		return &Error{
			OrderID: orderID,
			Type:    ErrOrderNotCancelable,
			Err:     fmt.Errorf("cancel - order in final state (%s)", order.Status),
		}
	}

	cancelParams := exchange.CancelOrderRequestParams{
		Symbol:  order.Symbol,
		OrderId: orderID,
	}

	_, err = m.exchange.CancelOrder(ctx, cancelParams)
	if err != nil {
		return &Error{
			OrderID: orderID,
			Type:    ErrUnexpected,
			Err:     fmt.Errorf("cancel - failed to cancel order: %w", err),
		}
	}

	return nil
}
func (m *Manager) GetById(ctx context.Context, orderID string) (*Order, error) {
	params := exchange.OrderStatusRequestParams{
		OrderId: orderID,
	}

	order, err := m.exchange.GetOrderStatus(ctx, params)
	if err != nil {
		return nil, &Error{
			OrderID: orderID,
			Type:    ErrOrderNotFound,
			Err:     fmt.Errorf("get - failed to fetch order status: %w", err),
		}
	}

	return &Order{
		ID:           orderID,
		Symbol:       order.Symbol,
		Price:        order.Price,
		Side:         order.Side,
		Type:         order.Type,
		Status:       order.Status,
		Quantity:     order.RemainingQty,
		RemainingQty: order.RemainingQty,
		UpdatedAt:    order.UpdatedAt,
		CreatedAt:    order.CreatedAt,
	}, nil
}

func (m *Manager) createBuyOrder(ctx context.Context, params CreateParams) (*Order, error) {

	buyParams := exchange.BuyOrderRequestParams{
		Symbol:      params.Symbol,
		OrderAmount: params.Quantity,
		OrderType:   params.Type,
	}

	if params.StopLoss != nil {
		buyParams.StopLimit = *params.StopLoss
	}

	if params.Type == model.OrderTypeLimit {
		buyParams.LimitPrice = params.Price
	}

	order, err := m.exchange.PlaceBuyOrder(ctx, buyParams)
	if err != nil {
		return nil, fmt.Errorf("createBuyOrder - %w", err)
	}

	result := Order{
		ID:        order.Details.OrderId,
		Symbol:    params.Symbol,
		Side:      model.OrderSideBuy,
		Type:      params.Type,
		Status:    model.OrderOpen,
		Quantity:  params.Quantity,
		StopLoss:  params.StopLoss,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if params.Type == model.OrderTypeLimit {
		result.Price = *params.Price
		result.RemainingQty = params.Quantity
	}

	return &result, nil
}

func (m *Manager) createSellOrder(ctx context.Context, params CreateParams) (*Order, error) {

	err := m.modifyTPSLOrders(ctx, params.Symbol)

	if err != nil {
		return nil, fmt.Errorf("createSellOrder - canceltpsl - %w", err)
	}

	sellOrderParams := exchange.SellOrderRequestParams{
		Symbol:      params.Symbol,
		OrderAmount: params.Quantity,
		OrderType:   params.Type,
		LimitPrice:  params.Price,
	}

	order, err := m.exchange.PlaceSellOrder(ctx, sellOrderParams)
	if err != nil {
		return nil, fmt.Errorf("createSellOrder - place order - %w", err)
	}

	orderResult := Order{
		ID:        order.Details.OrderId,
		Symbol:    params.Symbol,
		Side:      model.OrderSideSell,
		Type:      params.Type,
		Status:    model.OrderOpen,
		Quantity:  params.Quantity,
		StopLoss:  nil,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if params.Type == model.OrderTypeLimit {
		orderResult.Price = *params.Price
	}

	return &orderResult, nil

}

// modifyTPSLOrders for now cancels all tpsl orders. In the future, quantity for SL order will be updated to new logic
func (m *Manager) modifyTPSLOrders(ctx context.Context, symbol string) error {

	err := m.exchange.CancelAllTPSLOrders(ctx, symbol)
	if err != nil {
		return err
	}
	return nil
}
