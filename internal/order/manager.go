package order

import (
	"context"
	"fmt"
	"time"

	"github.com/nihadazimli/crypbot/internal/model"

	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/exchange"
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
	order, err := m.Get(ctx, params.OrderID)

	if err != nil {
		return nil, &Error{
			OrderID: params.OrderID,
			Type:    ErrOrderNotFound,
			Err:     err,
		}
	}

	if order.Status == model.OrderFilled ||
		order.Status == model.OrderPartiallyFilledCancelled ||
		order.Status == model.OrderCanceled {
		return nil, &Error{
			OrderID: params.OrderID,
			Type:    ErrOrderNotModifiable,
			Err:     fmt.Errorf("order is in final state. Status: %s", order.Status),
		}
	}

	// Try to amend order
	amendParams := exchange.AmendOrderRequestParams{
		Symbol:   "",
		OrderId:  "",
		Price:    nil,
		Quantity: nil,
		StopLoss: nil,
	}
}

func (m *Manager) Cancel(ctx context.Context, orderID string) error {
	//TODO implement me
	panic("implement me")
}

func (m *Manager) Get(ctx context.Context, orderID string) (*Order, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Manager) createBuyOrder(ctx context.Context, params CreateParams) (*Order, error) {

	buyParams := exchange.BuyOrderRequestParams{
		Symbol:      params.Symbol,
		OrderAmount: params.Quantity,
		StopLimit:   *params.StopLoss,
		OrderType:   params.Type,
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
		Quantity:  order.Details.OrderAmount,
		StopLoss:  params.StopLoss,
		CreatedAt: time.Time{},
		UpdatedAt: time.Time{},
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
		CreatedAt: time.Time{},
		UpdatedAt: time.Time{},
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
