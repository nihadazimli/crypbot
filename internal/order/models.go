package order

import (
	"time"

	"github.com/nihadazimli/crypbot/internal/model"
)

type CreateParams struct {
	Symbol   string
	Side     model.OrderSide
	Type     model.OrderType
	Price    *float64
	Quantity float64
	StopLoss *float64
}

type UpdateParams struct {
	OrderID  string
	Symbol   string
	Price    *float64
	Quantity *float64
	StopLoss *float64
}

type Order struct {
	ID        string
	Symbol    string
	Side      model.OrderSide // Buy/Sell
	Type      model.OrderType // Market/Limit
	Status    model.OrderStatus
	Price     float64
	Quantity  float64
	StopLoss  *float64
	CreatedAt time.Time
	UpdatedAt time.Time
}
