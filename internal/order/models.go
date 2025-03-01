package order

import (
	"fmt"
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

type ErrType int

const (
	ErrOrderNotModifiable ErrType = iota
	ErrOrderNotFound
	ErrOrderUnchangedParams
	ErrUnexpected
	ErrInvalidParams
)

type Error struct {
	OrderID string
	Type    ErrType
	Err     error
}

func (e *Error) Error() string {
	return fmt.Sprintf("order error (%s): %v", e.OrderID, e.Err)
}
