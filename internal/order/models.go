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
	ID           string
	Symbol       string
	Side         model.OrderSide // Buy/Sell
	Type         model.OrderType // Market/Limit
	Status       model.OrderStatus
	Price        float64
	Quantity     float64
	FilledQty    float64
	RemainingQty float64
	StopLoss     *float64
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type ErrorType string

const (
	ErrOrderNotFound            ErrorType = "OrderNotFound"
	ErrOrderAtFinalState        ErrorType = "OrderAtFinalState"
	ErrOrderInsufficientBalance ErrorType = "OrderInsufficientBalance"
	ErrOrderNotCancelable       ErrorType = "OrderNotCancelable"
	ErrOrderUnchangedParams     ErrorType = "OrderUnchangedParams"
	ErrInvalidParams            ErrorType = "InvalidParams"
	ErrUnexpected               ErrorType = "UnexpectedError"
)

type Error struct {
	OrderID string
	Type    ErrorType
	Err     error
}

func (e *Error) Error() string {
	return fmt.Sprintf("Order Error [%s] %s: %v", e.OrderID, e.Type, e.Err)
}

func (e *Error) Unwrap() error {
	return e.Err
}
