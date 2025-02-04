package order

import "time"

type CreateParams struct {
	Symbol   string
	Side     Side
	Type     Type
	Price    float64
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
	Side      Side // Buy/Sell
	Type      Type // Market/Limit
	Status    Status
	Price     float64
	Quantity  float64
	StopLoss  *float64
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Side string

const (
	SideBuy  Side = "Buy"
	SideSell Side = "Sell"
)

type Type string

const (
	TypeMarket Type = "Market"
	TypeLimit  Type = "Limit"
)

type Status string

const (
	StatusOpen            Status = "Open"
	StatusFilled          Status = "Filled"
	StatusCanceled        Status = "Canceled"
	StatusPartiallyFilled Status = "PartiallyFilled"
)
