package exchange

import (
	"errors"
	"time"

	"github.com/nihadazimli/crypbot/internal/model"
)

type KlineHistoryParams struct {
	Symbol   string
	Interval int
	Limit    int
}

type SymbolBalanceResult struct {
	WalletBalance float64
	UsdValue      float64
}

type KlineHistoryResult struct {
	ClosePrices []float64
	Volumes     []float64
}

type BuyOrderResult struct {
	Success bool
	Details BuyOrderResultDetails
}

type AccountBalanceResult struct {
	Total float64
}

type KlineStreamRequestParams struct {
	Symbols        []string
	KlineWriteChan chan model.WSInternalKline
	Interval       int
}

type BuyOrderResultDetails struct {
	OrderId     string
	OrderLinkId string
}

type BuyOrderRequestParams struct {
	Symbol      string
	LimitPrice  *float64
	OrderAmount float64
	StopLimit   float64
	OrderType   model.OrderType
}

type SellOrderResult struct {
	Success bool
	Details SellOrderResultDetails
}

type SellOrderResultDetails struct {
	OrderId     string
	OrderLinkId string
}

type SellOrderRequestParams struct {
	Symbol      string
	LimitPrice  *float64
	OrderAmount float64
	OrderType   model.OrderType
}

type OrderBookRequestParams struct {
	Symbol string
	Limit  int
}

type OrderBookResult struct {
	Ask []OrderBookDetail //Bid, buyer. Sort by price desc
	Bid []OrderBookDetail //Ask, seller. Sort by price asc
}

type OrderBookDetail struct {
	Price    float64
	Quantity float64
}

type CancelOrderRequestParams struct {
	Symbol  string
	OrderId string
}

type CancelOrderResult struct {
	OrderId string
}

type AmendOrderRequestParams struct {
	Symbol   string
	OrderId  string
	Price    *float64
	Quantity *float64
	StopLoss *float64
}

type AmendOrderResult struct {
	orderLinkId string
	OrderId     string
}

type OrderStatusRequestParams struct {
	OrderId string
	Symbol  string
}

type OrderStatusResponse struct {
	OrderId      string
	Symbol       string
	Status       model.OrderStatus
	Type         model.OrderType
	Side         model.OrderSide
	Price        float64
	AvgPrice     float64
	Qty          float64
	RemainingQty float64
	FilledQty    float64
	UpdatedAt    time.Time
	CreatedAt    time.Time
}

var (
	AmendErrQuantityTooLow      = errors.New("quantity below minimum required")
	AmendErrUnchangedParams     = errors.New("unchanged parameters for amend")
	AmendErrOrderDoesNotExist   = errors.New("order does not exist")
	AmendErrInsufficientBalance = errors.New("insufficient balance")
)
