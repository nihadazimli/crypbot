// Package broker_api is interface that implements interaction between broker and application
package broker_api

import (
	"context"
	"errors"

	"github.com/nihadazimli/crypbot/internal/model"
)

type BrokerAPI interface {
	PlaceBuyOrder(ctx context.Context, params BuyOrderRequestParams) (BuyOrderResult, error)
	PlaceSellOrder(ctx context.Context, params SellOrderRequestParams) (SellOrderResult, error)
	GetOrderBook(ctx context.Context, params OrderBookRequestParams) (OrderBookResult, error)
	GetAccountBalance(ctx context.Context) (AccountBalanceResult, error)
	AmendOrder(ctx context.Context, params AmendOrderRequestParams) (AmendOrderResult, error)
	CancelOrder(ctx context.Context, params CancelOrderRequestParams) (CancelOrderResult, error)
	CancelAllTPSLOrders(ctx context.Context, symbol string) error
	StartKlineStream(ctx context.Context, params KlineStreamRequestParams) error
	GetSymbolBalance(ctx context.Context, symbol string) (SymbolBalanceResult, error)
	GetOrderStatus(ctx context.Context, orderId string) (OrderStatus, error)
	GetRemainingQtyForPartialOrder(
		ctx context.Context,
		params OpenOrderRequestParams,
	) (float64, error)
	GetKlineHistory(ctx context.Context, params KlineHistoryParams) (KlineHistoryResult, error)
	StartOrderStream() error
	StopOrderStream() error
}

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
	OrderAmount float64
}

type BuyOrderRequestParams struct {
	Symbol      string
	LimitPrice  float64
	OrderAmount float64
	StopLimit   float64
}

type SellOrderResult struct {
	Success bool
	Details SellOrderResultDetails
}

type SellOrderResultDetails struct {
	OrderId     string
	OrderLinkId string
	OrderAmount float64
}

type SellOrderRequestParams struct {
	Symbol      string
	LimitPrice  float64
	OrderAmount float64
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

type OrderStatus int

const (
	Invalid OrderStatus = iota
	Filled
	PartiallyFilled
	Canceled
	Open
	PartiallyFilledCancelled
)

type ActiveOrderTracker struct {
	Status       OrderStatus
	RemainingQty float64
}

type OpenOrderRequestParams struct {
	OrderId string
	Symbol  string
}

var (
	AmendErrQuantityTooLow  = errors.New("quantity below minimum required")
	AmendErrUnchangedParams = errors.New("unchanged parameters for amend")
)
