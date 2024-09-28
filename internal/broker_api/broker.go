package broker_api

import (
	"context"
	"github.com/nihadazimli/crypbot/pkg/model"
)

type BrokerAPI interface {
	PlaceBuyOrder(ctx context.Context, params BuyOrderRequestParams) (BuyOrderResult, error)
	PlaceSellOrder(ctx context.Context, params SellOrderRequestParams) (SellOrderResult, error)
	GetOrderBook(ctx context.Context, params OrderBookRequestParams) (OrderBookResult, error)
	GetAccountBalance(ctx context.Context) (AccountBalanceResult, error)
	AmendOrder(ctx context.Context, params AmendOrderRequestParams) (AmendOrderResult, error)
	CancelOrder(ctx context.Context, params CancelOrderRequestParams) (CancelOrderResult, error)
	GetOrderStatus(ctx context.Context, orderId string) (OrderStatus, error)
	StartKlineStream(ctx context.Context, symbols []string, klineWriteChan chan model.WSInternalKline) error
}

type BuyOrderResult struct {
	Success bool
	Details BuyOrderResultDetails
}

type AccountBalanceResult struct {
	Total float64
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
	OrderId int
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
	Pending OrderStatus = iota
	Filled
	PartiallyFilled
	Canceled
	Failed
	NotFound
)
