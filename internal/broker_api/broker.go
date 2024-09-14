package broker_api

import (
	"context"
)

type BrokerAPI interface {
	PlaceBuyOrder(ctx context.Context, params BuyOrderRequestParams) (BuyOrderResult, error)
	PlaceSellOrder(ctx context.Context, params SellOrderRequestParams) (SellOrderResult, error)
	GetOrderBook(ctx context.Context, params OrderBookRequestParams) (OrderBookResult, error)
	GetAccountBalance(ctx context.Context) (AccountBalanceResult, error)
	CancelOrder(ctx context.Context, params CancelOrderRequestParams) (CancelOrderResult, error)
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
	OrderId int
}
