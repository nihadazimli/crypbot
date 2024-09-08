package broker_api

import (
	"context"
)

type BrokerAPI interface {
	PlaceBuyOrder(ctx context.Context, params BuyOrderRequestParams) (BuyOrderResult, error)
	PlaceSellOrder(ctx context.Context, params SellOrderRequestParams) (SellOrderResult, error)
	GetOrderBook(ctx context.Context, params OrderBookRequestParams) (OrderBookResponse, error)
	GetAccountBalance(ctx context.Context) (AccountBalanceResult, error)
}

type BuyOrderResult struct {
	Success bool
	Details BuyResultDetails
}

type AccountBalanceResult struct {
	Total float64
}

type BuyResultDetails struct {
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
	Details SellResultDetails
}

type SellResultDetails struct {
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
	Category string
	Symbol   string
	Limit    int
}

type OrderBookResponse struct {
	Ask []OrderBookDetail //Bid, buyer. Sort by price desc
	Bid []OrderBookDetail //Ask, seller. Sort by price asc
}

type OrderBookDetail struct {
	Price    float64
	Quantity float64
}
