package broker_api

import (
	"context"
)

type BrokerAPI interface {
	PlaceBuyOrder(ctx context.Context, params BuyOrderRequestParams) (BuyOrderResult, error)
	GetOrderBook(ctx context.Context, params OrderBookRequestParams) (OrderBookResponse, error)
	GetAccountBalance(ctx context.Context) (float64, error)
}

type BuyOrderResult struct {
	RetCode    int              `json:"retCode"`
	RetMsg     string           `json:"retMsg"`
	Result     BuyResultDetails `json:"result"`
	RetExtInfo struct{}         `json:"retExtInfo"`
	Time       int64            `json:"time"`
}

type BuyResultDetails struct {
	OrderId     string `json:"orderId"`
	OrderLinkId string `json:"orderLinkId"`
}

type BuyOrderRequestParams struct {
	Symbol      string
	LimitPrice  float64
	OrderAmount float64
	StopLimit   float64
}

type OrderBookRequestParams struct {
	Symbol string
	Limit  int
}

type OrderBookResponse struct {
	Ask []OrderBookDetail //Bid, buyer. Sort by price desc
	Bid []OrderBookDetail //Ask, seller. Sort by price asc
}

type OrderBookDetail struct {
	Price    float64
	Quantity float64
}
