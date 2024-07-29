package broker_api

import (
	"context"
)

type BrokerAPI interface {
	PlaceOrder(ctx context.Context, params map[string]interface{}) (OrderResult, error)
	GetOrderBook(ctx context.Context, params map[string]interface{}) (string, error)
	GetAccountBalance(ctx context.Context) (float64, error)
}

type OrderResult struct {
	RetCode    int           `json:"retCode"`
	RetMsg     string        `json:"retMsg"`
	Result     ResultDetails `json:"result"`
	RetExtInfo struct{}      `json:"retExtInfo"`
	Time       int64         `json:"time"`
}

type ResultDetails struct {
	OrderId     string `json:"orderId"`
	OrderLinkId string `json:"orderLinkId"`
}
