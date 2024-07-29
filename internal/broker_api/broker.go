package market_api

import (
	"context"
)

type MarketAPI interface {
	PlaceOrder(ctx context.Context, params map[string]interface{}) (OrderResult, error)
	GetOrderBook(ctx context.Context, params map[string]interface{}) (string, error)
	GetAccountBalance(ctx context.Context) (float64, error)
}

type OrderResult struct {
	RetCode    int
	RetMsg     string
	Result     interface{}
	RetExtInfo struct{}
	Time       int64
}
