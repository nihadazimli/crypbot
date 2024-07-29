package bybit

import (
	"context"
	"errors"
	"github.com/nihadazimli/crypbot/internal/market_api"
	bybit "github.com/wuhewuhe/bybit.go.api"
)

type BybitAdapter struct {
	Client *bybit.Client
}

func (ba *BybitAdapter) PlaceOrder(ctx context.Context, params map[string]interface{}) (market_api.OrderResult, error) {
	result, err := ba.Client.NewTradeService(params).PlaceOrder(ctx)
	if err != nil {
		return market_api.OrderResult{}, err
	}
	return market_api.OrderResult{
		RetCode:    result.RetCode,
		RetMsg:     result.RetMsg,
		Result:     result.Result,
		RetExtInfo: result.RetExtInfo,
		Time:       result.Time,
	}, nil
}

func (ba *BybitAdapter) GetOrderBook(ctx context.Context, params map[string]interface{}) (string, error) {
	return "", errors.New("not implemented")
}
