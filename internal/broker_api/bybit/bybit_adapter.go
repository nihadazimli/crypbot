package bybit

import (
	"context"
	"errors"
	"github.com/nihadazimli/crypbot/internal/broker_api"
	bybit "github.com/wuhewuhe/bybit.go.api"
)

type BybitAdapter struct {
	Client *bybit.Client
}

func (ba *BybitAdapter) GetAccountBalance(ctx context.Context) (float64, error) {
	//TODO implement me
	return 0.0, errors.New("not implemented")
}

func (ba *BybitAdapter) PlaceBuyOrder(ctx context.Context, params broker_api.BuyOrderRequestParams) (broker_api.BuyOrderResult, error) {

	requestParams := map[string]interface{}{
		"category":  "spot",
		"symbol":    params.Symbol,
		"side":      "Buy",
		"orderType": "Limit",
		"qty":       params.OrderAmount,
		"price":     params.LimitPrice,
		"stopLoss":  params.StopLimit,
	}

	result, err := ba.Client.NewTradeService(requestParams).PlaceOrder(ctx)
	if err != nil {
		return broker_api.BuyOrderResult{}, err
	}
	return broker_api.BuyOrderResult{
		RetCode: result.RetCode,
		RetMsg:  result.RetMsg,
		//Result:     result.Result,
		RetExtInfo: result.RetExtInfo,
		Time:       result.Time,
	}, nil
}

func (ba *BybitAdapter) GetOrderBook(ctx context.Context, params broker_api.OrderBookRequestParams) (broker_api.OrderBookResponse, error) {
	return broker_api.OrderBookResponse{}, errors.New("not implemented")
}
