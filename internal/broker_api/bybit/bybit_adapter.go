package bybit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

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

	response, err := ba.Client.NewTradeService(requestParams).PlaceOrder(ctx)

	if err != nil {
		return broker_api.BuyOrderResult{}, err
	}

	// Check if the response indicates success
	if response.RetCode != 0 {
		return broker_api.BuyOrderResult{}, fmt.Errorf("API error: %s", response.RetMsg)
	}

	// Handling the Result field which is interface{}
	resultData, err := json.Marshal(response.Result)
	if err != nil {
		return broker_api.BuyOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}

	var buyDetails broker_api.BuyResultDetails
	err = json.Unmarshal(resultData, &buyDetails)
	if err != nil {
		return broker_api.BuyOrderResult{}, fmt.Errorf("error unmarshalling result to BuyResultDetails: %s", err)
	}

	return broker_api.BuyOrderResult{
		Success: true,
		Details: broker_api.BuyResultDetails{
			OrderId:     buyDetails.OrderId,
			OrderLinkId: buyDetails.OrderLinkId,
		},
	}, nil
}

func (ba *BybitAdapter) GetOrderBook(ctx context.Context, params broker_api.OrderBookRequestParams) (broker_api.OrderBookResponse, error) {
	return broker_api.OrderBookResponse{}, errors.New("not implemented")
}

func (ba *BybitAdapter) PlaceSellOrder(ctx context.Context, params broker_api.SellOrderRequestParams) (broker_api.SellResultDetails, error) {
	return broker_api.SellResultDetails{}, errors.New("not implemented")
}
