package bybit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nihadazimli/crypbot/internal/config"

	"github.com/nihadazimli/crypbot/internal/broker_api"
	bybit "github.com/wuhewuhe/bybit.go.api"
)

type BybitAdapter struct {
	HTTPClient *bybit.Client
	cfg        *config.Config
}

type WalletResult struct {
	TotalAvailableBalance float64 `json:"totalAvailableBalance"`
}

func (ba *BybitAdapter) GetAccountBalance(ctx context.Context) (broker_api.AccountBalanceResult, error) {

	requestParams := map[string]interface{}{
		"accountType": "SPOT",
	}
	walletResponse, err := ba.HTTPClient.NewAccountService(requestParams).GetAccountWallet(ctx)
	if err != nil {
		return broker_api.AccountBalanceResult{}, err
	}

	var walletResult WalletResult
	resultData, err := json.Marshal(walletResponse)
	if err != nil {
		return broker_api.AccountBalanceResult{}, err
	}

	err = json.Unmarshal(resultData, &walletResult)
	if err != nil {
		return broker_api.AccountBalanceResult{}, err
	}

	return broker_api.AccountBalanceResult{Total: walletResult.TotalAvailableBalance}, nil
}

func NewBybitAdapter() *BybitAdapter {
	cfg := config.GetConfig()
	return &BybitAdapter{
		HTTPClient: bybit.NewBybitHttpClient(cfg.Broker.API.Key, cfg.Broker.API.Secret, bybit.WithBaseURL(bybit.TESTNET)),
		cfg:        cfg,
	}
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

	response, err := ba.HTTPClient.NewTradeService(requestParams).PlaceOrder(ctx)

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

func (ba *BybitAdapter) PlaceSellOrder(ctx context.Context, params broker_api.SellOrderRequestParams) (broker_api.SellOrderResult, error) {
	return broker_api.SellOrderResult{}, errors.New("not implemented")

}
