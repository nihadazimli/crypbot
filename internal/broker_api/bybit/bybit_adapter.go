package bybit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nihadazimli/crypbot/internal/broker_api"
	"github.com/nihadazimli/crypbot/internal/config"
	bybit "github.com/wuhewuhe/bybit.go.api"
	"github.com/wuhewuhe/bybit.go.api/models"
	"strconv"
)

type BybitAdapter struct {
	HTTPClient *bybit.Client
	cfg        *config.Config
}

type WalletResult struct {
	TotalAvailableBalance float64 `json:"totalAvailableBalance"`
}

func NewBybitAdapter() *BybitAdapter {
	cfg := config.GetConfig()
	return &BybitAdapter{
		HTTPClient: bybit.NewBybitHttpClient(cfg.Broker.API.Key, cfg.Broker.API.Secret, bybit.WithBaseURL(bybit.TESTNET)),
		cfg:        cfg,
	}
}

func (ba *BybitAdapter) GetAccountBalance(ctx context.Context) (broker_api.AccountBalanceResult, error) {

	requestParams := map[string]interface{}{
		"accountType": "UNIFIED",
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
	orderBookService := ba.HTTPClient.NewOrderBookService()
	orderBookService.Symbol(params.Symbol)
	orderBookService.Limit(params.Limit)
	orderBookService.Category(models.Category(params.Category))

	orderBook, err := orderBookService.Do(ctx)

	if err != nil {
		return broker_api.OrderBookResponse{}, fmt.Errorf("cannot fetch orderbook\n symbol:%s\nerr:%v", params.Symbol, err)
	}

	fmt.Println(orderBook)

	var all_asks []broker_api.OrderBookDetail
	var all_bids []broker_api.OrderBookDetail

	for _, ask := range orderBook.Asks {
		price, _ := strconv.ParseFloat(ask[0], 64)
		qty, _ := strconv.ParseFloat(ask[1], 64)

		all_asks = append(all_asks, broker_api.OrderBookDetail{Price: price, Quantity: qty})
	}
	for _, bid := range orderBook.Bids {
		price, _ := strconv.ParseFloat(bid[0], 64)
		qty, _ := strconv.ParseFloat(bid[1], 64)
		all_bids = append(all_bids, broker_api.OrderBookDetail{Price: price, Quantity: qty})
	}

	return broker_api.OrderBookResponse{
		Ask: all_asks,
		Bid: all_bids,
	}, nil
}

func (ba *BybitAdapter) PlaceSellOrder(ctx context.Context, params broker_api.SellOrderRequestParams) (broker_api.SellOrderResult, error) {
	return broker_api.SellOrderResult{}, errors.New("not implemented")

}
