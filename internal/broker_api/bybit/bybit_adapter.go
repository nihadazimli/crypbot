package bybit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nihadazimli/crypbot/internal/broker_api"
	"github.com/nihadazimli/crypbot/internal/config"
	"github.com/nihadazimli/crypbot/pkg/model"
	"github.com/nihadazimli/crypbot/pkg/websocket"
	bybit "github.com/wuhewuhe/bybit.go.api"
	"log"
	"strconv"
	"strings"
)

type WalletResult struct {
	List []struct {
		TotalAvailableBalance string `json:"totalAvailableBalance"`
		TotalWalletBalance    string `json:"totalWalletBalance"`
	} `json:"list"`
}

type OrderBookEntry []string

type OrderBookResponse struct {
	Symbol    string           `json:"s"`
	Bids      []OrderBookEntry `json:"b"`
	Asks      []OrderBookEntry `json:"a"`
	Timestamp int64            `json:"ts"`
	UpdateID  int64            `json:"u"`
}

type Adapter struct {
	HTTPClient     *bybit.Client
	cfg            *config.Config
	orderStatus    map[string]broker_api.OrderStatus
	klineWriteChan chan model.WSInternalKline
	precisionPrice map[string]int
	precisionQty   map[string]int
}

func NewBybitAdapter() *Adapter {
	cfg := config.GetConfig()
	ba := &Adapter{
		HTTPClient:     bybit.NewBybitHttpClient(cfg.Broker.API.Key, cfg.Broker.API.Secret, bybit.WithBaseURL(bybit.MAINNET)),
		cfg:            cfg,
		precisionPrice: make(map[string]int),
		precisionQty:   make(map[string]int),
	}
	err := ba.startOrderStream(context.Background())
	if err != nil {
		log.Printf("Cannot subscribe to order stream. Err: %v\n", err)
		return nil
	}
	err = ba.setupPrecisionDetails(cfg.Symbols)

	return ba
}

func (ba *Adapter) GetAccountBalance(ctx context.Context) (broker_api.AccountBalanceResult, error) {

	requestParams := map[string]interface{}{
		"accountType": "UNIFIED",
	}
	walletResponse, err := ba.HTTPClient.NewUtaBybitServiceWithParams(requestParams).GetAccountWallet(ctx)
	if err != nil {
		return broker_api.AccountBalanceResult{}, err
	}
	resultData, err := json.Marshal(walletResponse.Result)
	if err != nil {
		return broker_api.AccountBalanceResult{}, err
	}

	var walletResult WalletResult
	err = json.Unmarshal(resultData, &walletResult)
	if err != nil {
		return broker_api.AccountBalanceResult{}, err
	}

	if len(walletResult.List) == 0 {
		return broker_api.AccountBalanceResult{}, errors.New("wallet result list is empty")
	}
	totalAvailableBalance, err := strconv.ParseFloat(walletResult.List[0].TotalWalletBalance, 64)

	return broker_api.AccountBalanceResult{Total: totalAvailableBalance}, nil
}

func (ba *Adapter) PlaceBuyOrder(ctx context.Context, params broker_api.BuyOrderRequestParams) (broker_api.BuyOrderResult, error) {
	requestParams := map[string]interface{}{
		"category":    "spot",
		"symbol":      params.Symbol,
		"side":        "Buy",
		"orderType":   "Limit",
		"qty":         strconv.FormatFloat(params.OrderAmount, 'f', ba.getQtyPrecision(params.Symbol), 64),
		"price":       strconv.FormatFloat(params.LimitPrice, 'f', ba.getPricePrecision(params.Symbol), 64),
		"stopLoss":    strconv.FormatFloat(params.StopLimit, 'f', ba.getPricePrecision(params.Symbol), 64),
		"slOrderType": "Market",
	}
	response, err := ba.HTTPClient.NewUtaBybitServiceWithParams(requestParams).PlaceOrder(ctx)

	if err != nil {
		return broker_api.BuyOrderResult{}, err
	}

	// Check if the response indicates success
	if response.RetCode != 0 {
		responseJSON, jsonErr := json.Marshal(response)
		if jsonErr != nil {
			fmt.Printf("error marshalling full response: %s\n", jsonErr)
		} else {
			fmt.Printf("Error: Place Buy Order\nFull response: %s", string(responseJSON))
		}
		return broker_api.BuyOrderResult{}, fmt.Errorf("API error. Code: %v Error: %s", response.RetCode, response.RetMsg)
	}

	// Handling the Result field which is interface{}
	resultData, err := json.Marshal(response.Result)
	if err != nil {
		return broker_api.BuyOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}

	var buyDetails broker_api.BuyOrderResultDetails
	err = json.Unmarshal(resultData, &buyDetails)
	if err != nil {
		return broker_api.BuyOrderResult{}, fmt.Errorf("error unmarshalling result to BuyOrderResultDetails: %s", err)
	}

	return broker_api.BuyOrderResult{
		Success: true,
		Details: broker_api.BuyOrderResultDetails{
			OrderId:     buyDetails.OrderId,
			OrderLinkId: buyDetails.OrderLinkId,
		},
	}, nil
}

func (ba *Adapter) GetOrderBook(ctx context.Context, params broker_api.OrderBookRequestParams) (broker_api.OrderBookResult, error) {
	requestParams := map[string]interface{}{
		"symbol":   params.Symbol,
		"limit":    params.Limit,
		"category": "spot",
	}
	orderBookService := ba.HTTPClient.NewUtaBybitServiceWithParams(requestParams)

	response, err := orderBookService.GetOrderBookInfo(ctx)
	if err != nil {
		return broker_api.OrderBookResult{}, fmt.Errorf("cannot fetch orderbook\n symbol:%s\nerr:%v", params.Symbol, err)
	}

	// Check if the response indicates success
	if response.RetCode != 0 {
		responseJSON, jsonErr := json.Marshal(response)
		if jsonErr != nil {
			fmt.Printf("error marshalling full response: %s\n", jsonErr)
		} else {
			fmt.Printf("Error: Get Order Book\nFull response: %s", string(responseJSON))
		}
		return broker_api.OrderBookResult{}, fmt.Errorf("API error. Code: %v Error: %s", response.RetCode, response.RetMsg)
	}

	resultData, err := json.Marshal(response.Result)
	if err != nil {
		return broker_api.OrderBookResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}

	var orderBookResponse OrderBookResponse
	err = json.Unmarshal(resultData, &orderBookResponse)
	if err != nil {
		return broker_api.OrderBookResult{}, fmt.Errorf("error unmarshalling result to OrderBookResponse: %s", err)
	}

	var all_asks []broker_api.OrderBookDetail
	var all_bids []broker_api.OrderBookDetail
	for _, ask := range orderBookResponse.Asks {
		price, _ := strconv.ParseFloat(ask[0], 64)
		qty, _ := strconv.ParseFloat(ask[1], 64)

		all_asks = append(all_asks, broker_api.OrderBookDetail{Price: price, Quantity: qty})
	}
	for _, bid := range orderBookResponse.Bids {
		price, _ := strconv.ParseFloat(bid[0], 64)
		qty, _ := strconv.ParseFloat(bid[1], 64)
		all_bids = append(all_bids, broker_api.OrderBookDetail{Price: price, Quantity: qty})
	}

	return broker_api.OrderBookResult{
		Ask: all_asks,
		Bid: all_bids,
	}, nil
}

func (ba *Adapter) PlaceSellOrder(ctx context.Context, params broker_api.SellOrderRequestParams) (broker_api.SellOrderResult, error) {
	requestParams := map[string]interface{}{
		"category":  "spot",
		"symbol":    params.Symbol,
		"side":      "Sell",
		"orderType": "Limit",
		"qty":       strconv.FormatFloat(params.OrderAmount, 'f', ba.getQtyPrecision(params.Symbol), 64),
		"price":     strconv.FormatFloat(params.LimitPrice, 'f', ba.getPricePrecision(params.Symbol), 64),
	}
	fmt.Println(requestParams)
	fmt.Println(ba.getQtyPrecision(params.Symbol), ba.getPricePrecision(params.Symbol))
	response, err := ba.HTTPClient.NewUtaBybitServiceWithParams(requestParams).PlaceOrder(ctx)

	if err != nil {
		return broker_api.SellOrderResult{}, err
	}

	// Check if the response indicates success
	if response.RetCode != 0 {
		responseJSON, jsonErr := json.Marshal(response)
		if jsonErr != nil {
			fmt.Printf("error marshalling full response: %s\n", jsonErr)
		} else {
			fmt.Printf("Error: Place Sell Order\nFull response: %s", string(responseJSON))
		}
		return broker_api.SellOrderResult{}, fmt.Errorf("API error. Code: %v Error: %s", response.RetCode, response.RetMsg)
	}

	// Handling the Result field which is interface{}
	resultData, err := json.Marshal(response.Result)
	if err != nil {
		return broker_api.SellOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}

	var sellDetails broker_api.SellOrderResultDetails
	err = json.Unmarshal(resultData, &sellDetails)
	if err != nil {
		return broker_api.SellOrderResult{}, fmt.Errorf("error unmarshalling result to BuyOrderResultDetails: %s", err)
	}

	return broker_api.SellOrderResult{
		Success: true,
		Details: broker_api.SellOrderResultDetails{
			OrderId:     sellDetails.OrderId,
			OrderLinkId: sellDetails.OrderLinkId,
		},
	}, nil
}

func (ba *Adapter) CancelOrder(ctx context.Context, params broker_api.CancelOrderRequestParams) (broker_api.CancelOrderResult, error) {
	requestParams := map[string]interface{}{
		"category": "spot",
		"symbol":   params.Symbol,
		"orderId":  strconv.Itoa(params.OrderId),
	}
	response, err := ba.HTTPClient.NewUtaBybitServiceWithParams(requestParams).CancelOrder(ctx)

	if err != nil {
		return broker_api.CancelOrderResult{}, err
	}

	// Check if the response indicates success
	if response.RetCode != 0 {
		responseJSON, jsonErr := json.Marshal(response)
		if jsonErr != nil {
			fmt.Printf("error marshalling full response: %s\n", jsonErr)
		} else {
			fmt.Printf("Error: Cancel Order\nFull response: %s", string(responseJSON))
		}
		return broker_api.CancelOrderResult{}, fmt.Errorf("API error. Code: %v Error: %s", response.RetCode, response.RetMsg)
	}

	resultData, err := json.Marshal(response.Result)

	if err != nil {
		return broker_api.CancelOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}
	var cancelOrderDetails broker_api.CancelOrderResult
	err = json.Unmarshal(resultData, &cancelOrderDetails)

	return broker_api.CancelOrderResult{OrderId: cancelOrderDetails.OrderId}, nil

}

func (ba *Adapter) AmendOrder(ctx context.Context, params broker_api.AmendOrderRequestParams) (broker_api.AmendOrderResult, error) {
	requestParams := make(map[string]interface{})
	requestParams["category"] = "spot"
	requestParams["symbol"] = params.Symbol
	requestParams["orderId"] = params.OrderId

	if params.Price != nil {
		requestParams["price"] = strconv.FormatFloat(*params.Price, 'f', ba.getPricePrecision(params.Symbol), 64)
	}

	if params.Quantity != nil {
		requestParams["qty"] = strconv.FormatFloat(*params.Quantity, 'f', ba.getQtyPrecision(params.Symbol), 64)
	}

	if params.StopLoss != nil {
		requestParams["stopLoss"] = strconv.FormatFloat(*params.StopLoss, 'f', ba.getPricePrecision(params.Symbol), 64)
	}

	response, err := ba.HTTPClient.NewUtaBybitServiceWithParams(requestParams).AmendOrder(ctx)

	if err != nil {
		return broker_api.AmendOrderResult{}, err
	}

	if response.RetCode != 0 {
		responseJSON, jsonErr := json.Marshal(response)
		if jsonErr != nil {
			fmt.Printf("error marshalling full response: %s\n", jsonErr)
		} else {
			fmt.Printf("Error: Amend Order\nFull response: %s", string(responseJSON))
		}
		return broker_api.AmendOrderResult{}, fmt.Errorf("API error. Code: %v Error: %s", response.RetCode, response.RetMsg)
	}

	resultData, err := json.Marshal(response.Result)
	if err != nil {
		return broker_api.AmendOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}

	var details broker_api.AmendOrderResult
	err = json.Unmarshal(resultData, &details)
	if err != nil {
		return broker_api.AmendOrderResult{}, fmt.Errorf("error unmarshalling result to AmendOrderResultDetails: %s", err)
	}
	return details, nil
}

func (ba *Adapter) GetOrderStatus(ctx context.Context, orderId string) (broker_api.OrderStatus, error) {
	//TODO implement me
	return broker_api.Pending, nil
}

func (ba *Adapter) StartKlineStream(ctx context.Context, symbols []string, klineWriteChan chan model.WSInternalKline) error {
	ws := websocket.NewWebSocketClientPublic(ctx, ba.cfg.Broker.WebSocket.AddressPublic, ba.cfg.Broker.WebSocket.PingInterval, ba.handleKlineMessages)
	ba.klineWriteChan = klineWriteChan

	err := ws.ConnectAndStart(symbols)
	if err != nil {
		return err
	}

	return nil
}

func (ba *Adapter) handleKlineMessages(message []byte) error {
	var WSKline model.WSRawKline
	err := json.Unmarshal(message, &WSKline)
	if err != nil {
		log.Println("Failed to unmarshal JSON:", err)
		return err
	}

	for _, kline := range WSKline.Data {
		internalKline := model.WSInternalKline{
			Symbol:  getSymbolFromKlineTopic(WSKline.Topic),
			Close:   kline.Close,
			Confirm: kline.Confirm,
			Volume:  kline.Volume,
		}
		ba.klineWriteChan <- internalKline

	}
	return nil
}

func getSymbolFromKlineTopic(topic string) string {
	return topic[strings.LastIndex(topic, ".")+1:]
}

func (ba *Adapter) startOrderStream(ctx context.Context) error {
	ws := websocket.NewWebSocketClientPrivate(ctx, ba.cfg.Broker.WebSocket.AddressPrivate,
		ba.cfg.Broker.API.Key, ba.cfg.Broker.API.Secret, ba.cfg.Broker.WebSocket.PingInterval, ba.handleOrderMessages)
	err := ws.ConnectAndStart([]string{"order", "position", "execution.fast"})
	if err != nil {
		return err
	}
	return nil
}

func (ba *Adapter) handleOrderMessages(message []byte) error {
	fmt.Printf("Received Private Stream Message: %s\n", string(message))
	return nil
}

func (ba *Adapter) setQtyPrecision(symbol string, precision int) {
	if precision > 1 {
		ba.precisionQty[symbol] = precision
	} else {
		ba.precisionQty[symbol] = 4
	}

}

func (ba *Adapter) setPricePrecision(symbol string, precision int) {
	ba.precisionPrice[symbol] = precision
}

func (ba *Adapter) getQtyPrecision(symbol string) int {
	return ba.precisionQty[symbol]
}

func (ba *Adapter) getPricePrecision(symbol string) int {
	return ba.precisionPrice[symbol]
}

func (ba *Adapter) setupPrecisionDetails(symbols []string) error {
	for _, symbol := range symbols {
		price, qty := ba.getPrecisionInfo(symbol)
		ba.setPricePrecision(symbol, price)
		ba.setQtyPrecision(symbol, qty)

	}
	return nil
}

func (ba *Adapter) getPrecisionInfo(symbol string) (int, int) {
	return 4, 3
}
