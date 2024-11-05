// Package bybit implements broker_api interface for specific broker called Bybit
package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/broker_api"
	"github.com/nihadazimli/crypbot/internal/model"
	"github.com/nihadazimli/crypbot/pkg/numutils"
	"github.com/nihadazimli/crypbot/pkg/websocket"
	bybit "github.com/wuhewuhe/bybit.go.api"
	"log"
	"strconv"
	"strings"
	"sync"
)

type OrderInfoResult struct {
	List []struct {
		OrderId   string `json:"orderId"`
		Price     string `json:"price"`
		Qty       string `json:"qty"`
		LeavesQty string `json:"leavesQty"`
		Symbol    string `json:"symbol"`
	}
}
type OrderInfo struct {
	OrderId   string `json:"orderId"`
	Price     string `json:"price"`
	Qty       string `json:"qty"`
	LeavesQty string `json:"leavesQty"`
	Symbol    string `json:"symbol"`
}

type InstrumentResult struct {
	List []struct {
		Symbol        string `json:"symbol"`
		LotSizeFilter struct {
			BasePrecision string `json:"basePrecision"`
		} `json:"lotSizeFilter"`
		PriceFilter struct {
			TickSize string `json:"tickSize"`
		} `json:"priceFilter"`
	} `json:"list"`
}

type WalletResult struct {
	List []struct {
		TotalAvailableBalance string `json:"totalAvailableBalance"`
		TotalWalletBalance    string `json:"totalWalletBalance"`
		Coin                  []struct {
			WalletBalance string `json:"walletBalance"`
		} `json:"coin"`
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
	activeOrders   map[string]broker_api.ActiveOrderTracker
	klineWriteChan chan model.WSInternalKline
	precision      map[string]model.Precision
	precisionMutex sync.RWMutex
}

func NewBybitAdapter() *Adapter {
	cfg := config.GetConfig()
	ba := &Adapter{
		HTTPClient: bybit.NewBybitHttpClient(
			cfg.Broker.API.Key,
			cfg.Broker.API.Secret,
			bybit.WithBaseURL(bybit.MAINNET),
		),
		cfg:          cfg,
		precision:    make(map[string]model.Precision),
		activeOrders: make(map[string]broker_api.ActiveOrderTracker),
	}
	err := ba.startOrderStream(context.Background())
	if err != nil {
		log.Printf("Cannot subscribe to order stream. Err: %v\n", err)
		return nil
	}

	return ba
}

func (ba *Adapter) GetAccountBalance(ctx context.Context) (broker_api.AccountBalanceResult, error) {
	requestParams := map[string]interface{}{
		"accountType": "UNIFIED",
	}

	walletResult, err := ba.getWalletResult(ctx, requestParams)

	if err != nil {
		return broker_api.AccountBalanceResult{}, err
	}

	totalAvailableBalance, err := strconv.ParseFloat(walletResult.List[0].TotalWalletBalance, 64)

	return broker_api.AccountBalanceResult{Total: totalAvailableBalance}, nil
}

func (ba *Adapter) getWalletResult(
	ctx context.Context,
	requestParams map[string]interface{},
) (WalletResult, error) {
	walletResponse, err := ba.HTTPClient.NewUtaBybitServiceWithParams(requestParams).
		GetAccountWallet(ctx)
	if err != nil {
		return WalletResult{}, err
	}
	resultData, err := json.Marshal(walletResponse.Result)
	if err != nil {
		return WalletResult{}, err
	}

	var walletResult WalletResult
	err = json.Unmarshal(resultData, &walletResult)
	if err != nil {
		return WalletResult{}, err
	}
	return walletResult, nil
}

func (ba *Adapter) PlaceBuyOrder(
	ctx context.Context,
	params broker_api.BuyOrderRequestParams,
) (broker_api.BuyOrderResult, error) {
	precision, err := ba.getPrecision(ctx, params.Symbol)
	if err != nil {
		return broker_api.BuyOrderResult{}, fmt.Errorf(
			"PlaceBuyOrder - failed to get precision: %w",
			err,
		)
	}

	qty := numutils.RoundDownToPrecision(params.OrderAmount, precision.Qty)
	limitPrice := numutils.RoundDownToPrecision(params.LimitPrice, precision.Price)
	stopLossPrice := numutils.RoundDownToPrecision(params.StopLimit, precision.Price)

	requestParams := map[string]interface{}{
		"category":    "spot",
		"symbol":      params.Symbol,
		"side":        "Buy",
		"orderType":   "Limit",
		"qty":         strconv.FormatFloat(qty, 'f', -1, 64),
		"price":       strconv.FormatFloat(limitPrice, 'f', -1, 64),
		"stopLoss":    strconv.FormatFloat(stopLossPrice, 'f', -1, 64),
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
		return broker_api.BuyOrderResult{}, fmt.Errorf(
			"API error. Code: %v Error: %s",
			response.RetCode,
			response.RetMsg,
		)
	}

	// Handling the Result field which is interface{}
	resultData, err := json.Marshal(response.Result)
	if err != nil {
		return broker_api.BuyOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}

	var buyDetails broker_api.BuyOrderResultDetails
	err = json.Unmarshal(resultData, &buyDetails)
	if err != nil {
		return broker_api.BuyOrderResult{}, fmt.Errorf(
			"error unmarshalling result to BuyOrderResultDetails: %s",
			err,
		)
	}

	return broker_api.BuyOrderResult{
		Success: true,
		Details: broker_api.BuyOrderResultDetails{
			OrderId:     buyDetails.OrderId,
			OrderLinkId: buyDetails.OrderLinkId,
		},
	}, nil
}

func (ba *Adapter) GetOrderBook(
	ctx context.Context,
	params broker_api.OrderBookRequestParams,
) (broker_api.OrderBookResult, error) {
	requestParams := map[string]interface{}{
		"symbol":   params.Symbol,
		"limit":    params.Limit,
		"category": "spot",
	}
	orderBookService := ba.HTTPClient.NewUtaBybitServiceWithParams(requestParams)

	response, err := orderBookService.GetOrderBookInfo(ctx)
	if err != nil {
		return broker_api.OrderBookResult{}, fmt.Errorf(
			"cannot fetch orderbook\n symbol:%s\nerr:%v",
			params.Symbol,
			err,
		)
	}

	// Check if the response indicates success
	if response.RetCode != 0 {
		responseJSON, jsonErr := json.Marshal(response)
		if jsonErr != nil {
			fmt.Printf("error marshalling full response: %s\n", jsonErr)
		} else {
			fmt.Printf("Error: Get Order Book\nFull response: %s", string(responseJSON))
		}
		return broker_api.OrderBookResult{}, fmt.Errorf(
			"API error. Code: %v Error: %s",
			response.RetCode,
			response.RetMsg,
		)
	}

	resultData, err := json.Marshal(response.Result)
	if err != nil {
		return broker_api.OrderBookResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}

	var orderBookResponse OrderBookResponse
	err = json.Unmarshal(resultData, &orderBookResponse)
	if err != nil {
		return broker_api.OrderBookResult{}, fmt.Errorf(
			"error unmarshalling result to OrderBookResponse: %s",
			err,
		)
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

func (ba *Adapter) PlaceSellOrder(
	ctx context.Context,
	params broker_api.SellOrderRequestParams,
) (broker_api.SellOrderResult, error) {

	precision, err := ba.getPrecision(ctx, params.Symbol)
	if err != nil {
		return broker_api.SellOrderResult{}, fmt.Errorf(
			"PlaceSellOrder - failed to get precision: %w",
			err,
		)
	}

	qty := numutils.RoundDownToPrecision(params.OrderAmount, precision.Qty)
	limitPrice := numutils.RoundDownToPrecision(params.LimitPrice, precision.Price)

	fmt.Printf("params: %v\n", params)
	requestParams := map[string]interface{}{
		"category":  "spot",
		"symbol":    params.Symbol,
		"side":      "Sell",
		"orderType": "Limit",
		"qty":       strconv.FormatFloat(qty, 'f', -1, 64),
		"price":     strconv.FormatFloat(limitPrice, 'f', -1, 64),
	}

	fmt.Printf("request params: %v\n", requestParams)
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
		return broker_api.SellOrderResult{}, fmt.Errorf(
			"API error. Code: %v Error: %s",
			response.RetCode,
			response.RetMsg,
		)
	}

	// Handling the Result field which is interface{}
	resultData, err := json.Marshal(response.Result)
	if err != nil {
		return broker_api.SellOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}

	var sellDetails broker_api.SellOrderResultDetails
	err = json.Unmarshal(resultData, &sellDetails)
	if err != nil {
		return broker_api.SellOrderResult{}, fmt.Errorf(
			"error unmarshalling result to BuyOrderResultDetails: %s",
			err,
		)
	}

	return broker_api.SellOrderResult{
		Success: true,
		Details: broker_api.SellOrderResultDetails{
			OrderId:     sellDetails.OrderId,
			OrderLinkId: sellDetails.OrderLinkId,
		},
	}, nil
}

func (ba *Adapter) CancelOrder(
	ctx context.Context,
	params broker_api.CancelOrderRequestParams,
) (broker_api.CancelOrderResult, error) {
	requestParams := map[string]interface{}{
		"category": "spot",
		"symbol":   params.Symbol,
		"orderId":  params.OrderId,
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
		return broker_api.CancelOrderResult{}, fmt.Errorf(
			"API error. Code: %v Error: %s",
			response.RetCode,
			response.RetMsg,
		)
	}

	resultData, err := json.Marshal(response.Result)

	if err != nil {
		return broker_api.CancelOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}
	var cancelOrderDetails broker_api.CancelOrderResult
	err = json.Unmarshal(resultData, &cancelOrderDetails)

	return broker_api.CancelOrderResult{OrderId: cancelOrderDetails.OrderId}, nil

}

func (ba *Adapter) AmendOrder(
	ctx context.Context,
	params broker_api.AmendOrderRequestParams,
) (broker_api.AmendOrderResult, error) {
	requestParams := make(map[string]interface{})
	requestParams["category"] = "spot"
	requestParams["symbol"] = params.Symbol
	requestParams["orderId"] = params.OrderId

	precision, err := ba.getPrecision(ctx, params.Symbol)
	if err != nil {
		return broker_api.AmendOrderResult{}, fmt.Errorf(
			"AmendOrder - failed to get precision: %w",
			err,
		)
	}

	if params.Price != nil {
		price := numutils.RoundDownToPrecision(*params.Price, precision.Qty)
		requestParams["price"] = strconv.FormatFloat(price, 'f', -1, 64)
	}

	if params.Quantity != nil {
		qty := numutils.RoundDownToPrecision(*params.Quantity, precision.Qty)
		requestParams["qty"] = strconv.FormatFloat(qty, 'f', -1, 64)
	}

	if params.StopLoss != nil {
		stopLoss := numutils.RoundDownToPrecision(*params.StopLoss, precision.Qty)
		requestParams["stopLoss"] = strconv.FormatFloat(stopLoss, 'f', -1, 64)
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
		return broker_api.AmendOrderResult{}, fmt.Errorf(
			"API error. Code: %v Error: %s",
			response.RetCode,
			response.RetMsg,
		)
	}

	resultData, err := json.Marshal(response.Result)
	if err != nil {
		return broker_api.AmendOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}

	var details broker_api.AmendOrderResult
	err = json.Unmarshal(resultData, &details)
	if err != nil {
		return broker_api.AmendOrderResult{}, fmt.Errorf(
			"error unmarshalling result to AmendOrderResultDetails: %s",
			err,
		)
	}
	return details, nil
}

func (ba *Adapter) GetOrderStatus(ctx context.Context, orderId string) broker_api.OrderStatus {
	if order, exists := ba.activeOrders[orderId]; exists {
		return order.Status
	}
	return broker_api.NotFound
}

func (ba *Adapter) GetSymbolBalance(ctx context.Context, symbol string) (float64, error) {
	baseCoin := removeUSDTSuffix(symbol)

	requestParams := map[string]interface{}{
		"accountType": "UNIFIED",
		"coin":        baseCoin,
	}

	walletResult, err := ba.getWalletResult(ctx, requestParams)
	if err != nil {
		return 0.0, err
	}

	if len(walletResult.List[0].Coin) == 0 {
		return 0.0, fmt.Errorf("cannot get balance for coin %s", symbol)
	}

	totalSymbolBalance, err := strconv.ParseFloat(walletResult.List[0].Coin[0].WalletBalance, 64)
	if err != nil {
		return 0.0, fmt.Errorf("error parsing total symbol wallet balance for %s symbol", symbol)
	}

	return totalSymbolBalance, nil
}

func (ba *Adapter) StartKlineStream(
	ctx context.Context,
	params broker_api.KlineStreamRequestParams,
) error {
	ws := websocket.NewWebSocketClientPublic(
		ctx,
		ba.cfg.Broker.WebSocket.AddressPublic,
		ba.cfg.Broker.WebSocket.PingInterval,
		ba.handleKlineMessages,
	)
	ba.klineWriteChan = params.KlineWriteChan

	err := ws.ConnectAndStart(params.Symbols)
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
	ws := websocket.NewWebSocketClientPrivate(
		ctx,
		ba.cfg.Broker.WebSocket.AddressPrivate,
		ba.cfg.Broker.API.Key,
		ba.cfg.Broker.API.Secret,
		ba.cfg.Broker.WebSocket.PingInterval,
		ba.handlePrivateWSMessages,
	)
	err := ws.ConnectAndStart([]string{"order"})
	if err != nil {
		return err
	}
	return nil
}

func (ba *Adapter) handlePrivateWSMessages(message []byte) error {
	fmt.Printf("Received Private Stream Message: %s\n", string(message))
	var privateMessage model.BybitWSPrivateMessage
	err := json.Unmarshal(message, &privateMessage)
	if err != nil {
		return err
	}

	switch privateMessage.Topic {
	case "order":
		return ba.handleOrderData(privateMessage.Data)
	default:
		return fmt.Errorf(
			"unhandled topic: %s,Data:%s",
			privateMessage.Topic,
			string(privateMessage.Data),
		)
	}

}
func (ba *Adapter) GetPrecision(ctx context.Context, symbol string) (model.Precision, error) {
	return ba.getPrecision(ctx, symbol)
}

func (ba *Adapter) getPrecision(ctx context.Context, symbol string) (model.Precision, error) {
	ba.precisionMutex.RLock()
	p, exists := ba.precision[symbol]
	ba.precisionMutex.RUnlock()

	if exists {
		return p, nil
	}
	price, qty, err := ba.fetchPrecisionFromAPI(ctx, symbol)
	if err != nil {
		return model.Precision{}, err
	}

	ba.precisionMutex.Lock()
	ba.precision[symbol] = model.Precision{Price: price, Qty: qty}
	ba.precisionMutex.Unlock()

	return ba.precision[symbol], nil
}

func (ba *Adapter) handleOrderData(data json.RawMessage) error {
	var orders []model.BybitWSOrderData
	err := json.Unmarshal(data, &orders)
	if err != nil {
		return err
	}

	for _, order := range orders {
		var orderState broker_api.ActiveOrderTracker
		if order.OrderStatus == "Filled" {
			orderState = broker_api.ActiveOrderTracker{Status: broker_api.Filled}
		} else if order.OrderStatus == "New" {
			orderState = broker_api.ActiveOrderTracker{Status: broker_api.Pending}
		} else if order.OrderStatus == "PartiallyFilled" {
			remainingQty, err := strconv.ParseFloat(order.LeavesQty, 64)
			if err != nil {
				return err
			}
			orderState = broker_api.ActiveOrderTracker{Status: broker_api.PartiallyFilled, RemainingQty: remainingQty}
		}
		ba.activeOrders[order.OrderID] = orderState

	}
	return nil
}

func (ba *Adapter) GetRemainingQtyForPartialOrder(
	ctx context.Context,
	params broker_api.OpenOrderRequestParams,
) (float64, error) {
	if order, exists := ba.activeOrders[params.OrderId]; exists {
		return order.RemainingQty, nil // If it exists locally, return it directly.
	}

	openOrderInfo, err := ba.getOpenOrder(ctx, params)
	if err != nil {
		return 0.0, fmt.Errorf("failed to get open order for ID %s: %w", params.OrderId, err)
	}

	remainingQty, err := strconv.ParseFloat(openOrderInfo.LeavesQty, 64)
	if err != nil {
		return 0.0, fmt.Errorf(
			"error parsing remaining quantity for order ID %s: %v",
			params.OrderId,
			err,
		)
	}

	return remainingQty, nil
}

func (ba *Adapter) GetOpenOrder(
	ctx context.Context,
	params broker_api.OpenOrderRequestParams,
) (OrderInfo, error) {
	return ba.getOpenOrder(ctx, params)
}

func (ba *Adapter) getOpenOrder(
	ctx context.Context,
	params broker_api.OpenOrderRequestParams,
) (OrderInfo, error) {
	queryParams := map[string]interface{}{"category": "spot"}

	if params.OrderId != "" {
		queryParams["orderId"] = params.OrderId
	}
	if params.Symbol != "" {
		queryParams["symbol"] = params.Symbol
	}

	openOrdersResponse, err := ba.HTTPClient.NewUtaBybitServiceWithParams(queryParams).
		GetOpenOrders(ctx)
	if err != nil {
		return OrderInfo{}, err
	}
	resultData, err := json.Marshal(openOrdersResponse.Result)
	if err != nil {
		return OrderInfo{}, err
	}

	var orderInfoResult OrderInfoResult
	err = json.Unmarshal(resultData, &orderInfoResult)
	if err != nil {
		return OrderInfo{}, err
	}

	for _, entry := range orderInfoResult.List {
		if entry.OrderId == params.OrderId || entry.Symbol == params.Symbol {
			return OrderInfo{
				OrderId:   entry.OrderId,
				Symbol:    entry.Symbol,
				Price:     entry.Price,
				Qty:       entry.Qty,
				LeavesQty: entry.LeavesQty,
			}, nil
		}
	}
	return OrderInfo{}, fmt.Errorf(
		"order with following params %v not found in response for open orders",
		params,
	)
}

func (ba *Adapter) fetchPrecisionFromAPI(
	ctx context.Context,
	symbol string,
) (float64, float64, error) {
	params := map[string]interface{}{
		"symbol":   symbol,
		"category": "spot",
	}

	response, err := ba.HTTPClient.NewUtaBybitServiceWithParams(params).GetInstrumentInfo(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("API request failed: %w", err)
	}
	if response.RetCode != 0 {
		return 0, 0, fmt.Errorf(
			"API error. Code: %d, Message: %s",
			response.RetCode,
			response.RetMsg,
		)
	}
	// Unmarshal Result into InstrumentResult
	var instrumentResult InstrumentResult
	resultData, err := json.Marshal(response.Result)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to marshal result: %w", err)
	}

	err = json.Unmarshal(resultData, &instrumentResult)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to unmarshal result: %w", err)
	}

	for _, entry := range instrumentResult.List {
		if entry.Symbol == symbol {
			// Calculate precision for Qty
			qtyPrec, err := strconv.ParseFloat(
				strings.TrimSpace(entry.LotSizeFilter.BasePrecision),
				64,
			)
			if err != nil {
				return 0, 0, fmt.Errorf("invalid basePrecision format: %w", err)
			}

			// Calculate precision for Price
			pricePrec, err := strconv.ParseFloat(strings.TrimSpace(entry.PriceFilter.TickSize), 64)
			if err != nil {
				return 0, 0, fmt.Errorf("invalid tickSize format: %w", err)
			}

			return pricePrec, qtyPrec, nil
		}
	}

	return 0, 0, fmt.Errorf("symbol %s not found in response", symbol)
}

// removeUSDTSuffix removes the "USDT" suffix from the symbol if present.
// If the symbol doesn't end with "USDT", it returns the symbol unchanged.
func removeUSDTSuffix(symbol string) string {
	const suffix = "USDT"
	if strings.HasSuffix(symbol, suffix) {
		return strings.TrimSuffix(symbol, suffix)
	}
	return symbol
}
