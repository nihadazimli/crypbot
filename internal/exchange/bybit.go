// Package exchange implements exchange interface for specific exchange called Bybit
package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/model"
	"github.com/nihadazimli/crypbot/pkg/numutils"
	"github.com/nihadazimli/crypbot/pkg/websocket"
	bybit "github.com/wuhewuhe/bybit.go.api"
)

type OrderInfoResult struct {
	List []struct {
		OrderId     string `json:"orderId"`
		Price       string `json:"price"`
		Qty         string `json:"qty"`
		LeavesQty   string `json:"leavesQty"`
		Symbol      string `json:"symbol"`
		OrderStatus string `json:"orderStatus"`
	}
}
type OrderSource int

const (
	OrderSourceRealtime OrderSource = iota
	OrderSourceHistory
)

type ActiveOrderTracker struct {
	Status       model.OrderStatus
	RemainingQty float64
	LastUpdated  time.Time
}

type OrderInfo struct {
	OrderId   string
	Price     string
	Qty       string
	LeavesQty string
	Symbol    string
	Status    model.OrderStatus
}

func (oi OrderInfo) isEmpty() bool {
	return oi.OrderId == "" && oi.Symbol == ""
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
			UsdValue      string `json:"usdValue"`
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

var orderStatusMap = map[string]model.OrderStatus{
	"Filled":                  model.OrderFilled,
	"Cancelled":               model.OrderCanceled,
	"Rejected":                model.OrderCanceled,
	"PartiallyFilled":         model.OrderPartiallyFilled,
	"Untriggered":             model.OrderOpen,
	"New":                     model.OrderOpen,
	"Triggered":               model.OrderOpen,
	"Deactivated":             model.OrderCanceled,
	"PartiallyFilledCanceled": model.OrderPartiallyFilledCancelled,
}

const (
	AmendOrderErrCodeQuantityTooLow      = 170136
	AmendOrderErrCodeUnchangedParameters = 10001
	AmendOrderErrOrderDoesNotExist       = 170213
	AmendOrderSuccess                    = 0
)

type Adapter struct {
	BybitClient           *bybit.Client
	cfg                   *config.Config
	orderWebsocket        *websocket.WebSocketClient
	activeOrders          map[string]ActiveOrderTracker
	activeOrdersToFollow  map[string]struct{} // These are the orders that are state followed through order websocket
	klineWriteChan        chan model.WSInternalKline
	precision             map[string]model.Precision
	precisionMutex        sync.RWMutex
	orderStreamSubscribed bool
}

func NewBybitAdapter() *Adapter {
	cfg := config.GetConfig()
	ba := &Adapter{
		BybitClient: bybit.NewBybitHttpClient(
			cfg.Exchange.API.Key,
			cfg.Exchange.API.Secret,
			bybit.WithBaseURL(cfg.Exchange.API.Host),
		),
		cfg:                  cfg,
		precision:            make(map[string]model.Precision),
		activeOrders:         make(map[string]ActiveOrderTracker),
		activeOrdersToFollow: make(map[string]struct{}),
	}
	ba.orderWebsocket = websocket.NewWebSocketClientPrivate(
		ba.cfg.Exchange.WebSocket.AddressPrivate,
		ba.cfg.Exchange.API.Key,
		ba.cfg.Exchange.API.Secret,
		ba.cfg.Exchange.WebSocket.PingInterval,
		ba.handlePrivateWSMessages,
	)
	ba.monitorActiveOrders(time.Duration(cfg.Exchange.WebSocket.MonitorInterval) * time.Second)
	return ba
}

func (ba *Adapter) GetAccountBalance(ctx context.Context) (AccountBalanceResult, error) {
	requestParams := map[string]interface{}{
		"accountType": "UNIFIED",
	}

	walletResult, err := ba.getWalletResult(ctx, requestParams)

	if err != nil {
		return AccountBalanceResult{}, err
	}

	totalAvailableBalance, err := strconv.ParseFloat(walletResult.List[0].TotalWalletBalance, 64)

	if err != nil {
		return AccountBalanceResult{}, err
	}

	return AccountBalanceResult{Total: totalAvailableBalance}, nil
}

func (ba *Adapter) getWalletResult(
	ctx context.Context,
	requestParams map[string]interface{},
) (WalletResult, error) {
	walletResponse, err := ba.BybitClient.NewUtaBybitServiceWithParams(requestParams).
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
	params BuyOrderRequestParams,
) (BuyOrderResult, error) {

	precision, err := ba.getPrecision(ctx, params.Symbol)
	if err != nil {
		return BuyOrderResult{}, fmt.Errorf(
			"PlaceBuyOrder - failed to get precision: %w",
			err,
		)
	}

	qty := numutils.RoundDownToPrecision(params.OrderAmount, precision.Qty)
	stopLossPrice := numutils.RoundDownToPrecision(params.StopLimit, precision.Price)

	requestParams := map[string]interface{}{
		"category":    "spot",
		"symbol":      params.Symbol,
		"side":        "Buy",
		"qty":         strconv.FormatFloat(qty, 'f', -1, 64),
		"stopLoss":    strconv.FormatFloat(stopLossPrice, 'f', -1, 64),
		"slOrderType": "Market",
	}

	switch params.OrderType {
	case model.OrderTypeLimit:
		if params.LimitPrice == nil {
			return BuyOrderResult{}, fmt.Errorf("limit price is required for limit orders")
		}
		limitPrice := numutils.RoundDownToPrecision(*params.LimitPrice, precision.Price)
		requestParams["orderType"] = "Limit"
		requestParams["price"] = strconv.FormatFloat(limitPrice, 'f', -1, 64)
	case model.OrderTypeMarket:
		requestParams["orderType"] = "Market"
	default:
		return BuyOrderResult{}, fmt.Errorf("unsupported order type: %s", params.OrderType)
	}

	response, err := ba.BybitClient.NewUtaBybitServiceWithParams(requestParams).PlaceOrder(ctx)

	if err != nil {
		return BuyOrderResult{}, err
	}

	// Check if the response indicates success
	if response.RetCode != 0 {
		responseJSON, jsonErr := json.Marshal(response)
		if jsonErr != nil {
			log.Printf("error marshalling full response: %s\n", jsonErr)
		} else {
			log.Printf("\nError: Place Buy Order\nFull response: %s\nParams: %s\n", string(responseJSON), requestParams)
		}
		return BuyOrderResult{}, fmt.Errorf(
			"API error. Code: %v Error: %s",
			response.RetCode,
			response.RetMsg,
		)
	}

	// Handling the Result field which is interface{}
	resultData, err := json.Marshal(response.Result)
	if err != nil {
		return BuyOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}

	var buyDetails BuyOrderResultDetails
	err = json.Unmarshal(resultData, &buyDetails)
	if err != nil {
		return BuyOrderResult{}, fmt.Errorf(
			"error unmarshalling result to BuyOrderResultDetails: %s",
			err,
		)
	}

	if params.OrderType == model.OrderTypeLimit {
		err := ba.followOrder(ctx, buyDetails.OrderId)

		if err != nil {
			log.Printf("PlaceBuyOrder - cannot connect to order websocket stream. Err:%v\n", err)
		}
	}

	return BuyOrderResult{
		Success: true,
		Details: BuyOrderResultDetails{
			OrderId:     buyDetails.OrderId,
			OrderLinkId: buyDetails.OrderLinkId,
		},
	}, nil
}

func (ba *Adapter) GetOrderBook(
	ctx context.Context,
	params OrderBookRequestParams,
) (OrderBookResult, error) {
	requestParams := map[string]interface{}{
		"symbol":   params.Symbol,
		"limit":    params.Limit,
		"category": "spot",
	}
	orderBookService := ba.BybitClient.NewUtaBybitServiceWithParams(requestParams)

	response, err := orderBookService.GetOrderBookInfo(ctx)
	if err != nil {
		return OrderBookResult{}, fmt.Errorf(
			"cannot fetch orderbook\n symbol:%s\nerr:%v",
			params.Symbol,
			err,
		)
	}

	// Check if the response indicates success
	if response.RetCode != 0 {
		responseJSON, jsonErr := json.Marshal(response)
		if jsonErr != nil {
			log.Printf("error marshalling full response: %s\n", jsonErr)
		} else {
			log.Printf("Error: Get Order Book\nFull response: %s\n", string(responseJSON))
		}
		return OrderBookResult{}, fmt.Errorf(
			"API error. Code: %v Error: %s",
			response.RetCode,
			response.RetMsg,
		)
	}

	resultData, err := json.Marshal(response.Result)
	if err != nil {
		return OrderBookResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}

	var orderBookResponse OrderBookResponse
	err = json.Unmarshal(resultData, &orderBookResponse)
	if err != nil {
		return OrderBookResult{}, fmt.Errorf(
			"error unmarshalling result to OrderBookResponse: %s",
			err,
		)
	}

	var all_asks []OrderBookDetail
	var all_bids []OrderBookDetail
	for _, ask := range orderBookResponse.Asks {
		price, _ := strconv.ParseFloat(ask[0], 64)
		qty, _ := strconv.ParseFloat(ask[1], 64)

		all_asks = append(all_asks, OrderBookDetail{Price: price, Quantity: qty})
	}
	for _, bid := range orderBookResponse.Bids {
		price, _ := strconv.ParseFloat(bid[0], 64)
		qty, _ := strconv.ParseFloat(bid[1], 64)
		all_bids = append(all_bids, OrderBookDetail{Price: price, Quantity: qty})
	}

	return OrderBookResult{
		Ask: all_asks,
		Bid: all_bids,
	}, nil
}

func (ba *Adapter) PlaceSellOrder(
	ctx context.Context,
	params SellOrderRequestParams,
) (SellOrderResult, error) {

	precision, err := ba.getPrecision(ctx, params.Symbol)
	if err != nil {
		return SellOrderResult{}, fmt.Errorf(
			"PlaceSellOrder - failed to get precision: %w",
			err,
		)
	}

	qty := numutils.RoundDownToPrecision(params.OrderAmount, precision.Qty)

	// Base request parameters
	requestParams := map[string]interface{}{
		"category": "spot",
		"symbol":   params.Symbol,
		"side":     "Sell",
		"qty":      strconv.FormatFloat(qty, 'f', -1, 64),
	}

	// Set order type specific parameters
	switch params.OrderType {
	case model.OrderTypeLimit:
		if params.LimitPrice == nil {
			return SellOrderResult{}, fmt.Errorf("limit price is required for limit orders")
		}
		limitPrice := numutils.RoundDownToPrecision(*params.LimitPrice, precision.Price)
		requestParams["orderType"] = "Limit"
		requestParams["price"] = strconv.FormatFloat(limitPrice, 'f', -1, 64)
	case model.OrderTypeMarket:
		requestParams["orderType"] = "Market"
	default:
		return SellOrderResult{}, fmt.Errorf("unsupported order type: %s", params.OrderType)
	}

	log.Printf("request params: %v\n", requestParams)
	response, err := ba.BybitClient.NewUtaBybitServiceWithParams(requestParams).PlaceOrder(ctx)

	if err != nil {
		return SellOrderResult{}, err
	}

	// Check if the response indicates success
	if response.RetCode != 0 {
		responseJSON, jsonErr := json.Marshal(response)
		if jsonErr != nil {
			log.Printf("error marshalling full response: %s\n", jsonErr)
		} else {
			log.Printf("Error: Place Sell Order\nFull response: %s\n", string(responseJSON))
		}
		return SellOrderResult{}, fmt.Errorf(
			"API error. Code: %v Error: %s",
			response.RetCode,
			response.RetMsg,
		)
	}

	// Handling the Result field which is interface{}
	resultData, err := json.Marshal(response.Result)
	if err != nil {
		return SellOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}

	var sellDetails SellOrderResultDetails
	err = json.Unmarshal(resultData, &sellDetails)
	if err != nil {
		return SellOrderResult{}, fmt.Errorf(
			"error unmarshalling result to SellOrderResultDetails: %s",
			err,
		)
	}
	if params.OrderType == model.OrderTypeLimit {
		err := ba.followOrder(ctx, sellDetails.OrderId)
		if err != nil {
			log.Printf("PlaceBuyOrder - cannot connect to order websocket stream. Err:%v\n", err)
		}
	}
	return SellOrderResult{
		Success: true,
		Details: SellOrderResultDetails{
			OrderId:     sellDetails.OrderId,
			OrderLinkId: sellDetails.OrderLinkId,
		},
	}, nil
}

func (ba *Adapter) CancelAllTPSLOrders(ctx context.Context, symbol string) error {
	requestParams := map[string]interface{}{
		"category":    "spot",
		"symbol":      symbol,
		"orderFilter": "tpslOrder",
	}
	response, err := ba.BybitClient.NewUtaBybitServiceWithParams(requestParams).CancelAllOrders(ctx)
	if err != nil {
		return fmt.Errorf("Cannot cancel order. Err: %s\n", err)
	}
	// Check if the response indicates success
	if response.RetCode != 0 {
		responseJSON, jsonErr := json.Marshal(response)
		if jsonErr != nil {
			return fmt.Errorf("error marshalling full response: %s\n", jsonErr)
		} else {
			return fmt.Errorf("Error: Place Sell Order\nFull response: %s", string(responseJSON))
		}
	}
	return nil
}

func (ba *Adapter) CancelOrder(
	ctx context.Context,
	params CancelOrderRequestParams,
) (CancelOrderResult, error) {
	requestParams := map[string]interface{}{
		"category": "spot",
		"symbol":   params.Symbol,
		"orderId":  params.OrderId,
	}
	response, err := ba.BybitClient.NewUtaBybitServiceWithParams(requestParams).CancelOrder(ctx)

	if err != nil {
		return CancelOrderResult{}, err
	}

	// Check if the response indicates success
	if response.RetCode != 0 {
		responseJSON, jsonErr := json.Marshal(response)
		if jsonErr != nil {
			log.Printf("error marshalling full response: %s\n", jsonErr)
		} else {
			log.Printf("Error: Cancel Order\nFull response: %s\n", string(responseJSON))
		}
		return CancelOrderResult{}, fmt.Errorf(
			"API error. Code: %v Error: %s",
			response.RetCode,
			response.RetMsg,
		)
	}

	resultData, err := json.Marshal(response.Result)

	if err != nil {
		return CancelOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}
	var cancelOrderDetails CancelOrderResult
	err = json.Unmarshal(resultData, &cancelOrderDetails)

	return CancelOrderResult{OrderId: cancelOrderDetails.OrderId}, nil

}

func (ba *Adapter) AmendOrder(
	ctx context.Context,
	params AmendOrderRequestParams,
) (AmendOrderResult, error) {
	requestParams := make(map[string]interface{})
	requestParams["category"] = "spot"
	requestParams["symbol"] = params.Symbol
	requestParams["orderId"] = params.OrderId

	precision, err := ba.getPrecision(ctx, params.Symbol)
	if err != nil {
		return AmendOrderResult{}, fmt.Errorf(
			"AmendOrder - failed to get precision: %w",
			err,
		)
	}

	if params.Price != nil {
		price := numutils.RoundDownToPrecision(*params.Price, precision.Price)
		requestParams["price"] = strconv.FormatFloat(price, 'f', -1, 64)
	}

	if params.Quantity != nil {
		qty := numutils.RoundDownToPrecision(*params.Quantity, precision.Qty)
		requestParams["qty"] = strconv.FormatFloat(qty, 'f', -1, 64)
	}

	if params.StopLoss != nil {
		stopLoss := numutils.RoundDownToPrecision(*params.StopLoss, precision.Price)
		requestParams["stopLoss"] = strconv.FormatFloat(stopLoss, 'f', -1, 64)
	}

	response, err := ba.BybitClient.NewUtaBybitServiceWithParams(requestParams).AmendOrder(ctx)

	if err != nil {
		return AmendOrderResult{}, err
	}

	if response.RetCode == AmendOrderErrCodeQuantityTooLow { // Use the constant here
		return AmendOrderResult{}, fmt.Errorf("order quantity too low. err: %w", AmendErrQuantityTooLow)
	} else if response.RetCode == AmendOrderErrCodeUnchangedParameters {
		return AmendOrderResult{}, fmt.Errorf("err: %w, params:%v", AmendErrUnchangedParams, params)
	} else if response.RetCode == AmendOrderErrOrderDoesNotExist {
		return AmendOrderResult{}, fmt.Errorf("error: %w, params: %v", AmendErrOrderDoesNotExist, params)
	} else if response.RetCode != AmendOrderSuccess {
		responseJSON, jsonErr := json.Marshal(response)
		if jsonErr != nil {
			log.Printf("error marshalling full response: %s\n", jsonErr)
		} else {
			log.Printf("\nError: Amend Order\nFull response: %s\nparams:%s\n", string(responseJSON), requestParams)
		}
		return AmendOrderResult{}, fmt.Errorf(
			"API error. Code: %v Error: %s",
			response.RetCode,
			response.RetMsg,
		)
	}
	log.Printf("Order amended successfully. Params: %v", requestParams)

	resultData, err := json.Marshal(response.Result)
	if err != nil {
		return AmendOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}

	var details AmendOrderResult
	err = json.Unmarshal(resultData, &details)
	if err != nil {
		return AmendOrderResult{}, fmt.Errorf(
			"error unmarshalling result to AmendOrderResultDetails: %s",
			err,
		)
	}
	return details, nil
}

func (ba *Adapter) GetKlineHistory(ctx context.Context, params KlineHistoryParams) (KlineHistoryResult, error) {
	startTime, endTime := determineTimeInterval(params.Limit, params.Interval)

	requestParams := map[string]string{
		"symbol":   params.Symbol,
		"category": ba.cfg.MarketConstants.KlineCategory,
		"interval": strconv.Itoa(params.Interval),
		"start":    strconv.FormatInt(startTime, 10),
		"end":      strconv.FormatInt(endTime, 10),
		"limit":    strconv.Itoa(params.Limit + 1),
	}

	requestUrl := buildUrl(ba.cfg.Exchange.API.Host, ba.cfg.Exchange.API.RequestPaths.Kline, requestParams)
	req, err := http.NewRequestWithContext(ctx, "GET", requestUrl, nil)
	if err != nil {
		return KlineHistoryResult{}, fmt.Errorf("error creating request for kline data: %w", err)
	}

	response, err := ba.BybitClient.HTTPClient.Do(req)
	if err != nil {
		return KlineHistoryResult{}, fmt.Errorf("error fetching kline data: %w", err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return KlineHistoryResult{}, fmt.Errorf("error reading response body: %w", err)
	}

	var apiResponse model.KlineApiResponse
	if err := json.Unmarshal(body, &apiResponse); err != nil {
		return KlineHistoryResult{}, fmt.Errorf("error unmarshaling JSON. Respomse: %v Err: %w",
			apiResponse, err)
	}

	if len(apiResponse.Result.List) < 1 {
		return KlineHistoryResult{}, fmt.Errorf("no kline data available for symbol: %s", params.Symbol)
	}

	// Extract the 5th value (close price) from each list
	closePrices := make([]float64, 0, len(apiResponse.Result.List))
	volumeValues := make([]float64, 0, len(apiResponse.Result.List))
	for _, item := range apiResponse.Result.List {
		closePrice, _ := strconv.ParseFloat(strings.TrimSpace(item[4]), 64)
		volumeAmount, _ := strconv.ParseFloat(strings.TrimSpace(item[5]), 64)

		closePrices = append(closePrices, closePrice)
		volumeValues = append(volumeValues, volumeAmount*closePrice)
	}
	reverse(closePrices)
	reverse(volumeValues)

	// Eliminate live value, just return most recent closed values
	return KlineHistoryResult{
		ClosePrices: closePrices[:len(closePrices)-1],
		Volumes:     volumeValues[:len(volumeValues)-1],
	}, nil
}

func (ba *Adapter) GetOrderStatus(ctx context.Context, params OrderRequestParams) (model.OrderStatus, error) {
	order, exists := ba.activeOrders[params.OrderId]

	if exists && time.Since(order.LastUpdated) <= ba.getOrderStatusCacheDuration() {
		return order.Status, nil
	}

	orderFromApi, err := ba.getOrderWithApi(ctx, params)
	if err != nil {
		return model.OrderInvalid, err
	}
	remainingQty, err := strconv.ParseFloat(orderFromApi.LeavesQty, 64)
	if err != nil {
		remainingQty = 0.0 // default to 0 if not parsable
	}

	ba.activeOrders[params.OrderId] = ActiveOrderTracker{
		Status:       orderFromApi.Status,
		RemainingQty: remainingQty,
		LastUpdated:  time.Now(),
	}

	return orderFromApi.Status, nil

}

func (ba *Adapter) GetSymbolBalance(ctx context.Context, symbol string) (SymbolBalanceResult, error) {
	baseCoin := removeUSDTSuffix(symbol)

	requestParams := map[string]interface{}{
		"accountType": "UNIFIED",
		"coin":        baseCoin,
	}

	walletResult, err := ba.getWalletResult(ctx, requestParams)
	if err != nil {
		return SymbolBalanceResult{}, err
	}

	if len(walletResult.List[0].Coin) == 0 {
		return SymbolBalanceResult{}, fmt.Errorf("cannot get balance for coin %s", symbol)
	}

	totalSymbolBalance, err := strconv.ParseFloat(walletResult.List[0].Coin[0].WalletBalance, 64)
	if err != nil {
		return SymbolBalanceResult{}, fmt.Errorf("error parsing total symbol wallet balance for %s", symbol)
	}
	usdValue, err := strconv.ParseFloat(walletResult.List[0].Coin[0].UsdValue, 64)
	if err != nil {
		return SymbolBalanceResult{}, fmt.Errorf("error parsing total symbol wallet balance for %s", symbol)
	}
	symbolBalanceResponse := SymbolBalanceResult{
		WalletBalance: totalSymbolBalance,
		UsdValue:      usdValue,
	}
	return symbolBalanceResponse, nil
}

func (ba *Adapter) StartKlineStream(
	ctx context.Context,
	params KlineStreamRequestParams,
) error {
	klineTopics := ba.createKlineTopics(params.Symbols, params.Interval)
	symbolChunks := chunkSymbols(klineTopics, ba.cfg.Exchange.WebSocket.MaxSymbolsPerConnection)
	var webSockets []*websocket.WebSocketClient

	for _, chunk := range symbolChunks {
		ws := websocket.NewWebSocketClientPublic(
			ba.cfg.Exchange.WebSocket.AddressPublic,
			ba.cfg.Exchange.WebSocket.PingInterval,
			ba.handleKlineMessages,
		)
		ba.klineWriteChan = params.KlineWriteChan

		err := ws.ConnectAndStart(ctx, chunk)
		if err != nil {
			for _, ws := range webSockets {
				ws.Shutdown()
			}
			return err
		}
		webSockets = append(webSockets, ws)
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
func (ba *Adapter) monitorActiveOrders(pollInterval time.Duration) {
	go func() {
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()

		for range ticker.C {
			for orderId := range ba.activeOrdersToFollow {
				state, exists := ba.activeOrders[orderId]
				if !exists {
					continue // no update yet, wait for next tick
				}

				if state.Status == model.OrderFilled ||
					state.Status == model.OrderCanceled ||
					state.Status == model.OrderPartiallyFilledCancelled {
					delete(ba.activeOrdersToFollow, orderId)
					log.Printf("Order %s reached final state (%s), removed from watch list\n", orderId, state.Status)
				}
			}

			if len(ba.activeOrdersToFollow) == 0 && ba.orderStreamSubscribed {
				log.Println("No active orders left. Stopping order stream.")
				ba.unsubscribeFromOrderStream()
				ba.orderStreamSubscribed = false
			}
		}
	}()
}

func (ba *Adapter) followOrder(ctx context.Context, orderId string) error {
	if !ba.orderStreamSubscribed {
		err := ba.subscribeToOrderStream(ctx)
		if err != nil {
			return err
		}
		ba.orderStreamSubscribed = true
	}

	ba.activeOrdersToFollow[orderId] = struct{}{}
	return nil
}

func (ba *Adapter) unsubscribeFromOrderStream() error {
	ba.orderWebsocket.Shutdown()
	return nil
}

func (ba *Adapter) subscribeToOrderStream(ctx context.Context) error {
	err := ba.orderWebsocket.ConnectAndStart(ctx, []string{"order"})
	if err != nil {
		return err
	}
	return nil
}

func (ba *Adapter) handlePrivateWSMessages(message []byte) error {
	log.Printf("Received Private Stream Message: %s\n", string(message))
	var privateMessage model.BybitWSPrivateMessage
	err := json.Unmarshal(message, &privateMessage)
	if err != nil {
		return err
	}

	switch privateMessage.Topic {
	case "order":
		return ba.handleOrderMessage(privateMessage.Data)
	default:
		log.Printf(
			"unhandled topic: %s,Data:%s\n",
			privateMessage.Topic,
			string(privateMessage.Data),
		)
		return nil
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

func (ba *Adapter) handleOrderMessage(data json.RawMessage) error {
	var orders []model.BybitWSOrderData
	err := json.Unmarshal(data, &orders)
	if err != nil {
		return err
	}

	for _, order := range orders {
		var orderState ActiveOrderTracker

		orderStatus, ok := parseOrderStatus(order.OrderStatus)

		if !ok {
			return fmt.Errorf("bybit - Unhandled Order Status. Status: %s", order.OrderStatus)
		}

		if orderStatus == model.OrderPartiallyFilled {
			remainingQty, err := strconv.ParseFloat(order.LeavesQty, 64)
			if err != nil {
				remainingQty = 0.0
				log.Printf("bybit - error parsing remaining quantity. Err: %v\n", err)
			}
			orderState = ActiveOrderTracker{
				Status:       model.OrderPartiallyFilled,
				RemainingQty: remainingQty,
				LastUpdated:  time.Now(),
			}
		} else {
			orderState = ActiveOrderTracker{Status: orderStatus, LastUpdated: time.Now()}
		}

		ba.activeOrders[order.OrderID] = orderState
	}
	return nil
}

func (ba *Adapter) GetRemainingQtyForPartialOrder(
	ctx context.Context,
	params OrderRequestParams,
) (float64, error) {
	if order, exists := ba.activeOrders[params.OrderId]; exists {
		return order.RemainingQty, nil // If it exists locally, return it directly.
	}

	openOrderInfo, err := ba.getOrderWithApi(ctx, params)
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

	//add to existing local map before return
	ba.activeOrders[params.OrderId] = ActiveOrderTracker{
		Status:       openOrderInfo.Status,
		RemainingQty: remainingQty,
		LastUpdated:  time.Now(),
	}
	return remainingQty, nil
}

// getOrderWithApi fetches order details from /v5/order/realtime or /v5/order/history endpoint of Bybit
func (ba *Adapter) getOrderWithApi(
	ctx context.Context,
	params OrderRequestParams,
) (OrderInfo, error) {
	queryParams := map[string]interface{}{
		"category": "spot",
		"orderId":  params.OrderId,
		"symbol":   params.Symbol,
	}

	orderInfo, err := ba.fetchOrderFromSource(ctx, queryParams, params, OrderSourceRealtime)
	if err != nil {
		return OrderInfo{}, fmt.Errorf("failed to fetch realtime order: %w", err)
	}
	if !orderInfo.isEmpty() {
		return orderInfo, nil
	}

	orderInfo, err = ba.fetchOrderFromSource(ctx, queryParams, params, OrderSourceHistory)
	if err != nil {
		return OrderInfo{}, fmt.Errorf("failed to fetch historical order: %w", err)
	}

	if !orderInfo.isEmpty() {
		return orderInfo, nil
	}

	return OrderInfo{}, fmt.Errorf("order not found for params: %+v", params)
}

func (ba *Adapter) fetchOrderFromSource(
	ctx context.Context,
	queryParams map[string]interface{},
	params OrderRequestParams,
	source OrderSource,
) (OrderInfo, error) {
	var response *bybit.ServerResponse
	var err error

	switch source {
	case OrderSourceRealtime:
		response, err = ba.BybitClient.NewUtaBybitServiceWithParams(queryParams).GetOpenOrders(ctx)
	case OrderSourceHistory:
		response, err = ba.BybitClient.NewUtaBybitServiceWithParams(queryParams).GetOrderHistory(ctx)
	}

	if err != nil {
		return OrderInfo{}, err
	}

	return ba.parseOrderResponse(response, params)
}

func (ba *Adapter) parseOrderResponse(
	ordersResponse *bybit.ServerResponse,
	params OrderRequestParams,
) (OrderInfo, error) {

	resultData, err := json.Marshal(ordersResponse.Result)
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
			orderStatus, ok := parseOrderStatus(entry.OrderStatus)
			if !ok {
				return OrderInfo{}, fmt.Errorf(
					"bybit - get_open_order - Cannot parse unknown order status. Status: %s",
					entry.OrderStatus,
				)
			}

			return OrderInfo{
				OrderId:   entry.OrderId,
				Symbol:    entry.Symbol,
				Price:     entry.Price,
				Qty:       entry.Qty,
				LeavesQty: entry.LeavesQty,
				Status:    orderStatus,
			}, nil
		}
	}
	return OrderInfo{}, nil
}

func (ba *Adapter) fetchPrecisionFromAPI(
	ctx context.Context,
	symbol string,
) (float64, float64, error) {
	params := map[string]interface{}{
		"symbol":   symbol,
		"category": "spot",
	}

	response, err := ba.BybitClient.NewUtaBybitServiceWithParams(params).GetInstrumentInfo(ctx)
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

func (ba *Adapter) createKlineTopics(symbols []string, interval int) []string {
	var klineTopics []string
	for _, symbol := range symbols {
		klineTopics = append(klineTopics, constructKlineTopic(symbol, interval))
	}
	return klineTopics
}

func (ba *Adapter) getOrderStatusCacheDuration() time.Duration {
	return time.Duration(ba.cfg.Defaults.OrderStatusCacheDuration) * time.Second
}

func constructKlineTopic(symbol string, klineInterval int) string {
	return "kline." + strconv.Itoa(klineInterval) + "." + symbol
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

// parseOrderStatus converts api response to OrderStatus map
func parseOrderStatus(status string) (model.OrderStatus, bool) {
	enumStatus, ok := orderStatusMap[status]
	return enumStatus, ok
}

func chunkSymbols(symbols []string, chunkSize int) [][]string {
	var allChunks [][]string

	for i := 0; i < len(symbols); i += chunkSize {
		end := i + chunkSize
		if end > len(symbols) {
			end = len(symbols)
		}

		allChunks = append(allChunks, symbols[i:end])
	}

	return allChunks
}

// determineTimeInterval determines for a given interval how much time needed to get n number of latest Klines
// it returns startTime and endTime in milliseconds since the epoch.
func determineTimeInterval(n int, interval int) (int64, int64) {
	now := time.Now()
	pastDuration := time.Duration(n*interval) * time.Minute
	pastTime := now.Add(-pastDuration)

	return pastTime.UnixMilli(), now.UnixMilli()
}

func buildUrl(host string, path string, params map[string]string) string {
	baseUrl, err := url.Parse(host)

	if err != nil {
		log.Fatal(err)
	}
	baseUrl.Path, _ = url.JoinPath(baseUrl.Path, path)

	queryParams := url.Values{}
	for k, v := range params {
		queryParams.Add(k, v)
	}
	baseUrl.RawQuery = queryParams.Encode()

	return baseUrl.String()
}

// reverse given float64 numbers slice this function reverses every value in place
func reverse(numbers []float64) {
	for i, j := 0, len(numbers)-1; i < j; i, j = i+1, j-1 {
		numbers[i], numbers[j] = numbers[j], numbers[i]
	}
}
