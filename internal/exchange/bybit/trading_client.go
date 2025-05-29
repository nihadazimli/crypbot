// Package exchange implements exchange interface for specific exchange called Bybit
package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nihadazimli/crypbot/internal/exchange"

	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/model"
	"github.com/nihadazimli/crypbot/pkg/numutils"
	"github.com/nihadazimli/crypbot/pkg/websocket"
	bybit "github.com/wuhewuhe/bybit.go.api"
)

type OrderInfoResult struct {
	List []struct {
		OrderId     string  `json:"orderId"`
		Price       float64 `json:"price,string"`
		Qty         float64 `json:"qty,string"`
		LeavesQty   float64 `json:"leavesQty,string"`
		Symbol      string  `json:"symbol"`
		Status      string  `json:"orderStatus"`
		Side        string  `json:"side"`
		CreatedTime int     `json:"createdTime,string"`
		UpdatedTime int     `json:"updatedTime,string"`
		Type        string  `json:"orderType"`
	}
}
type OrderSource int

const (
	OrderSourceRealtime OrderSource = iota
	OrderSourceHistory
)

type ActiveOrderTracker struct {
	Status       model.OrderStatus
	Symbol       string
	Price        float64
	Type         model.OrderType
	RemainingQty float64
	Qty          float64
	UpdatedAt    time.Time
	CreatedAt    time.Time
	Side         model.OrderSide
	LastUpdated  time.Time // Last time order tracker received data
}

type OrderInfo struct {
	OrderId   string
	Price     float64
	Qty       float64
	LeavesQty float64
	Symbol    string
	Status    model.OrderStatus
	Side      model.OrderSide
	CreatedAt time.Time
	UpdatedAt time.Time
	Type      model.OrderType
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
	AmendOrderErrCodeInsufficientBalance = 170131
	AmendOrderErrCodeUnchangedParameters = 10001
	AmendOrderErrOrderDoesNotExist       = 170213
	AmendOrderSuccess                    = 0
)

type TradingClient struct {
	strategyID            model.StrategyID
	BybitClient           *bybit.Client
	cfg                   *config.Config
	orderWebsocket        *websocket.Client
	activeOrders          map[string]ActiveOrderTracker
	activeOrdersToFollow  map[string]struct{} // These are the orders that are state followed through order websocket
	klineWriteChan        chan model.WSInternalKline
	precision             map[string]model.Precision
	precisionMutex        sync.RWMutex
	orderStreamSubscribed bool
}

func (tc *TradingClient) GetAccountBalance(ctx context.Context) (exchange.AccountBalanceResult, error) {
	requestParams := map[string]interface{}{
		"accountType": "UNIFIED",
	}

	walletResult, err := tc.getWalletResult(ctx, requestParams)

	if err != nil {
		return exchange.AccountBalanceResult{}, err
	}

	totalAvailableBalance, err := strconv.ParseFloat(walletResult.List[0].TotalWalletBalance, 64)

	if err != nil {
		return exchange.AccountBalanceResult{}, err
	}

	return exchange.AccountBalanceResult{Total: totalAvailableBalance}, nil
}

func (tc *TradingClient) getWalletResult(
	ctx context.Context,
	requestParams map[string]interface{},
) (WalletResult, error) {
	walletResponse, err := tc.BybitClient.NewUtaBybitServiceWithParams(requestParams).
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

func (tc *TradingClient) PlaceBuyOrder(
	ctx context.Context,
	params exchange.BuyOrderRequestParams,
) (exchange.BuyOrderResult, error) {

	precision, err := tc.getPrecision(ctx, params.Symbol)
	if err != nil {
		return exchange.BuyOrderResult{}, fmt.Errorf(
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
			return exchange.BuyOrderResult{}, fmt.Errorf("limit price is required for limit orders")
		}
		limitPrice := numutils.RoundDownToPrecision(*params.LimitPrice, precision.Price)
		requestParams["orderType"] = "Limit"
		requestParams["price"] = strconv.FormatFloat(limitPrice, 'f', -1, 64)
	case model.OrderTypeMarket:
		requestParams["orderType"] = "Market"
	default:
		return exchange.BuyOrderResult{}, fmt.Errorf("unsupported order type: %s", params.OrderType)
	}

	response, err := tc.BybitClient.NewUtaBybitServiceWithParams(requestParams).PlaceOrder(ctx)

	if err != nil {
		return exchange.BuyOrderResult{}, err
	}

	// Check if the response indicates success
	if response.RetCode != 0 {
		responseJSON, jsonErr := json.Marshal(response)
		if jsonErr != nil {
			log.Printf("error marshalling full response: %s\n", jsonErr)
		} else {
			log.Printf("\nError: Place Buy Order\nFull response: %s\nParams: %s\n", string(responseJSON), requestParams)
		}
		return exchange.BuyOrderResult{}, fmt.Errorf(
			"API error. Code: %v Error: %s",
			response.RetCode,
			response.RetMsg,
		)
	}

	// Handling the Result field which is interface{}
	resultData, err := json.Marshal(response.Result)
	if err != nil {
		return exchange.BuyOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}

	var buyDetails exchange.BuyOrderResultDetails
	err = json.Unmarshal(resultData, &buyDetails)
	if err != nil {
		return exchange.BuyOrderResult{}, fmt.Errorf(
			"error unmarshalling result to BuyOrderResultDetails: %s",
			err,
		)
	}

	if params.OrderType == model.OrderTypeLimit {
		err := tc.followOrder(ctx, buyDetails.OrderId)

		if err != nil {
			log.Printf("PlaceBuyOrder - cannot connect to order websocket stream. Err:%v\n", err)
		}
	}

	return exchange.BuyOrderResult{
		Success: true,
		Details: exchange.BuyOrderResultDetails{
			OrderId:     buyDetails.OrderId,
			OrderLinkId: buyDetails.OrderLinkId,
		},
	}, nil
}

func (tc *TradingClient) GetOrderBook(
	ctx context.Context,
	params exchange.OrderBookRequestParams,
) (exchange.OrderBookResult, error) {
	requestParams := map[string]interface{}{
		"symbol":   params.Symbol,
		"limit":    params.Limit,
		"category": "spot",
	}
	orderBookService := tc.BybitClient.NewUtaBybitServiceWithParams(requestParams)

	response, err := orderBookService.GetOrderBookInfo(ctx)
	if err != nil {
		return exchange.OrderBookResult{}, fmt.Errorf(
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
		return exchange.OrderBookResult{}, fmt.Errorf(
			"API error. Code: %v Error: %s",
			response.RetCode,
			response.RetMsg,
		)
	}

	resultData, err := json.Marshal(response.Result)
	if err != nil {
		return exchange.OrderBookResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}

	var orderBookResponse OrderBookResponse
	err = json.Unmarshal(resultData, &orderBookResponse)
	if err != nil {
		return exchange.OrderBookResult{}, fmt.Errorf(
			"error unmarshalling result to OrderBookResponse: %s",
			err,
		)
	}

	var all_asks []exchange.OrderBookDetail
	var all_bids []exchange.OrderBookDetail
	for _, ask := range orderBookResponse.Asks {
		price, _ := strconv.ParseFloat(ask[0], 64)
		qty, _ := strconv.ParseFloat(ask[1], 64)

		all_asks = append(all_asks, exchange.OrderBookDetail{Price: price, Quantity: qty})
	}
	for _, bid := range orderBookResponse.Bids {
		price, _ := strconv.ParseFloat(bid[0], 64)
		qty, _ := strconv.ParseFloat(bid[1], 64)
		all_bids = append(all_bids, exchange.OrderBookDetail{Price: price, Quantity: qty})
	}

	return exchange.OrderBookResult{
		Ask: all_asks,
		Bid: all_bids,
	}, nil
}

func (tc *TradingClient) PlaceSellOrder(
	ctx context.Context,
	params exchange.SellOrderRequestParams,
) (exchange.SellOrderResult, error) {

	precision, err := tc.getPrecision(ctx, params.Symbol)
	if err != nil {
		return exchange.SellOrderResult{}, fmt.Errorf(
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
			return exchange.SellOrderResult{}, fmt.Errorf("limit price is required for limit orders")
		}
		limitPrice := numutils.RoundDownToPrecision(*params.LimitPrice, precision.Price)
		requestParams["orderType"] = "Limit"
		requestParams["price"] = strconv.FormatFloat(limitPrice, 'f', -1, 64)
	case model.OrderTypeMarket:
		requestParams["orderType"] = "Market"
	default:
		return exchange.SellOrderResult{}, fmt.Errorf("unsupported order type: %s", params.OrderType)
	}

	log.Printf("request params: %v\n", requestParams)
	response, err := tc.BybitClient.NewUtaBybitServiceWithParams(requestParams).PlaceOrder(ctx)

	if err != nil {
		return exchange.SellOrderResult{}, err
	}

	// Check if the response indicates success
	if response.RetCode != 0 {
		responseJSON, jsonErr := json.Marshal(response)
		if jsonErr != nil {
			log.Printf("error marshalling full response: %s\n", jsonErr)
		} else {
			log.Printf("Error: Place Sell Order\nFull response: %s\n", string(responseJSON))
		}
		return exchange.SellOrderResult{}, fmt.Errorf(
			"API error. Code: %v Error: %s",
			response.RetCode,
			response.RetMsg,
		)
	}

	// Handling the Result field which is interface{}
	resultData, err := json.Marshal(response.Result)
	if err != nil {
		return exchange.SellOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}

	var sellDetails exchange.SellOrderResultDetails
	err = json.Unmarshal(resultData, &sellDetails)
	if err != nil {
		return exchange.SellOrderResult{}, fmt.Errorf(
			"error unmarshalling result to SellOrderResultDetails: %s",
			err,
		)
	}
	if params.OrderType == model.OrderTypeLimit {
		err := tc.followOrder(ctx, sellDetails.OrderId)
		if err != nil {
			log.Printf("PlaceBuyOrder - cannot connect to order websocket stream. Err:%v\n", err)
		}
	}
	return exchange.SellOrderResult{
		Success: true,
		Details: exchange.SellOrderResultDetails{
			OrderId:     sellDetails.OrderId,
			OrderLinkId: sellDetails.OrderLinkId,
		},
	}, nil
}

func (tc *TradingClient) CancelAllTPSLOrders(ctx context.Context, symbol string) error {
	requestParams := map[string]interface{}{
		"category":    "spot",
		"symbol":      symbol,
		"orderFilter": "tpslOrder",
	}
	response, err := tc.BybitClient.NewUtaBybitServiceWithParams(requestParams).CancelAllOrders(ctx)
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

func (tc *TradingClient) CancelOrder(
	ctx context.Context,
	params exchange.CancelOrderRequestParams,
) (exchange.CancelOrderResult, error) {
	requestParams := map[string]interface{}{
		"category": "spot",
		"symbol":   params.Symbol,
		"orderId":  params.OrderId,
	}
	response, err := tc.BybitClient.NewUtaBybitServiceWithParams(requestParams).CancelOrder(ctx)

	if err != nil {
		return exchange.CancelOrderResult{}, err
	}

	// Check if the response indicates success
	if response.RetCode != 0 {
		responseJSON, jsonErr := json.Marshal(response)
		if jsonErr != nil {
			log.Printf("error marshalling full response: %s\n", jsonErr)
		} else {
			log.Printf("Error: Cancel Order\nFull response: %s\n", string(responseJSON))
		}
		return exchange.CancelOrderResult{}, fmt.Errorf(
			"API error. Code: %v Error: %s",
			response.RetCode,
			response.RetMsg,
		)
	}

	resultData, err := json.Marshal(response.Result)

	if err != nil {
		return exchange.CancelOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}
	var cancelOrderDetails exchange.CancelOrderResult
	err = json.Unmarshal(resultData, &cancelOrderDetails)

	return exchange.CancelOrderResult{OrderId: cancelOrderDetails.OrderId}, nil

}

func (tc *TradingClient) AmendOrder(
	ctx context.Context,
	params exchange.AmendOrderRequestParams,
) (exchange.AmendOrderResult, error) {
	requestParams := make(map[string]interface{})
	requestParams["category"] = "spot"
	requestParams["symbol"] = params.Symbol
	requestParams["orderId"] = params.OrderId

	precision, err := tc.getPrecision(ctx, params.Symbol)
	if err != nil {
		return exchange.AmendOrderResult{}, fmt.Errorf(
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

	response, err := tc.BybitClient.NewUtaBybitServiceWithParams(requestParams).AmendOrder(ctx)

	if err != nil {
		return exchange.AmendOrderResult{}, err
	}

	if response.RetCode == AmendOrderErrCodeQuantityTooLow { // Use the constant here
		return exchange.AmendOrderResult{}, fmt.Errorf(
			"order quantity too low. err: %w",
			exchange.AmendErrQuantityTooLow,
		)
	} else if response.RetCode == AmendOrderErrCodeUnchangedParameters {
		return exchange.AmendOrderResult{}, fmt.Errorf("err: %w, params:%v", exchange.AmendErrUnchangedParams, params)
	} else if response.RetCode == AmendOrderErrOrderDoesNotExist {
		return exchange.AmendOrderResult{}, fmt.Errorf("err: %w, params: %v", exchange.AmendErrOrderDoesNotExist, params)
	} else if response.RetCode == AmendOrderErrCodeInsufficientBalance {
		return exchange.AmendOrderResult{}, fmt.Errorf("err: %w, params: %v", exchange.AmendErrInsufficientBalance, params)
	} else if response.RetCode != AmendOrderSuccess {
		responseJSON, jsonErr := json.Marshal(response)
		if jsonErr != nil {
			log.Printf("error marshalling full response: %s\n", jsonErr)
		} else {
			log.Printf("\nError: Amend Order\nFull response: %s\nparams:%s\n", string(responseJSON), requestParams)
		}
		return exchange.AmendOrderResult{}, fmt.Errorf(
			"API error. Code: %v Error: %s",
			response.RetCode,
			response.RetMsg,
		)
	}
	log.Printf("Order amended successfully. Params: %v", requestParams)

	resultData, err := json.Marshal(response.Result)
	if err != nil {
		return exchange.AmendOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
	}

	var details exchange.AmendOrderResult
	err = json.Unmarshal(resultData, &details)
	if err != nil {
		return exchange.AmendOrderResult{}, fmt.Errorf(
			"error unmarshalling result to AmendOrderResultDetails: %s",
			err,
		)
	}
	delete(tc.activeOrders, params.OrderId)

	err = tc.followOrder(ctx, details.OrderId)
	if err != nil {
		log.Printf("PlaceBuyOrder - cannot connect to order websocket stream. Err:%v\n", err)
	}
	return details, nil
}

func (tc *TradingClient) GetOrderStatus(
	ctx context.Context,
	params exchange.OrderStatusRequestParams,
) (exchange.OrderStatusResponse, error) {
	order, exists := tc.activeOrders[params.OrderId]

	if exists && time.Since(order.LastUpdated) <= tc.getOrderStatusCacheDuration() {
		return exchange.OrderStatusResponse{
			Symbol:       order.Symbol,
			OrderId:      params.OrderId,
			Status:       order.Status,
			Side:         order.Side,
			Price:        order.Price,
			Type:         order.Type,
			RemainingQty: order.RemainingQty,
			Qty:          order.Qty,
			UpdatedAt:    order.UpdatedAt,
			CreatedAt:    order.CreatedAt,
		}, nil
	}

	orderFromApi, err := tc.getOrderWithApi(ctx, params)
	if err != nil {
		return exchange.OrderStatusResponse{Status: model.OrderInvalid}, err
	}

	tc.activeOrders[params.OrderId] = ActiveOrderTracker{
		Symbol:       orderFromApi.Symbol,
		Status:       orderFromApi.Status,
		RemainingQty: orderFromApi.LeavesQty,
		CreatedAt:    orderFromApi.CreatedAt,
		UpdatedAt:    orderFromApi.UpdatedAt,
		Price:        orderFromApi.Price,
		Type:         orderFromApi.Type,
		Side:         orderFromApi.Side,
		LastUpdated:  time.Now(),
	}

	return exchange.OrderStatusResponse{
		Symbol:       orderFromApi.Symbol,
		OrderId:      orderFromApi.OrderId,
		Price:        orderFromApi.Price,
		Type:         orderFromApi.Type,
		Status:       orderFromApi.Status,
		Side:         orderFromApi.Side,
		CreatedAt:    orderFromApi.CreatedAt,
		UpdatedAt:    orderFromApi.UpdatedAt,
		RemainingQty: orderFromApi.LeavesQty,
		Qty:          orderFromApi.Qty,
	}, nil

}

func (tc *TradingClient) GetSymbolBalance(ctx context.Context, symbol string) (exchange.SymbolBalanceResult, error) {
	baseCoin := removeUSDTSuffix(symbol)

	requestParams := map[string]interface{}{
		"accountType": "UNIFIED",
		"coin":        baseCoin,
	}

	walletResult, err := tc.getWalletResult(ctx, requestParams)
	if err != nil {
		return exchange.SymbolBalanceResult{}, err
	}

	if len(walletResult.List[0].Coin) == 0 {
		return exchange.SymbolBalanceResult{}, fmt.Errorf("cannot get balance for coin %s", symbol)
	}

	totalSymbolBalance, err := strconv.ParseFloat(walletResult.List[0].Coin[0].WalletBalance, 64)
	if err != nil {
		return exchange.SymbolBalanceResult{}, fmt.Errorf("error parsing total symbol wallet balance for %s", symbol)
	}
	usdValue, err := strconv.ParseFloat(walletResult.List[0].Coin[0].UsdValue, 64)
	if err != nil {
		return exchange.SymbolBalanceResult{}, fmt.Errorf("error parsing total symbol wallet balance for %s", symbol)
	}
	symbolBalanceResponse := exchange.SymbolBalanceResult{
		WalletBalance: totalSymbolBalance,
		UsdValue:      usdValue,
	}
	return symbolBalanceResponse, nil
}

func (tc *TradingClient) Shutdown() {
	log.Printf("TradingClient[%s]: Shutting down...", tc.strategyID)
	if tc.orderWebsocket != nil {
		tc.orderWebsocket.Shutdown()
		tc.orderWebsocket = nil
	}
}

func (tc *TradingClient) monitorActiveOrders(pollInterval time.Duration) {
	go func() {
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()

		for range ticker.C {
			for orderId := range tc.activeOrdersToFollow {
				state, exists := tc.activeOrders[orderId]
				if !exists {
					continue // no update yet, wait for next tick
				}

				if state.Status.IsFinished() {
					delete(tc.activeOrdersToFollow, orderId)
					log.Printf("Order %s reached final state (%s), removed from watch list\n", orderId, state.Status)
				}
			}

			if len(tc.activeOrdersToFollow) == 0 && tc.orderStreamSubscribed {
				log.Println("No active orders left. Stopping order stream.")
				err := tc.unsubscribeFromOrderStream()
				if err != nil {
					log.Printf("Cannot unsubscribe from order stream. Err: %v\n", err)
				}
				tc.orderStreamSubscribed = false
			}
		}
	}()
}

func (tc *TradingClient) followOrder(ctx context.Context, orderId string) error {
	if !tc.orderStreamSubscribed {
		err := tc.subscribeToOrderStream(ctx)
		if err != nil {
			return err
		}
		tc.orderStreamSubscribed = true
	}

	tc.activeOrdersToFollow[orderId] = struct{}{}
	return nil
}

func (tc *TradingClient) unsubscribeFromOrderStream() error {
	tc.orderWebsocket.Shutdown()
	return nil
}

func (tc *TradingClient) subscribeToOrderStream(ctx context.Context) error {
	err := tc.orderWebsocket.ConnectAndStart(ctx, []string{"order"})
	if err != nil {
		return err
	}
	return nil
}

func (tc *TradingClient) handlePrivateWSMessages(message []byte) error {
	log.Printf("Received Private Stream Message: %s\n", string(message))
	var privateMessage model.BybitWSPrivateMessage
	err := json.Unmarshal(message, &privateMessage)
	if err != nil {
		return err
	}

	switch privateMessage.Topic {
	case "order":
		return tc.handleOrderMessage(privateMessage.Data)
	default:
		log.Printf(
			"unhandled topic: %s,Data:%s\n",
			privateMessage.Topic,
			string(privateMessage.Data),
		)
		return nil
	}

}
func (tc *TradingClient) GetPrecision(ctx context.Context, symbol string) (model.Precision, error) {
	return tc.getPrecision(ctx, symbol)
}

func (tc *TradingClient) getPrecision(ctx context.Context, symbol string) (model.Precision, error) {
	tc.precisionMutex.RLock()
	p, exists := tc.precision[symbol]
	tc.precisionMutex.RUnlock()

	if exists {
		return p, nil
	}
	price, qty, err := tc.fetchPrecisionFromAPI(ctx, symbol)
	if err != nil {
		return model.Precision{}, err
	}

	tc.precisionMutex.Lock()
	tc.precision[symbol] = model.Precision{Price: price, Qty: qty}
	tc.precisionMutex.Unlock()

	return tc.precision[symbol], nil
}

func (tc *TradingClient) handleOrderMessage(data json.RawMessage) error {
	var orders []model.BybitWSOrderData
	err := json.Unmarshal(data, &orders)
	if err != nil {
		return err
	}

	for _, order := range orders {
		orderStatus, ok := parseOrderStatus(order.Status)

		orderState := ActiveOrderTracker{
			Status:       orderStatus,
			Symbol:       order.Symbol,
			Price:        order.Price,
			Side:         model.ParseOrderSide(order.Side),
			CreatedAt:    millisecondsToTime(order.CreatedTime),
			UpdatedAt:    millisecondsToTime(order.UpdatedTime),
			Type:         model.ParseOrderType(order.Type),
			Qty:          order.Qty,
			RemainingQty: order.LeavesQty,
			LastUpdated:  time.Now(),
		}

		if !ok {
			return fmt.Errorf("bybit - Unhandled Order Status. Status: %s", order.Status)
		}

		tc.activeOrders[order.OrderID] = orderState
	}
	return nil
}

// getOrderWithApi fetches order details from /v5/order/realtime or /v5/order/history endpoint of Bybit
func (tc *TradingClient) getOrderWithApi(
	ctx context.Context,
	params exchange.OrderStatusRequestParams,
) (OrderInfo, error) {
	queryParams := map[string]interface{}{
		"category": "spot",
		"orderId":  params.OrderId,
		"symbol":   params.Symbol,
	}

	orderInfo, err := tc.fetchOrderFromSource(ctx, queryParams, params, OrderSourceRealtime)
	if err != nil {
		return OrderInfo{}, fmt.Errorf("failed to fetch realtime order: %w", err)
	}
	if !orderInfo.isEmpty() {
		return orderInfo, nil
	}

	orderInfo, err = tc.fetchOrderFromSource(ctx, queryParams, params, OrderSourceHistory)
	if err != nil {
		return OrderInfo{}, fmt.Errorf("failed to fetch historical order: %w", err)
	}

	if !orderInfo.isEmpty() {
		return orderInfo, nil
	}

	return OrderInfo{}, fmt.Errorf("order not found for params: %+v", params)
}

func (tc *TradingClient) fetchOrderFromSource(
	ctx context.Context,
	queryParams map[string]interface{},
	params exchange.OrderStatusRequestParams,
	source OrderSource,
) (OrderInfo, error) {
	var response *bybit.ServerResponse
	var err error

	switch source {
	case OrderSourceRealtime:
		response, err = tc.BybitClient.NewUtaBybitServiceWithParams(queryParams).GetOpenOrders(ctx)
	case OrderSourceHistory:
		response, err = tc.BybitClient.NewUtaBybitServiceWithParams(queryParams).GetOrderHistory(ctx)
	}

	if err != nil {
		return OrderInfo{}, err
	}

	return tc.parseOrderResponse(response, params)
}

func (tc *TradingClient) parseOrderResponse(
	ordersResponse *bybit.ServerResponse,
	params exchange.OrderStatusRequestParams,
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
			orderStatus, ok := parseOrderStatus(entry.Status)
			if !ok {
				return OrderInfo{}, fmt.Errorf(
					"bybit - get_open_order - Cannot parse unknown order status. Status: %s",
					entry.Status,
				)
			}

			return OrderInfo{
				OrderId:   entry.OrderId,
				Symbol:    entry.Symbol,
				Price:     entry.Price,
				Qty:       entry.Qty,
				LeavesQty: entry.LeavesQty,
				Status:    orderStatus,
				Side:      model.ParseOrderSide(entry.Side),
				CreatedAt: millisecondsToTime(entry.CreatedTime),
				UpdatedAt: millisecondsToTime(entry.UpdatedTime),
				Type:      model.ParseOrderType(entry.Type),
			}, nil
		}
	}
	return OrderInfo{}, nil
}

func (tc *TradingClient) fetchPrecisionFromAPI(
	ctx context.Context,
	symbol string,
) (float64, float64, error) {
	params := map[string]interface{}{
		"symbol":   symbol,
		"category": "spot",
	}

	response, err := tc.BybitClient.NewUtaBybitServiceWithParams(params).GetInstrumentInfo(ctx)
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

func (tc *TradingClient) getOrderStatusCacheDuration() time.Duration {
	return tc.cfg.Defaults.OrderStatusCacheDuration
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

// millisecondsToTime convert a timestamp in milliseconds into a Time object
func millisecondsToTime(ms int) time.Time {
	return time.Unix(0, int64(ms)*int64(time.Millisecond))
}
