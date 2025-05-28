// // Package exchange implements exchange interface for specific exchange called Bybit
package bybit

//
//import (
//	"context"
//	"encoding/json"
//	"fmt"
//	"io"
//	"log"
//	"net/http"
//	"net/url"
//	"strconv"
//	"strings"
//	"sync"
//	"time"
//
//	"github.com/nihadazimli/crypbot/internal/exchange"
//
//	"github.com/nihadazimli/crypbot/config"
//	"github.com/nihadazimli/crypbot/internal/model"
//	"github.com/nihadazimli/crypbot/pkg/numutils"
//	"github.com/nihadazimli/crypbot/pkg/websocket"
//	bybit "github.com/wuhewuhe/bybit.go.api"
//)
//
//type OrderInfoResult struct {
//	List []struct {
//		OrderId     string  `json:"orderId"`
//		Price       float64 `json:"price,string"`
//		Qty         float64 `json:"qty,string"`
//		LeavesQty   float64 `json:"leavesQty,string"`
//		Symbol      string  `json:"symbol"`
//		Status      string  `json:"orderStatus"`
//		Side        string  `json:"side"`
//		CreatedTime int     `json:"createdTime,string"`
//		UpdatedTime int     `json:"updatedTime,string"`
//		Type        string  `json:"orderType"`
//	}
//}
//type OrderSource int
//
//const (
//	OrderSourceRealtime OrderSource = iota
//	OrderSourceHistory
//)
//
//type ActiveOrderTracker struct {
//	Status       model.OrderStatus
//	Symbol       string
//	Price        float64
//	Type         model.OrderType
//	RemainingQty float64
//	Qty          float64
//	UpdatedAt    time.Time
//	CreatedAt    time.Time
//	Side         model.OrderSide
//	LastUpdated  time.Time // Last time order tracker received data
//}
//
//type OrderInfo struct {
//	OrderId   string
//	Price     float64
//	Qty       float64
//	LeavesQty float64
//	Symbol    string
//	Status    model.OrderStatus
//	Side      model.OrderSide
//	CreatedAt time.Time
//	UpdatedAt time.Time
//	Type      model.OrderType
//}
//
//func (oi OrderInfo) isEmpty() bool {
//	return oi.OrderId == "" && oi.Symbol == ""
//}
//
//type InstrumentResult struct {
//	List []struct {
//		Symbol        string `json:"symbol"`
//		LotSizeFilter struct {
//			BasePrecision string `json:"basePrecision"`
//		} `json:"lotSizeFilter"`
//		PriceFilter struct {
//			TickSize string `json:"tickSize"`
//		} `json:"priceFilter"`
//	} `json:"list"`
//}
//
//type WalletResult struct {
//	List []struct {
//		TotalAvailableBalance string `json:"totalAvailableBalance"`
//		TotalWalletBalance    string `json:"totalWalletBalance"`
//		Coin                  []struct {
//			WalletBalance string `json:"walletBalance"`
//			UsdValue      string `json:"usdValue"`
//		} `json:"coin"`
//	} `json:"list"`
//}
//
//type OrderBookEntry []string
//
//type OrderBookResponse struct {
//	Symbol    string           `json:"s"`
//	Bids      []OrderBookEntry `json:"b"`
//	Asks      []OrderBookEntry `json:"a"`
//	Timestamp int64            `json:"ts"`
//	UpdateID  int64            `json:"u"`
//}
//
//var orderStatusMap = map[string]model.OrderStatus{
//	"Filled":                  model.OrderFilled,
//	"Cancelled":               model.OrderCanceled,
//	"Rejected":                model.OrderCanceled,
//	"PartiallyFilled":         model.OrderPartiallyFilled,
//	"Untriggered":             model.OrderOpen,
//	"New":                     model.OrderOpen,
//	"Triggered":               model.OrderOpen,
//	"Deactivated":             model.OrderCanceled,
//	"PartiallyFilledCanceled": model.OrderPartiallyFilledCancelled,
//}
//
//const (
//	AmendOrderErrCodeQuantityTooLow      = 170136
//	AmendOrderErrCodeInsufficientBalance = 170131
//	AmendOrderErrCodeUnchangedParameters = 10001
//	AmendOrderErrOrderDoesNotExist       = 170213
//	AmendOrderSuccess                    = 0
//)
//
//type Adapter struct {
//	ctx                   context.Context
//	BybitClient           *bybit.Client
//	cfg                   *config.Config
//	orderWebsocket        *websocket.Client
//	activeOrders          map[string]ActiveOrderTracker
//	activeOrdersToFollow  map[string]struct{} // These are the orders that are state followed through order websocket
//	klineWriteChan        chan model.WSInternalKline
//	precision             map[string]model.Precision
//	precisionMutex        sync.RWMutex
//	orderStreamSubscribed bool
//}
//
//func NewBybitAdapter(ctx context.Context) *Adapter {
//	cfg := config.GetConfig()
//	ba := &Adapter{
//		ctx: ctx,
//		BybitClient: bybit.NewBybitHttpClient(
//			cfg.Exchange.API.Key,
//			cfg.Exchange.API.Secret,
//			bybit.WithBaseURL(cfg.Exchange.API.Host),
//		),
//		cfg:                  cfg,
//		precision:            make(map[string]model.Precision),
//		activeOrders:         make(map[string]ActiveOrderTracker),
//		activeOrdersToFollow: make(map[string]struct{}),
//	}
//	ba.orderWebsocket = websocket.NewWebSocketClientPrivate(
//		ba.cfg.Exchange.WebSocket.AddressPrivate,
//		ba.cfg.Exchange.API.Key,
//		ba.cfg.Exchange.API.Secret,
//		ba.cfg.Exchange.WebSocket.PingIntervalDuration,
//		ba.handlePrivateWSMessages,
//	)
//	ba.monitorActiveOrders(cfg.Exchange.WebSocket.MonitorIntervalDuration)
//	return ba
//}
//
//func (ba *Adapter) GetAccountBalance(ctx context.Context) (exchange.AccountBalanceResult, error) {
//	requestParams := map[string]interface{}{
//		"accountType": "UNIFIED",
//	}
//
//	walletResult, err := ba.getWalletResult(ctx, requestParams)
//
//	if err != nil {
//		return exchange.AccountBalanceResult{}, err
//	}
//
//	totalAvailableBalance, err := strconv.ParseFloat(walletResult.List[0].TotalWalletBalance, 64)
//
//	if err != nil {
//		return exchange.AccountBalanceResult{}, err
//	}
//
//	return exchange.AccountBalanceResult{Total: totalAvailableBalance}, nil
//}
//
//func (ba *Adapter) getWalletResult(
//	ctx context.Context,
//	requestParams map[string]interface{},
//) (WalletResult, error) {
//	walletResponse, err := ba.BybitClient.NewUtaBybitServiceWithParams(requestParams).
//		GetAccountWallet(ctx)
//	if err != nil {
//		return WalletResult{}, err
//	}
//	resultData, err := json.Marshal(walletResponse.Result)
//	if err != nil {
//		return WalletResult{}, err
//	}
//
//	var walletResult WalletResult
//	err = json.Unmarshal(resultData, &walletResult)
//	if err != nil {
//		return WalletResult{}, err
//	}
//	return walletResult, nil
//}
//
//func (ba *Adapter) PlaceBuyOrder(
//	ctx context.Context,
//	params exchange.BuyOrderRequestParams,
//) (exchange.BuyOrderResult, error) {
//
//	precision, err := ba.getPrecision(ctx, params.Symbol)
//	if err != nil {
//		return exchange.BuyOrderResult{}, fmt.Errorf(
//			"PlaceBuyOrder - failed to get precision: %w",
//			err,
//		)
//	}
//
//	qty := numutils.RoundDownToPrecision(params.OrderAmount, precision.Qty)
//	stopLossPrice := numutils.RoundDownToPrecision(params.StopLimit, precision.Price)
//
//	requestParams := map[string]interface{}{
//		"category":    "spot",
//		"symbol":      params.Symbol,
//		"side":        "Buy",
//		"qty":         strconv.FormatFloat(qty, 'f', -1, 64),
//		"stopLoss":    strconv.FormatFloat(stopLossPrice, 'f', -1, 64),
//		"slOrderType": "Market",
//	}
//
//	switch params.OrderType {
//	case model.OrderTypeLimit:
//		if params.LimitPrice == nil {
//			return exchange.BuyOrderResult{}, fmt.Errorf("limit price is required for limit orders")
//		}
//		limitPrice := numutils.RoundDownToPrecision(*params.LimitPrice, precision.Price)
//		requestParams["orderType"] = "Limit"
//		requestParams["price"] = strconv.FormatFloat(limitPrice, 'f', -1, 64)
//	case model.OrderTypeMarket:
//		requestParams["orderType"] = "Market"
//	default:
//		return exchange.BuyOrderResult{}, fmt.Errorf("unsupported order type: %s", params.OrderType)
//	}
//
//	response, err := ba.BybitClient.NewUtaBybitServiceWithParams(requestParams).PlaceOrder(ctx)
//
//	if err != nil {
//		return exchange.BuyOrderResult{}, err
//	}
//
//	// Check if the response indicates success
//	if response.RetCode != 0 {
//		responseJSON, jsonErr := json.Marshal(response)
//		if jsonErr != nil {
//			log.Printf("error marshalling full response: %s\n", jsonErr)
//		} else {
//			log.Printf("\nError: Place Buy Order\nFull response: %s\nParams: %s\n", string(responseJSON), requestParams)
//		}
//		return exchange.BuyOrderResult{}, fmt.Errorf(
//			"API error. Code: %v Error: %s",
//			response.RetCode,
//			response.RetMsg,
//		)
//	}
//
//	// Handling the Result field which is interface{}
//	resultData, err := json.Marshal(response.Result)
//	if err != nil {
//		return exchange.BuyOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
//	}
//
//	var buyDetails exchange.BuyOrderResultDetails
//	err = json.Unmarshal(resultData, &buyDetails)
//	if err != nil {
//		return exchange.BuyOrderResult{}, fmt.Errorf(
//			"error unmarshalling result to BuyOrderResultDetails: %s",
//			err,
//		)
//	}
//
//	if params.OrderType == model.OrderTypeLimit {
//		err := ba.followOrder(buyDetails.OrderId)
//
//		if err != nil {
//			log.Printf("PlaceBuyOrder - cannot connect to order websocket stream. Err:%v\n", err)
//		}
//	}
//
//	return exchange.BuyOrderResult{
//		Success: true,
//		Details: exchange.BuyOrderResultDetails{
//			OrderId:     buyDetails.OrderId,
//			OrderLinkId: buyDetails.OrderLinkId,
//		},
//	}, nil
//}
//
//func (ba *Adapter) GetOrderBook(
//	ctx context.Context,
//	params exchange.OrderBookRequestParams,
//) (exchange.OrderBookResult, error) {
//	requestParams := map[string]interface{}{
//		"symbol":   params.Symbol,
//		"limit":    params.Limit,
//		"category": "spot",
//	}
//	orderBookService := ba.BybitClient.NewUtaBybitServiceWithParams(requestParams)
//
//	response, err := orderBookService.GetOrderBookInfo(ctx)
//	if err != nil {
//		return exchange.OrderBookResult{}, fmt.Errorf(
//			"cannot fetch orderbook\n symbol:%s\nerr:%v",
//			params.Symbol,
//			err,
//		)
//	}
//
//	// Check if the response indicates success
//	if response.RetCode != 0 {
//		responseJSON, jsonErr := json.Marshal(response)
//		if jsonErr != nil {
//			log.Printf("error marshalling full response: %s\n", jsonErr)
//		} else {
//			log.Printf("Error: Get Order Book\nFull response: %s\n", string(responseJSON))
//		}
//		return exchange.OrderBookResult{}, fmt.Errorf(
//			"API error. Code: %v Error: %s",
//			response.RetCode,
//			response.RetMsg,
//		)
//	}
//
//	resultData, err := json.Marshal(response.Result)
//	if err != nil {
//		return exchange.OrderBookResult{}, fmt.Errorf("error marshalling result data: %s", err)
//	}
//
//	var orderBookResponse OrderBookResponse
//	err = json.Unmarshal(resultData, &orderBookResponse)
//	if err != nil {
//		return exchange.OrderBookResult{}, fmt.Errorf(
//			"error unmarshalling result to OrderBookResponse: %s",
//			err,
//		)
//	}
//
//	var all_asks []exchange.OrderBookDetail
//	var all_bids []exchange.OrderBookDetail
//	for _, ask := range orderBookResponse.Asks {
//		price, _ := strconv.ParseFloat(ask[0], 64)
//		qty, _ := strconv.ParseFloat(ask[1], 64)
//
//		all_asks = append(all_asks, exchange.OrderBookDetail{Price: price, Quantity: qty})
//	}
//	for _, bid := range orderBookResponse.Bids {
//		price, _ := strconv.ParseFloat(bid[0], 64)
//		qty, _ := strconv.ParseFloat(bid[1], 64)
//		all_bids = append(all_bids, exchange.OrderBookDetail{Price: price, Quantity: qty})
//	}
//
//	return exchange.OrderBookResult{
//		Ask: all_asks,
//		Bid: all_bids,
//	}, nil
//}
//
//func (ba *Adapter) PlaceSellOrder(
//	ctx context.Context,
//	params exchange.SellOrderRequestParams,
//) (exchange.SellOrderResult, error) {
//
//	precision, err := ba.getPrecision(ctx, params.Symbol)
//	if err != nil {
//		return exchange.SellOrderResult{}, fmt.Errorf(
//			"PlaceSellOrder - failed to get precision: %w",
//			err,
//		)
//	}
//
//	qty := numutils.RoundDownToPrecision(params.OrderAmount, precision.Qty)
//
//	// Base request parameters
//	requestParams := map[string]interface{}{
//		"category": "spot",
//		"symbol":   params.Symbol,
//		"side":     "Sell",
//		"qty":      strconv.FormatFloat(qty, 'f', -1, 64),
//	}
//
//	// Set order type specific parameters
//	switch params.OrderType {
//	case model.OrderTypeLimit:
//		if params.LimitPrice == nil {
//			return exchange.SellOrderResult{}, fmt.Errorf("limit price is required for limit orders")
//		}
//		limitPrice := numutils.RoundDownToPrecision(*params.LimitPrice, precision.Price)
//		requestParams["orderType"] = "Limit"
//		requestParams["price"] = strconv.FormatFloat(limitPrice, 'f', -1, 64)
//	case model.OrderTypeMarket:
//		requestParams["orderType"] = "Market"
//	default:
//		return exchange.SellOrderResult{}, fmt.Errorf("unsupported order type: %s", params.OrderType)
//	}
//
//	log.Printf("request params: %v\n", requestParams)
//	response, err := ba.BybitClient.NewUtaBybitServiceWithParams(requestParams).PlaceOrder(ctx)
//
//	if err != nil {
//		return exchange.SellOrderResult{}, err
//	}
//
//	// Check if the response indicates success
//	if response.RetCode != 0 {
//		responseJSON, jsonErr := json.Marshal(response)
//		if jsonErr != nil {
//			log.Printf("error marshalling full response: %s\n", jsonErr)
//		} else {
//			log.Printf("Error: Place Sell Order\nFull response: %s\n", string(responseJSON))
//		}
//		return exchange.SellOrderResult{}, fmt.Errorf(
//			"API error. Code: %v Error: %s",
//			response.RetCode,
//			response.RetMsg,
//		)
//	}
//
//	// Handling the Result field which is interface{}
//	resultData, err := json.Marshal(response.Result)
//	if err != nil {
//		return exchange.SellOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
//	}
//
//	var sellDetails exchange.SellOrderResultDetails
//	err = json.Unmarshal(resultData, &sellDetails)
//	if err != nil {
//		return exchange.SellOrderResult{}, fmt.Errorf(
//			"error unmarshalling result to SellOrderResultDetails: %s",
//			err,
//		)
//	}
//	if params.OrderType == model.OrderTypeLimit {
//		err := ba.followOrder(sellDetails.OrderId)
//		if err != nil {
//			log.Printf("PlaceBuyOrder - cannot connect to order websocket stream. Err:%v\n", err)
//		}
//	}
//	return exchange.SellOrderResult{
//		Success: true,
//		Details: exchange.SellOrderResultDetails{
//			OrderId:     sellDetails.OrderId,
//			OrderLinkId: sellDetails.OrderLinkId,
//		},
//	}, nil
//}
//
//func (ba *Adapter) CancelAllTPSLOrders(ctx context.Context, symbol string) error {
//	requestParams := map[string]interface{}{
//		"category":    "spot",
//		"symbol":      symbol,
//		"orderFilter": "tpslOrder",
//	}
//	response, err := ba.BybitClient.NewUtaBybitServiceWithParams(requestParams).CancelAllOrders(ctx)
//	if err != nil {
//		return fmt.Errorf("Cannot cancel order. Err: %s\n", err)
//	}
//	// Check if the response indicates success
//	if response.RetCode != 0 {
//		responseJSON, jsonErr := json.Marshal(response)
//		if jsonErr != nil {
//			return fmt.Errorf("error marshalling full response: %s\n", jsonErr)
//		} else {
//			return fmt.Errorf("Error: Place Sell Order\nFull response: %s", string(responseJSON))
//		}
//	}
//	return nil
//}
//
//func (ba *Adapter) CancelOrder(
//	ctx context.Context,
//	params exchange.CancelOrderRequestParams,
//) (exchange.CancelOrderResult, error) {
//	requestParams := map[string]interface{}{
//		"category": "spot",
//		"symbol":   params.Symbol,
//		"orderId":  params.OrderId,
//	}
//	response, err := ba.BybitClient.NewUtaBybitServiceWithParams(requestParams).CancelOrder(ctx)
//
//	if err != nil {
//		return exchange.CancelOrderResult{}, err
//	}
//
//	// Check if the response indicates success
//	if response.RetCode != 0 {
//		responseJSON, jsonErr := json.Marshal(response)
//		if jsonErr != nil {
//			log.Printf("error marshalling full response: %s\n", jsonErr)
//		} else {
//			log.Printf("Error: Cancel Order\nFull response: %s\n", string(responseJSON))
//		}
//		return exchange.CancelOrderResult{}, fmt.Errorf(
//			"API error. Code: %v Error: %s",
//			response.RetCode,
//			response.RetMsg,
//		)
//	}
//
//	resultData, err := json.Marshal(response.Result)
//
//	if err != nil {
//		return exchange.CancelOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
//	}
//	var cancelOrderDetails exchange.CancelOrderResult
//	err = json.Unmarshal(resultData, &cancelOrderDetails)
//
//	return exchange.CancelOrderResult{OrderId: cancelOrderDetails.OrderId}, nil
//
//}
//
//func (ba *Adapter) AmendOrder(
//	ctx context.Context,
//	params exchange.AmendOrderRequestParams,
//) (exchange.AmendOrderResult, error) {
//	requestParams := make(map[string]interface{})
//	requestParams["category"] = "spot"
//	requestParams["symbol"] = params.Symbol
//	requestParams["orderId"] = params.OrderId
//
//	precision, err := ba.getPrecision(ctx, params.Symbol)
//	if err != nil {
//		return exchange.AmendOrderResult{}, fmt.Errorf(
//			"AmendOrder - failed to get precision: %w",
//			err,
//		)
//	}
//
//	if params.Price != nil {
//		price := numutils.RoundDownToPrecision(*params.Price, precision.Price)
//		requestParams["price"] = strconv.FormatFloat(price, 'f', -1, 64)
//	}
//
//	if params.Quantity != nil {
//		qty := numutils.RoundDownToPrecision(*params.Quantity, precision.Qty)
//		requestParams["qty"] = strconv.FormatFloat(qty, 'f', -1, 64)
//	}
//
//	if params.StopLoss != nil {
//		stopLoss := numutils.RoundDownToPrecision(*params.StopLoss, precision.Price)
//		requestParams["stopLoss"] = strconv.FormatFloat(stopLoss, 'f', -1, 64)
//	}
//
//	response, err := ba.BybitClient.NewUtaBybitServiceWithParams(requestParams).AmendOrder(ctx)
//
//	if err != nil {
//		return exchange.AmendOrderResult{}, err
//	}
//
//	if response.RetCode == AmendOrderErrCodeQuantityTooLow { // Use the constant here
//		return exchange.AmendOrderResult{}, fmt.Errorf("order quantity too low. err: %w", exchange.AmendErrQuantityTooLow)
//	} else if response.RetCode == AmendOrderErrCodeUnchangedParameters {
//		return exchange.AmendOrderResult{}, fmt.Errorf("err: %w, params:%v", exchange.AmendErrUnchangedParams, params)
//	} else if response.RetCode == AmendOrderErrOrderDoesNotExist {
//		return exchange.AmendOrderResult{}, fmt.Errorf("err: %w, params: %v", exchange.AmendErrOrderDoesNotExist, params)
//	} else if response.RetCode == AmendOrderErrCodeInsufficientBalance {
//		return exchange.AmendOrderResult{}, fmt.Errorf("err: %w, params: %v", exchange.AmendErrInsufficientBalance, params)
//	} else if response.RetCode != AmendOrderSuccess {
//		responseJSON, jsonErr := json.Marshal(response)
//		if jsonErr != nil {
//			log.Printf("error marshalling full response: %s\n", jsonErr)
//		} else {
//			log.Printf("\nError: Amend Order\nFull response: %s\nparams:%s\n", string(responseJSON), requestParams)
//		}
//		return exchange.AmendOrderResult{}, fmt.Errorf(
//			"API error. Code: %v Error: %s",
//			response.RetCode,
//			response.RetMsg,
//		)
//	}
//	log.Printf("Order amended successfully. Params: %v", requestParams)
//
//	resultData, err := json.Marshal(response.Result)
//	if err != nil {
//		return exchange.AmendOrderResult{}, fmt.Errorf("error marshalling result data: %s", err)
//	}
//
//	var details exchange.AmendOrderResult
//	err = json.Unmarshal(resultData, &details)
//	if err != nil {
//		return exchange.AmendOrderResult{}, fmt.Errorf(
//			"error unmarshalling result to AmendOrderResultDetails: %s",
//			err,
//		)
//	}
//	delete(ba.activeOrders, params.OrderId)
//
//	err = ba.followOrder(details.OrderId)
//	if err != nil {
//		log.Printf("PlaceBuyOrder - cannot connect to order websocket stream. Err:%v\n", err)
//	}
//	return details, nil
//}
//
//func (ba *Adapter) GetKlineHistory(ctx context.Context, params exchange.KlineHistoryParams) (exchange.KlineHistoryResult, error) {
//	startTime, endTime := determineTimeInterval(params.Limit, params.Interval)
//
//	requestParams := map[string]string{
//		"symbol":   params.Symbol,
//		"category": ba.cfg.MarketConstants.KlineCategory,
//		"interval": strconv.Itoa(params.Interval),
//		"start":    strconv.FormatInt(startTime, 10),
//		"end":      strconv.FormatInt(endTime, 10),
//		"limit":    strconv.Itoa(params.Limit + 1),
//	}
//
//	requestUrl := buildUrl(ba.cfg.Exchange.API.Host, ba.cfg.Exchange.API.RequestPaths.Kline, requestParams)
//	req, err := http.NewRequestWithContext(ctx, "GET", requestUrl, nil)
//	if err != nil {
//		return exchange.KlineHistoryResult{}, fmt.Errorf("error creating request for kline data: %w", err)
//	}
//
//	response, err := ba.BybitClient.HTTPClient.Do(req)
//	if err != nil {
//		return exchange.KlineHistoryResult{}, fmt.Errorf("error fetching kline data: %w", err)
//	}
//	defer response.Body.Close()
//
//	body, err := io.ReadAll(response.Body)
//	if err != nil {
//		return exchange.KlineHistoryResult{}, fmt.Errorf("error reading response body: %w", err)
//	}
//
//	var apiResponse model.KlineApiResponse
//	if err := json.Unmarshal(body, &apiResponse); err != nil {
//		return exchange.KlineHistoryResult{}, fmt.Errorf("error unmarshaling JSON. Respomse: %v Err: %w",
//			apiResponse, err)
//	}
//
//	if len(apiResponse.Result.List) < 1 {
//		return exchange.KlineHistoryResult{}, fmt.Errorf("no kline data available for symbol: %s", params.Symbol)
//	}
//
//	// Extract the 5th value (close price) from each list
//	closePrices := make([]float64, 0, len(apiResponse.Result.List))
//	volumeValues := make([]float64, 0, len(apiResponse.Result.List))
//	for _, item := range apiResponse.Result.List {
//		closePrice, _ := strconv.ParseFloat(strings.TrimSpace(item[4]), 64)
//		volumeAmount, _ := strconv.ParseFloat(strings.TrimSpace(item[5]), 64)
//
//		closePrices = append(closePrices, closePrice)
//		volumeValues = append(volumeValues, volumeAmount*closePrice)
//	}
//	reverse(closePrices)
//	reverse(volumeValues)
//
//	// Eliminate live value, just return most recent closed values
//	return exchange.KlineHistoryResult{
//		ClosePrices: closePrices[:len(closePrices)-1],
//		Volumes:     volumeValues[:len(volumeValues)-1],
//	}, nil
//}
//
//func (ba *Adapter) GetOrderStatus(ctx context.Context, params exchange.OrderStatusRequestParams) (exchange.OrderStatusResponse, error) {
//	order, exists := ba.activeOrders[params.OrderId]
//
//	if exists && time.Since(order.LastUpdated) <= ba.getOrderStatusCacheDuration() {
//		return exchange.OrderStatusResponse{
//			Symbol:       order.Symbol,
//			OrderId:      params.OrderId,
//			Status:       order.Status,
//			Side:         order.Side,
//			Price:        order.Price,
//			Type:         order.Type,
//			RemainingQty: order.RemainingQty,
//			Qty:          order.Qty,
//			UpdatedAt:    order.UpdatedAt,
//			CreatedAt:    order.CreatedAt,
//		}, nil
//	}
//
//	orderFromApi, err := ba.getOrderWithApi(ctx, params)
//	if err != nil {
//		return exchange.OrderStatusResponse{Status: model.OrderInvalid}, err
//	}
//
//	ba.activeOrders[params.OrderId] = ActiveOrderTracker{
//		Symbol:       orderFromApi.Symbol,
//		Status:       orderFromApi.Status,
//		RemainingQty: orderFromApi.LeavesQty,
//		CreatedAt:    orderFromApi.CreatedAt,
//		UpdatedAt:    orderFromApi.UpdatedAt,
//		Price:        orderFromApi.Price,
//		Type:         orderFromApi.Type,
//		Side:         orderFromApi.Side,
//		LastUpdated:  time.Now(),
//	}
//
//	return exchange.OrderStatusResponse{
//		Symbol:       orderFromApi.Symbol,
//		OrderId:      orderFromApi.OrderId,
//		Price:        orderFromApi.Price,
//		Type:         orderFromApi.Type,
//		Status:       orderFromApi.Status,
//		Side:         orderFromApi.Side,
//		CreatedAt:    orderFromApi.CreatedAt,
//		UpdatedAt:    orderFromApi.UpdatedAt,
//		RemainingQty: orderFromApi.LeavesQty,
//		Qty:          orderFromApi.Qty,
//	}, nil
//
//}
//
//func (ba *Adapter) GetSymbolBalance(ctx context.Context, symbol string) (exchange.SymbolBalanceResult, error) {
//	baseCoin := removeUSDTSuffix(symbol)
//
//	requestParams := map[string]interface{}{
//		"accountType": "UNIFIED",
//		"coin":        baseCoin,
//	}
//
//	walletResult, err := ba.getWalletResult(ctx, requestParams)
//	if err != nil {
//		return exchange.SymbolBalanceResult{}, err
//	}
//
//	if len(walletResult.List[0].Coin) == 0 {
//		return exchange.SymbolBalanceResult{}, fmt.Errorf("cannot get balance for coin %s", symbol)
//	}
//
//	totalSymbolBalance, err := strconv.ParseFloat(walletResult.List[0].Coin[0].WalletBalance, 64)
//	if err != nil {
//		return exchange.SymbolBalanceResult{}, fmt.Errorf("error parsing total symbol wallet balance for %s", symbol)
//	}
//	usdValue, err := strconv.ParseFloat(walletResult.List[0].Coin[0].UsdValue, 64)
//	if err != nil {
//		return exchange.SymbolBalanceResult{}, fmt.Errorf("error parsing total symbol wallet balance for %s", symbol)
//	}
//	symbolBalanceResponse := exchange.SymbolBalanceResult{
//		WalletBalance: totalSymbolBalance,
//		UsdValue:      usdValue,
//	}
//	return symbolBalanceResponse, nil
//}
//
//func (ba *Adapter) StartKlineStream(
//	ctx context.Context,
//	params exchange.KlineStreamRequestParams,
//) error {
//	klineTopics := ba.createKlineTopics(params.Symbols, params.Interval)
//	symbolChunks := chunkSymbols(klineTopics, ba.cfg.Exchange.WebSocket.MaxSymbolsPerConnection)
//	var webSockets []*websocket.Client
//
//	for _, chunk := range symbolChunks {
//		ws := websocket.NewWebSocketClientPublic(
//			ba.cfg.Exchange.WebSocket.AddressPublic,
//			ba.cfg.Exchange.WebSocket.PingIntervalDuration,
//			ba.handleKlineMessages,
//		)
//		ba.klineWriteChan = params.KlineWriteChan
//
//		err := ws.ConnectAndStart(ctx, chunk)
//		if err != nil {
//			for _, ws := range webSockets {
//				ws.Shutdown()
//			}
//			return err
//		}
//		webSockets = append(webSockets, ws)
//	}
//	return nil
//}
//
//func (ba *Adapter) handleKlineMessages(message []byte) error {
//	var WSKline model.WSRawKline
//	err := json.Unmarshal(message, &WSKline)
//	if err != nil {
//		log.Println("Failed to unmarshal JSON:", err)
//		return err
//	}
//
//	for _, kline := range WSKline.Data {
//		internalKline := model.WSInternalKline{
//			Symbol:   getSymbolFromKlineTopic(WSKline.Topic),
//			Close:    kline.Close,
//			IsClosed: kline.Confirm,
//			Volume:   kline.Volume,
//		}
//		ba.klineWriteChan <- internalKline
//
//	}
//	return nil
//}
//
//func getSymbolFromKlineTopic(topic string) string {
//	return topic[strings.LastIndex(topic, ".")+1:]
//}
//func (ba *Adapter) monitorActiveOrders(pollInterval time.Duration) {
//	go func() {
//		ticker := time.NewTicker(pollInterval)
//		defer ticker.Stop()
//
//		for range ticker.C {
//			for orderId := range ba.activeOrdersToFollow {
//				state, exists := ba.activeOrders[orderId]
//				if !exists {
//					continue // no update yet, wait for next tick
//				}
//
//				if state.Status.IsFinished() {
//					delete(ba.activeOrdersToFollow, orderId)
//					log.Printf("Order %s reached final state (%s), removed from watch list\n", orderId, state.Status)
//				}
//			}
//
//			if len(ba.activeOrdersToFollow) == 0 && ba.orderStreamSubscribed {
//				log.Println("No active orders left. Stopping order stream.")
//				err := ba.unsubscribeFromOrderStream()
//				if err != nil {
//					log.Printf("Cannot unsubscribe from order stream. Err: %v\n", err)
//				}
//				ba.orderStreamSubscribed = false
//			}
//		}
//	}()
//}
//
//func (ba *Adapter) followOrder(orderId string) error {
//	if !ba.orderStreamSubscribed {
//		err := ba.subscribeToOrderStream(ba.ctx)
//		if err != nil {
//			return err
//		}
//		ba.orderStreamSubscribed = true
//	}
//
//	ba.activeOrdersToFollow[orderId] = struct{}{}
//	return nil
//}
//
//func (ba *Adapter) unsubscribeFromOrderStream() error {
//	ba.orderWebsocket.Shutdown()
//	return nil
//}
//
//func (ba *Adapter) subscribeToOrderStream(ctx context.Context) error {
//	err := ba.orderWebsocket.ConnectAndStart(ctx, []string{"order"})
//	if err != nil {
//		return err
//	}
//	return nil
//}
//
//func (ba *Adapter) handlePrivateWSMessages(message []byte) error {
//	log.Printf("Received Private Stream Message: %s\n", string(message))
//	var privateMessage model.BybitWSPrivateMessage
//	err := json.Unmarshal(message, &privateMessage)
//	if err != nil {
//		return err
//	}
//
//	switch privateMessage.Topic {
//	case "order":
//		return ba.handleOrderMessage(privateMessage.Data)
//	default:
//		log.Printf(
//			"unhandled topic: %s,Data:%s\n",
//			privateMessage.Topic,
//			string(privateMessage.Data),
//		)
//		return nil
//	}
//
//}
//func (ba *Adapter) GetPrecision(ctx context.Context, symbol string) (model.Precision, error) {
//	return ba.getPrecision(ctx, symbol)
//}
//
//func (ba *Adapter) getPrecision(ctx context.Context, symbol string) (model.Precision, error) {
//	ba.precisionMutex.RLock()
//	p, exists := ba.precision[symbol]
//	ba.precisionMutex.RUnlock()
//
//	if exists {
//		return p, nil
//	}
//	price, qty, err := ba.fetchPrecisionFromAPI(ctx, symbol)
//	if err != nil {
//		return model.Precision{}, err
//	}
//
//	ba.precisionMutex.Lock()
//	ba.precision[symbol] = model.Precision{Price: price, Qty: qty}
//	ba.precisionMutex.Unlock()
//
//	return ba.precision[symbol], nil
//}
//
//func (ba *Adapter) handleOrderMessage(data json.RawMessage) error {
//	var orders []model.BybitWSOrderData
//	err := json.Unmarshal(data, &orders)
//	if err != nil {
//		return err
//	}
//
//	for _, order := range orders {
//		orderStatus, ok := parseOrderStatus(order.Status)
//
//		orderState := ActiveOrderTracker{
//			Status:       orderStatus,
//			Symbol:       order.Symbol,
//			Price:        order.Price,
//			Side:         model.ParseOrderSide(order.Side),
//			CreatedAt:    millisecondsToTime(order.CreatedTime),
//			UpdatedAt:    millisecondsToTime(order.UpdatedTime),
//			Type:         model.ParseOrderType(order.Type),
//			Qty:          order.Qty,
//			RemainingQty: order.LeavesQty,
//			LastUpdated:  time.Now(),
//		}
//
//		if !ok {
//			return fmt.Errorf("bybit - Unhandled Order Status. Status: %s", order.Status)
//		}
//
//		ba.activeOrders[order.OrderID] = orderState
//	}
//	return nil
//}
//
//// getOrderWithApi fetches order details from /v5/order/realtime or /v5/order/history endpoint of Bybit
//func (ba *Adapter) getOrderWithApi(
//	ctx context.Context,
//	params exchange.OrderStatusRequestParams,
//) (OrderInfo, error) {
//	queryParams := map[string]interface{}{
//		"category": "spot",
//		"orderId":  params.OrderId,
//		"symbol":   params.Symbol,
//	}
//
//	orderInfo, err := ba.fetchOrderFromSource(ctx, queryParams, params, OrderSourceRealtime)
//	if err != nil {
//		return OrderInfo{}, fmt.Errorf("failed to fetch realtime order: %w", err)
//	}
//	if !orderInfo.isEmpty() {
//		return orderInfo, nil
//	}
//
//	orderInfo, err = ba.fetchOrderFromSource(ctx, queryParams, params, OrderSourceHistory)
//	if err != nil {
//		return OrderInfo{}, fmt.Errorf("failed to fetch historical order: %w", err)
//	}
//
//	if !orderInfo.isEmpty() {
//		return orderInfo, nil
//	}
//
//	return OrderInfo{}, fmt.Errorf("order not found for params: %+v", params)
//}
//
//func (ba *Adapter) fetchOrderFromSource(
//	ctx context.Context,
//	queryParams map[string]interface{},
//	params exchange.OrderStatusRequestParams,
//	source OrderSource,
//) (OrderInfo, error) {
//	var response *bybit.ServerResponse
//	var err error
//
//	switch source {
//	case OrderSourceRealtime:
//		response, err = ba.BybitClient.NewUtaBybitServiceWithParams(queryParams).GetOpenOrders(ctx)
//	case OrderSourceHistory:
//		response, err = ba.BybitClient.NewUtaBybitServiceWithParams(queryParams).GetOrderHistory(ctx)
//	}
//
//	if err != nil {
//		return OrderInfo{}, err
//	}
//
//	return ba.parseOrderResponse(response, params)
//}
//
//func (ba *Adapter) parseOrderResponse(
//	ordersResponse *bybit.ServerResponse,
//	params exchange.OrderStatusRequestParams,
//) (OrderInfo, error) {
//
//	resultData, err := json.Marshal(ordersResponse.Result)
//	if err != nil {
//		return OrderInfo{}, err
//	}
//
//	var orderInfoResult OrderInfoResult
//	err = json.Unmarshal(resultData, &orderInfoResult)
//	if err != nil {
//		return OrderInfo{}, err
//	}
//
//	for _, entry := range orderInfoResult.List {
//		if entry.OrderId == params.OrderId || entry.Symbol == params.Symbol {
//			orderStatus, ok := parseOrderStatus(entry.Status)
//			if !ok {
//				return OrderInfo{}, fmt.Errorf(
//					"bybit - get_open_order - Cannot parse unknown order status. Status: %s",
//					entry.Status,
//				)
//			}
//
//			return OrderInfo{
//				OrderId:   entry.OrderId,
//				Symbol:    entry.Symbol,
//				Price:     entry.Price,
//				Qty:       entry.Qty,
//				LeavesQty: entry.LeavesQty,
//				Status:    orderStatus,
//				Side:      model.ParseOrderSide(entry.Side),
//				CreatedAt: millisecondsToTime(entry.CreatedTime),
//				UpdatedAt: millisecondsToTime(entry.UpdatedTime),
//				Type:      model.ParseOrderType(entry.Type),
//			}, nil
//		}
//	}
//	return OrderInfo{}, nil
//}
//
//func (ba *Adapter) fetchPrecisionFromAPI(
//	ctx context.Context,
//	symbol string,
//) (float64, float64, error) {
//	params := map[string]interface{}{
//		"symbol":   symbol,
//		"category": "spot",
//	}
//
//	response, err := ba.BybitClient.NewUtaBybitServiceWithParams(params).GetInstrumentInfo(ctx)
//	if err != nil {
//		return 0, 0, fmt.Errorf("API request failed: %w", err)
//	}
//	if response.RetCode != 0 {
//		return 0, 0, fmt.Errorf(
//			"API error. Code: %d, Message: %s",
//			response.RetCode,
//			response.RetMsg,
//		)
//	}
//	// Unmarshal Result into InstrumentResult
//	var instrumentResult InstrumentResult
//	resultData, err := json.Marshal(response.Result)
//	if err != nil {
//		return 0, 0, fmt.Errorf("failed to marshal result: %w", err)
//	}
//
//	err = json.Unmarshal(resultData, &instrumentResult)
//	if err != nil {
//		return 0, 0, fmt.Errorf("failed to unmarshal result: %w", err)
//	}
//
//	for _, entry := range instrumentResult.List {
//		if entry.Symbol == symbol {
//			// Calculate precision for Qty
//			qtyPrec, err := strconv.ParseFloat(
//				strings.TrimSpace(entry.LotSizeFilter.BasePrecision),
//				64,
//			)
//			if err != nil {
//				return 0, 0, fmt.Errorf("invalid basePrecision format: %w", err)
//			}
//
//			// Calculate precision for Price
//			pricePrec, err := strconv.ParseFloat(strings.TrimSpace(entry.PriceFilter.TickSize), 64)
//			if err != nil {
//				return 0, 0, fmt.Errorf("invalid tickSize format: %w", err)
//			}
//
//			return pricePrec, qtyPrec, nil
//		}
//	}
//
//	return 0, 0, fmt.Errorf("symbol %s not found in response", symbol)
//}
//
//func (ba *Adapter) createKlineTopics(symbols []string, interval int) []string {
//	var klineTopics []string
//	for _, symbol := range symbols {
//		klineTopics = append(klineTopics, constructKlineTopic(symbol, interval))
//	}
//	return klineTopics
//}
//
//func (ba *Adapter) getOrderStatusCacheDuration() time.Duration {
//	return ba.cfg.Defaults.OrderStatusCacheDuration
//}
//
//func constructKlineTopic(symbol string, klineInterval int) string {
//	return "kline." + strconv.Itoa(klineInterval) + "." + symbol
//}
//
//// removeUSDTSuffix removes the "USDT" suffix from the symbol if present.
//// If the symbol doesn't end with "USDT", it returns the symbol unchanged.
//func removeUSDTSuffix(symbol string) string {
//	const suffix = "USDT"
//	if strings.HasSuffix(symbol, suffix) {
//		return strings.TrimSuffix(symbol, suffix)
//	}
//	return symbol
//}
//
//// parseOrderStatus converts api response to OrderStatus map
//func parseOrderStatus(status string) (model.OrderStatus, bool) {
//	enumStatus, ok := orderStatusMap[status]
//	return enumStatus, ok
//}
//
//func chunkSymbols(symbols []string, chunkSize int) [][]string {
//	var allChunks [][]string
//
//	for i := 0; i < len(symbols); i += chunkSize {
//		end := i + chunkSize
//		if end > len(symbols) {
//			end = len(symbols)
//		}
//
//		allChunks = append(allChunks, symbols[i:end])
//	}
//
//	return allChunks
//}
//
//// determineTimeInterval determines for a given interval how much time needed to get n number of latest Klines
//// it returns startTime and endTime in milliseconds since the epoch.
//func determineTimeInterval(n int, interval int) (int64, int64) {
//	now := time.Now()
//	pastDuration := time.Duration(n*interval) * time.Minute
//	pastTime := now.Add(-pastDuration)
//
//	return pastTime.UnixMilli(), now.UnixMilli()
//}
//
//func buildUrl(host string, path string, params map[string]string) string {
//	baseUrl, err := url.Parse(host)
//
//	if err != nil {
//		log.Fatal(err)
//	}
//	baseUrl.Path, _ = url.JoinPath(baseUrl.Path, path)
//
//	queryParams := url.Values{}
//	for k, v := range params {
//		queryParams.Add(k, v)
//	}
//	baseUrl.RawQuery = queryParams.Encode()
//
//	return baseUrl.String()
//}
//
//// reverse given float64 numbers slice this function reverses every value in place
//func reverse(numbers []float64) {
//	for i, j := 0, len(numbers)-1; i < j; i, j = i+1, j-1 {
//		numbers[i], numbers[j] = numbers[j], numbers[i]
//	}
//}
//
//// millisecondsToTime convert a timestamp in milliseconds into a Time object
//func millisecondsToTime(ms int) time.Time {
//	return time.Unix(0, int64(ms)*int64(time.Millisecond))
//}
