// Package model includes every model used in the code
package model

import (
	"encoding/json"
	"fmt"
	"strings"
)

type WSRawKline struct {
	Data  []WSKlineData `json:"data"`
	Topic string        `json:"topic"`
	Ts    int64         `json:"ts"`
	Type  string        `json:"type"`
}

type BybitWSPrivateMessage struct {
	Id           string          `json:"id"`
	Topic        string          `json:"topic"`
	CreationTime int             `json:"creationTime"`
	Data         json.RawMessage `json:"data"`
}

type BybitWSOrderData struct {
	OrderID     string  `json:"orderId"`
	Qty         float64 `json:"qty,string"`
	CumExecQty  float64 `json:"cumExecQty,string"`
	Price       float64 `json:"price,string"`
	Status      string  `json:"orderStatus"`
	Symbol      string  `json:"symbol"`
	LeavesQty   float64 `json:"leavesQty,string"`
	Side        string  `json:"side"`
	Type        string  `json:"orderType"`
	CreatedTime int     `json:"createdTime,string"`
	UpdatedTime int     `json:"updatedTime,string"`
}

type WSKlineData struct {
	Open      float64 `json:"open,string"`
	Close     float64 `json:"close,string"`
	High      float64 `json:"high,string"`
	Low       float64 `json:"low,string"`
	Volume    float64 `json:"volume,string"`
	Turnover  float64 `json:"turnover,string"`
	Start     int64   `json:"start"`
	End       int64   `json:"end"`
	Timestamp int64   `json:"timestamp"`
	Confirm   bool    `json:"confirm"`
	Interval  string  `json:"interval"`
}

// WSInternalKline is filtered kline data from websocket to internal logic for processing
type WSInternalKline struct {
	Symbol   string
	Close    float64
	IsClosed bool
	Volume   float64
}

type MarketEvent struct {
	Symbol    string
	Price     float64
	RSI       float64
	IsClosed  bool
	Timestamp float64
	ZScore20  float64
}

type KlineApiResponse struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Result  struct {
		Symbol   string     `json:"symbol"`
		Category string     `json:"category"`
		List     [][]string `json:"list"`
	} `json:"result"`
}

type CoinMetrics struct {
	ClosePriceClosed  []float64
	VolumeValueClosed []float64
	RSIClosed         float64
	RSILive           float64
	VolumeValueMedian float64
	ZScore20          float64
}

type Precision struct {
	Qty   float64
	Price float64
}

type MovingAverage struct {
	MA100 float64
	MA50  float64
}
type ConditionChecker func(symbol string) OrderConditionResult

type OrderType string

const (
	OrderTypeMarket  OrderType = "Market"
	OrderTypeLimit   OrderType = "Limit"
	OrderTypeUnknown OrderType = "N/A"
)

type OrderConditionResult int

const (
	ConditionMet OrderConditionResult = iota
	ConditionWait
	ConditionCancel
)

func ParseOrderType(orderType string) OrderType {
	switch strings.ToUpper(orderType) {
	case "MARKET":
		return OrderTypeMarket
	case "LIMIT":
		return OrderTypeLimit
	default:
		return OrderTypeUnknown
	}
}

type OrderSide string

const (
	OrderSideBuy     OrderSide = "Buy"
	OrderSideSell    OrderSide = "Sell"
	OrderSideUnknown OrderSide = "N/A"
)

func ParseOrderSide(side string) OrderSide {
	switch strings.ToUpper(side) {
	case "BUY":
		return OrderSideBuy
	case "SELL":
		return OrderSideSell
	default:
		return OrderSideUnknown
	}
}

type OrderStatus int

const (
	OrderInvalid OrderStatus = iota
	OrderFilled
	OrderPartiallyFilled
	OrderCanceled
	OrderOpen
	OrderPartiallyFilledCancelled
)

func (o OrderStatus) String() string {
	names := map[OrderStatus]string{
		OrderInvalid:                  "Invalid",
		OrderFilled:                   "Filled",
		OrderPartiallyFilled:          "PartiallyFilled",
		OrderCanceled:                 "Canceled",
		OrderOpen:                     "Open",
		OrderPartiallyFilledCancelled: "PartiallyFilledCancelled",
	}
	if name, ok := names[o]; ok {
		return name
	}
	return fmt.Sprintf("Unknown(%d)", o)
}
func (o OrderStatus) IsFinished() bool {
	switch o {
	case OrderFilled, OrderCanceled, OrderPartiallyFilledCancelled:
		return true
	default:
		return false
	}
}

type StrategyID string

const (
	StrategyRSI       StrategyID = "rsi"
	StrategyBollinger StrategyID = "bollinger"
	StrategyDummy     StrategyID = "dummy"
	StrategyUnknown   StrategyID = "N/A"
)

func ParseStrategyID(strategy string) StrategyID {
	switch strings.ToUpper(strategy) {
	case "RSI":
		return StrategyRSI
	case "BOLLINGER":
		return StrategyBollinger
	case "DUMMY":
		return StrategyDummy
	default:
		return StrategyUnknown
	}
}
