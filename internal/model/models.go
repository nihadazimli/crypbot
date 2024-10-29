package model

import (
	"encoding/json"
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
	OrderID     string `json:"orderId"`
	Qty         string `json:"qty"`
	CumExecQty  string `json:"cumExecQty"`
	AvgPrice    string `json:"avgPrice"`
	OrderStatus string `json:"orderStatus"`
	Symbol      string `json:"symbol"`
	LeavesQty   string `json:"leavesQty"`
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
	Symbol  string
	Close   float64
	Confirm bool
	Volume  float64
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
}

type Precision struct {
	Qty   int
	Price int
}
