package model

type WSRawKline struct {
	Data  []WSKlineData `json:"data"`
	Topic string        `json:"topic"`
	Ts    int64         `json:"ts"`
	Type  string        `json:"type"`
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
