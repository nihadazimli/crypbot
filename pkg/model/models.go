package model

type WSRawKline struct {
	Data  []KlineData `json:"data"`
	Topic string      `json:"topic"`
	Ts    int64       `json:"ts"`
	Type  string      `json:"type"`
}

type KlineData struct {
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

type InternalKline struct {
	Topic   string
	Close   float64
	Confirm bool
}
