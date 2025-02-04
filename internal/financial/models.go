package financial

type CoinMetrics struct {
	ClosePriceClosed  []float64
	VolumeValueClosed []float64
	RSIClosed         float64
	RSILive           float64
	VolumeValueMedian float64
}

type MovingAverage struct {
	MA100 float64
	MA50  float64
}

type Precision struct {
	Qty   float64
	Price float64
}

type WSInternalKline struct {
	Symbol  string
	Close   float64
	Confirm bool
	Volume  float64
}
