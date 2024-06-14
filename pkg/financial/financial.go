package financial

import (
	"github.com/nihadazimli/crypbot/pkg/model"
	"log"
	"sync"
)

var (
	mapMutex sync.Mutex
)

const (
	RequiredKlineThreshold = 15
)

type Engine struct {
	klineDataMap  map[string][]float64
	RSIDataMap    map[string]float64
	rsiReady      chan string
	buyReady      chan string
	klineReceived chan model.InternalKline
}

func NewFinancialEngine(klineReceived chan model.InternalKline, rsiReady chan string, buyReady chan string) *Engine {
	return &Engine{
		klineDataMap:  make(map[string][]float64),
		RSIDataMap:    make(map[string]float64),
		klineReceived: klineReceived,
		rsiReady:      rsiReady,
		buyReady:      buyReady,
	}
}

func (fe *Engine) ProcessKline() {

	for klineDatapoint := range fe.klineReceived {
		if klineDatapoint.Confirm {
			continue // Closed Kline logic is not implemented yet
		}
		// Calculate Live kline for debugging for now
		if _, exists := fe.klineDataMap[klineDatapoint.Topic]; !exists {
			fe.klineDataMap[klineDatapoint.Topic] = make([]float64, 0, 16)
		}

		klineData := append(fe.klineDataMap[klineDatapoint.Topic], klineDatapoint.Close)

		mapMutex.Lock()
		if len(klineData) > RequiredKlineThreshold {
			// Keep only latest RequiredKlineThreshold number of records
			klineData = klineData[len(klineData)-RequiredKlineThreshold:]
			fe.klineDataMap[klineDatapoint.Topic] = klineData
			fe.rsiReady <- klineDatapoint.Topic
		}

		fe.klineDataMap[klineDatapoint.Topic] = klineData
		mapMutex.Unlock()

	}
}

func (fe *Engine) ProcessRSI() {
	for topic := range fe.rsiReady {
		if rsi, ok := fe.calculatedAndStoreRSI(topic); ok {
			fe.evaluateRSI(topic, rsi)
		}
	}
}

func (fe *Engine) calculatedAndStoreRSI(topic string) (float64, bool) {
	mapMutex.Lock()
	klineData, exists := fe.klineDataMap[topic]
	mapMutex.Unlock()

	if canCalculateRSI(exists, klineData) {
		log.Println("Insufficient data to calculate RSI for topic:", topic)
		return 0, false
	}

	rsi := calculateRSI(klineData, RequiredKlineThreshold-1) //To get only latest RSI

	mapMutex.Lock()
	newMap := make(map[string]float64)
	for k, v := range fe.RSIDataMap {
		newMap[k] = v
	}
	newMap[topic] = rsi
	fe.RSIDataMap = newMap
	mapMutex.Unlock()

	return rsi, true
}

func (fe *Engine) evaluateRSI(topic string, rsi float64) {
	if rsi <= 20.0 {
		fe.buyReady <- topic
	}
}

//func Show() {
//	for {
//		mapMutex.Lock()
//
//		fmt.Print("\033[H\033[2J") // Move cursor to home and clear the screen
//		for topic, prices := range fc.klineDataMap {
//			fmt.Printf("%s: %v\n", topic, prices) // Print each topic and its prices on a new line
//		}
//		mapMutex.Unlock()
//		time.Sleep(1 * time.Second) // Update frequency; adjust as needed
//	}
//}

func calculateRSI(prices []float64, n int) float64 {
	if len(prices) < n+1 {
		return 0 // Not enough data to compute RSI
	}

	//Compute changes for gains and losses
	changes := make([]float64, len(prices)-1)
	for i := 1; i < len(prices); i++ {
		changes[i-1] = prices[i] - prices[i-1]
	}

	// We need gains and losses to calculate RSI, if gain or loss smaller than 0 we take it as 0
	gains := make([]float64, len(changes))
	losses := make([]float64, len(changes))
	for i, change := range changes {
		if change > 0 {
			gains[i] = change
			losses[i] = 0.0
		} else {
			losses[i] = -change
			gains[i] = 0.0
		}
	}

	//Calculate exponential moving average for gains and losses
	avgGain := ema(gains, n)
	avgLoss := ema(losses, n)

	// We only need the last RSI value
	if avgLoss[len(avgLoss)-1] == 0 {
		if avgGain[len(avgGain)-1] > 0 {
			return 100 // Avoid division by zero and define RSI as 100 when there are no losses but gains
		}
		return 0 // No gains and no losses
	}

	// Calculate RSI
	rs := avgGain[len(avgGain)-1] / avgLoss[len(avgLoss)-1]
	rsi := 100 - (100 / (1 + rs))

	return rsi
}

// ema calculates the Exponential Moving Average for a slice of float64.
// It returns the full series of EMA so that the calling function can select the last one.
func ema(values []float64, n int) []float64 {
	alpha := 1.0 / float64(n)
	emaValues := make([]float64, len(values))
	emaValues[0] = values[0] // Initialize the first EMA value to the first price

	for i := 1; i < len(values); i++ {
		emaValues[i] = values[i]*alpha + emaValues[i-1]*(1-alpha)
	}

	return emaValues
}

func canCalculateRSI(exists bool, klineData []float64) bool {
	return !exists || len(klineData) < RequiredKlineThreshold
}
