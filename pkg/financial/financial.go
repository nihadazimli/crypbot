package financial

import (
	"github.com/nihadazimli/crypbot/pkg/model"
	"log"
	"sync"
)

var (
	mapMutex sync.Mutex
)

type Client struct {
	klineDataMap  map[string][]float64
	RSIDataMap    map[string]float64
	rsiReady      chan string
	buyReady      chan string
	klineReceived chan model.InternalKline
}

func NewFinancialClient(klineReceived chan model.InternalKline, rsiReady chan string, buyReady chan string) *Client {
	return &Client{
		klineDataMap:  make(map[string][]float64),
		RSIDataMap:    make(map[string]float64),
		klineReceived: klineReceived,
		rsiReady:      rsiReady,
		buyReady:      buyReady,
	}
}

func (fc *Client) ProcessKline() {

	for klineDatapoint := range fc.klineReceived {
		if !(klineDatapoint.Confirm) {
			if _, exists := fc.klineDataMap[klineDatapoint.Topic]; !exists {
				fc.klineDataMap[klineDatapoint.Topic] = make([]float64, 0, 16)
			}
			klineData := append(fc.klineDataMap[klineDatapoint.Topic], klineDatapoint.Close)
			mapMutex.Lock()
			if len(klineData) > 15 {
				klineData = klineData[len(klineData)-15:]
				fc.klineDataMap[klineDatapoint.Topic] = klineData
				fc.rsiReady <- klineDatapoint.Topic
			}
			fc.klineDataMap[klineDatapoint.Topic] = klineData
			mapMutex.Unlock()

		}
	}
}

func (fc *Client) CalculateRSI() {

	for topic := range fc.rsiReady {
		mapMutex.Lock()
		klineData, exists := fc.klineDataMap[topic]
		mapMutex.Unlock()
		if !exists || len(klineData) < 15 {
			log.Println("Insufficient data to calculate RSI for topic:", topic)
			continue
		}

		rsi := calculateRSI(klineData, 14)
		mapMutex.Lock()
		fc.RSIDataMap[topic] = rsi
		mapMutex.Unlock()
		if rsi <= 20.0 {
			fc.buyReady <- topic
		}
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

	changes := make([]float64, len(prices)-1)
	for i := 1; i < len(prices); i++ {
		changes[i-1] = prices[i] - prices[i-1]
	}

	gains := make([]float64, len(changes))
	losses := make([]float64, len(changes))
	for i, change := range changes {
		if change > 0 {
			gains[i] = change
		} else {
			losses[i] = -change
		}
	}

	avgGain := ema(gains, n)
	avgLoss := ema(losses, n)

	// We only need the last RSI value
	if avgLoss[len(avgLoss)-1] == 0 {
		if avgGain[len(avgGain)-1] > 0 {
			return 100 // Avoid division by zero and define RSI as 100 when there are no losses but gains
		}
		return 0 // No gains and no losses
	}

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
