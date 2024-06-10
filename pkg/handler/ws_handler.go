package handler

import (
	"encoding/json"
	"fmt"
	"github.com/nihadazimli/crypbot/pkg/model"
	"log"
	"sync"
)

var (
	klineDataMap = make(map[string][]float64)
	RSIDataMap   = make(map[string]float64)
	RSIReady     = make(chan string, 5)
	mapMutex     sync.Mutex
)

func Receive(message []byte) {
	var WSKline model.WSRawKline
	err := json.Unmarshal(message, &WSKline)
	if err != nil {
		log.Println("Failed to unmarshal JSON:", err)
		return
	}
	mapMutex.Lock()
	defer mapMutex.Unlock()
	for _, kline := range WSKline.Data {
		if !(kline.Confirm) {
			if _, exists := klineDataMap[WSKline.Topic]; !exists {
				klineDataMap[WSKline.Topic] = make([]float64, 0, 16)
			}
			klineData := append(klineDataMap[WSKline.Topic], kline.Close)

			if len(klineData) > 15 {
				klineData = klineData[len(klineData)-15:]
				klineDataMap[WSKline.Topic] = klineData
				RSIReady <- WSKline.Topic
			}
			klineDataMap[WSKline.Topic] = klineData

		}
	}
}

func CalculateRSI() {

	for topic := range RSIReady {
		mapMutex.Lock()
		klineData, exists := klineDataMap[topic]
		mapMutex.Unlock()
		if !exists || len(klineData) < 15 {
			log.Println("Insufficient data to calculate RSI for topic:", topic)
			continue
		}

		rsi := calculateRSI(klineData, 14)
		mapMutex.Lock()
		RSIDataMap[topic] = rsi
		fmt.Println("RSIDataMap", RSIDataMap) // Temporary for now to see if RSI calculated
		mapMutex.Unlock()
	}
}

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

//func Show() {
//	for {
//		mapMutex.Lock()
//		fmt.Print("\033[H\033[2J") // Move cursor to home and clear the screen
//		for topic, prices := range klineDataMap {
//			fmt.Printf("%s: %v\n", topic, prices) // Print each topic and its prices on a new line
//		}
//		mapMutex.Unlock()
//		time.Sleep(1 * time.Second) // Update frequency; adjust as needed
//	}
//}
