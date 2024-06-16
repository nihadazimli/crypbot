package financial

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nihadazimli/crypbot/internal/config"
	"github.com/nihadazimli/crypbot/pkg/model"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	mapMutex sync.Mutex
)

const (
	RequiredKlineThreshold = 15
	DefaultKlineInterval   = 15
)

type Engine struct {
	httpClient    *http.Client
	cfg           *config.Config
	KlineDataMap  map[string][]float64
	RSIDataMap    map[string]float64
	rsiReady      chan string
	buyReady      chan string
	klineReceived chan model.InternalKline
}

// NewFinancialEngine creates a new Engine instance with specified channels and HTTP client timeout.
func NewFinancialEngine(klineReceived chan model.InternalKline, rsiReady chan string, buyReady chan string, httpTimeoutSec int) *Engine {
	return &Engine{
		httpClient:    &http.Client{Timeout: time.Duration(httpTimeoutSec) * time.Second},
		cfg:           config.GetConfig(),
		KlineDataMap:  make(map[string][]float64),
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
		if _, exists := fe.KlineDataMap[klineDatapoint.Topic]; !exists {
			fe.KlineDataMap[klineDatapoint.Topic] = make([]float64, 0, 16)
		}

		klineData := append(fe.KlineDataMap[klineDatapoint.Topic], klineDatapoint.Close)

		mapMutex.Lock()
		if len(klineData) > RequiredKlineThreshold {
			// Keep only latest RequiredKlineThreshold number of records
			klineData = klineData[len(klineData)-RequiredKlineThreshold:]
			fe.KlineDataMap[klineDatapoint.Topic] = klineData
			fe.rsiReady <- klineDatapoint.Topic
		}

		fe.KlineDataMap[klineDatapoint.Topic] = klineData
		mapMutex.Unlock()

	}
}

func (fe *Engine) ProcessRSI() {
	for topic := range fe.rsiReady {
		rsi, err := fe.calculatedAndStoreRSI(topic)
		if err != nil {
			log.Println("Error calculating RSI:", err)
			continue
		}
		fe.evaluateRSI(topic, rsi)
	}
}

func (fe *Engine) calculatedAndStoreRSI(topic string) (float64, error) {
	mapMutex.Lock()
	klineData, exists := fe.KlineDataMap[topic]
	mapMutex.Unlock()

	if canCalculateRSI(exists, klineData) {
		return 0, errors.New("insufficient data to calculate RSI for topic: " + topic)
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

	return rsi, nil
}

func (fe *Engine) evaluateRSI(topic string, rsi float64) {
	if rsi <= 20.0 {
		fe.buyReady <- topic
	}
}

// FetchRecentKlines retrieves most recent n klines for a given trading symbol from API
func (fe *Engine) FetchRecentKlines(symbol string, n int) []float64 {

	startTime, endTime := determineTimeInterval(n, fe.cfg.MarketConstants.KlineInterval)

	params := map[string]string{
		"symbol":   symbol,
		"category": fe.cfg.MarketConstants.KLineCategory,
		"interval": strconv.Itoa(fe.cfg.MarketConstants.KlineInterval),
		"start":    strconv.FormatInt(startTime, 10),
		"end":      strconv.FormatInt(endTime, 10),
		"limit":    strconv.Itoa(fe.cfg.MarketConstants.KlineInterval),
	}

	requestUrl := buildUrl(fe.cfg.Broker.API.Host, fe.cfg.Broker.API.RequestPaths.Kline, params)
	fmt.Println("building requestUrl:", requestUrl)
	req, err := http.NewRequest("GET", requestUrl, nil)
	if err != nil {
		log.Fatal("Can't create request for kline: ", err)
	}

	response, err := fe.httpClient.Do(req)
	if err != nil {
		log.Fatal("Can't do request for kline: ", err)
	}

	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)

	// Parse the JSON response
	var apiResponse model.KlineApiResponse
	if err := json.Unmarshal(body, &apiResponse); err != nil {
		log.Fatal("JSON Unmarshaling failed: ", err)
	}

	// Extract the 5th value (close price) from each list
	var closePrices []float64
	for _, item := range apiResponse.Result.List {
		closePrice, _ := strconv.ParseFloat(strings.TrimSpace(item[4]), 64)
		closePrices = append(closePrices, closePrice)
	}
	return closePrices

}

//func Show() {
//	for {
//		mapMutex.Lock()
//
//		fmt.Print("\033[H\033[2J") // Move cursor to home and clear the screen
//		for topic, prices := range fc.KlineDataMap {
//			fmt.Printf("%s: %v\n", topic, prices) // Print each topic and its prices on a new line
//		}
//		mapMutex.Unlock()
//		time.Sleep(1 * time.Second) // Update frequency; adjust as needed
//	}
//}

func DebugCalculateRSI(prices []float64) float64 {
	return calculateRSI(prices, 14)
}
func buildUrl(host string, path string, params map[string]string) string {
	baseUrl, err := url.Parse(host)

	if err != nil {
		log.Fatal(err)
	}
	baseUrl.Path, _ = url.JoinPath(baseUrl.Path, path)

	queryParams := url.Values{}
	for k, v := range params {
		queryParams.Add(k, v)
	}
	baseUrl.RawQuery = queryParams.Encode()

	return baseUrl.String()
}

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

// determineTimeInterval determines for a given interval how much time needed to get n number of latest Klines
// it returns startTime and endTime in milliseconds since the epoch.
func determineTimeInterval(n int, interval int) (int64, int64) {
	now := time.Now()
	pastDuration := time.Duration(n*interval) * time.Minute
	pastTime := now.Add(-pastDuration)

	return pastTime.UnixMilli(), now.UnixMilli()
}
