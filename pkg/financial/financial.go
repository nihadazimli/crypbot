package financial

import (
	"encoding/json"
	"errors"
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

type Engine struct {
	httpClient      *http.Client
	cfg             *config.Config
	KlineDataClosed map[string][]float64
	RSIDataClosed   map[string]float64
	RSIDataLive     map[string]float64
	rsiReady        chan string
	buyReady        chan string
	klineReceived   chan model.InternalKline
}

// NewFinancialEngine creates a new Engine instance with specified channels and HTTP client timeout.
func NewFinancialEngine(klineReceived chan model.InternalKline, rsiReady chan string, buyReady chan string, httpTimeoutSec int) *Engine {
	return &Engine{
		httpClient:      &http.Client{Timeout: time.Duration(httpTimeoutSec) * time.Second},
		cfg:             config.GetConfig(),
		KlineDataClosed: make(map[string][]float64),
		RSIDataClosed:   make(map[string]float64),
		RSIDataLive:     make(map[string]float64),
		klineReceived:   klineReceived,
		rsiReady:        rsiReady,
		buyReady:        buyReady,
	}
}

func Start(klineReceived chan model.InternalKline, rsiReady chan string, buyReady chan string, timeout int) *Engine {
	fe := NewFinancialEngine(klineReceived, rsiReady, buyReady, timeout)
	fe.loadRecentKlineData()

	return fe
}

func (fe *Engine) ProcessKline() {

	for klineDatapoint := range fe.klineReceived {
		if klineDatapoint.Confirm {
			// Processing closed Kline data
			if _, exists := fe.KlineDataClosed[klineDatapoint.Symbol]; !exists {
				fe.KlineDataClosed[klineDatapoint.Symbol] = make([]float64, 0, +1)
			}

			klineData := append(fe.KlineDataClosed[klineDatapoint.Symbol], klineDatapoint.Close)

			mapMutex.Lock()
			if len(klineData) > fe.cfg.MarketConstants.RSIWindowSize {
				// Keep only latest RequiredKlineThreshold number of records
				klineData = klineData[len(klineData)-fe.cfg.MarketConstants.RSIWindowSize:]
				fe.KlineDataClosed[klineDatapoint.Symbol] = klineData
				fe.rsiReady <- klineDatapoint.Symbol
			}

			fe.KlineDataClosed[klineDatapoint.Symbol] = klineData
			mapMutex.Unlock()
		} else {
			// Processing live Kline data
			if closedData, ok := fe.KlineDataClosed[klineDatapoint.Symbol]; ok && len(closedData) == fe.cfg.MarketConstants.RSIWindowSize {
				// Calculate live RSI using the last closed data points plus a live data point
				liveData := closedData[1:]
				liveData = append(liveData, klineDatapoint.Close) // Append live data point
				fe.RSIDataLive[klineDatapoint.Symbol] = calculateRSI(liveData, fe.cfg.MarketConstants.KlineInterval-1)
			}

		}
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
	klineData, exists := fe.KlineDataClosed[topic]
	mapMutex.Unlock()

	if canCalculateRSI(exists, klineData, fe.cfg.MarketConstants.RSIWindowSize) {
		return 0, errors.New("insufficient data to calculate RSI for topic: " + topic)
	}

	rsi := calculateRSI(klineData, fe.cfg.MarketConstants.KlineInterval-1) //To get only latest RSI

	mapMutex.Lock()
	newMap := make(map[string]float64)
	for k, v := range fe.RSIDataClosed {
		newMap[k] = v
	}
	newMap[topic] = rsi
	fe.RSIDataClosed = newMap
	mapMutex.Unlock()

	return rsi, nil
}

func (fe *Engine) evaluateRSI(topic string, rsi float64) {
	if rsi <= fe.cfg.Trader.RSIBuyValue {
		fe.buyReady <- topic
	}
}

// FetchRecentKlines retrieves most recent n closed klines for a given trading symbol from API
func (fe *Engine) FetchRecentKlines(symbol string, n int) ([]float64, error) {

	startTime, endTime := determineTimeInterval(n, fe.cfg.MarketConstants.KlineInterval)

	params := map[string]string{
		"symbol":   symbol,
		"category": fe.cfg.MarketConstants.KLineCategory,
		"interval": strconv.Itoa(fe.cfg.MarketConstants.KlineInterval),
		"start":    strconv.FormatInt(startTime, 10),
		"end":      strconv.FormatInt(endTime, 10),
		"limit":    strconv.Itoa(n + 1),
	}

	requestUrl := buildUrl(fe.cfg.Broker.API.Host, fe.cfg.Broker.API.RequestPaths.Kline, params)
	req, err := http.NewRequest("GET", requestUrl, nil)
	if err != nil {
		return nil, err
	}

	response, err := fe.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)

	if err != nil {
		return nil, err
	}

	// Parse the JSON response
	var apiResponse model.KlineApiResponse
	if err := json.Unmarshal(body, &apiResponse); err != nil {
		return nil, err
	}

	// Extract the 5th value (close price) from each list
	var closePrices []float64
	for _, item := range apiResponse.Result.List {
		closePrice, _ := strconv.ParseFloat(strings.TrimSpace(item[4]), 64)
		closePrices = append(closePrices, closePrice)
	}
	reverse(closePrices)
	// Eliminate live value, just return most recent closed values
	return closePrices[:len(closePrices)-1], nil

}

func (fe *Engine) loadRecentKlineData() {
	for _, symbol := range fe.cfg.Symbols {
		closePrices, err := fe.FetchRecentKlines(symbol, fe.cfg.MarketConstants.RSIWindowSize)
		if err != nil {
			log.Println("Error fetching recent kline data for", symbol, ":", err)
		}
		fe.KlineDataClosed[symbol] = closePrices

	}
}

//func (fe *Engine) Show() {
//	for {
//		mapMutex.Lock()
//
//		fmt.Print("\033[H\033[2J") // Move cursor to home and clear the screen
//		for topic, prices := range fe.RSIDataClosed {
//			fmt.Printf("%s: %v\n", topic, prices) // Print each topic and its prices on a new line
//		}
//		mapMutex.Unlock()
//		time.Sleep(1 * time.Second) // Update frequency; adjust as needed
//	}
//}
//
//func DebugCalculateRSI(prices []float64, n int) float64 {
//	return calculateRSI(prices, n)
//}

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

func canCalculateRSI(exists bool, klineData []float64, requiredKlineThreshold int) bool {
	return !exists || len(klineData) < requiredKlineThreshold
}

// determineTimeInterval determines for a given interval how much time needed to get n number of latest Klines
// it returns startTime and endTime in milliseconds since the epoch.
func determineTimeInterval(n int, interval int) (int64, int64) {
	now := time.Now()
	pastDuration := time.Duration(n*interval) * time.Minute
	pastTime := now.Add(-pastDuration)

	return pastTime.UnixMilli(), now.UnixMilli()
}

// reverse given float64 numbers slice this function reverses every value in place
func reverse(numbers []float64) {
	for i, j := 0, len(numbers)-1; i < j; i, j = i+1, j-1 {
		numbers[i], numbers[j] = numbers[j], numbers[i]
	}
}
