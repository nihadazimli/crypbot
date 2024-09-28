package financial

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nihadazimli/crypbot/internal/broker_api"
	"github.com/nihadazimli/crypbot/internal/config"
	"github.com/nihadazimli/crypbot/pkg/model"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	mapMutex sync.Mutex
)

type EngineInterface interface {
	ProcessKline()
	GetRSIClosed(symbol string) (float64, bool)
	GetRSILive(symbol string) (float64, bool)
	GetVolumeValueMedian(symbol string) (float64, bool)
	GetCurrentPrice(symbol string) (float64, bool)
	ProcessRSI()
	Show()
}

type Engine struct {
	httpClient          *http.Client
	cfg                 *config.Config
	broker              broker_api.BrokerAPI
	closedKlineReceived chan string
	buyReady            chan string
	klineReceived       chan model.WSInternalKline
	Coins               map[string]*model.CoinMetrics
}

// NewFinancialEngine creates a new Engine instance with specified channels and HTTP client timeout.
func NewFinancialEngine(cfg *config.Config, broker broker_api.BrokerAPI, klineReceived chan model.WSInternalKline, closedKlineReceived chan string, buyReady chan string, httpTimeoutSec int) *Engine {
	return &Engine{
		httpClient:          &http.Client{Timeout: time.Duration(httpTimeoutSec) * time.Second},
		cfg:                 cfg,
		Coins:               make(map[string]*model.CoinMetrics),
		klineReceived:       klineReceived,
		closedKlineReceived: closedKlineReceived,
		buyReady:            buyReady,
		broker:              broker,
	}
}

func Start(ctx context.Context, broker broker_api.BrokerAPI, closedKlineReceived chan string, buyReady chan string, timeout int) *Engine {
	cfg := config.GetConfig()
	klineReceived := make(chan model.WSInternalKline)

	fe := NewFinancialEngine(cfg, broker, klineReceived, closedKlineReceived, buyReady, timeout)
	fe.loadRecentKlineData()
	err := fe.broker.StartKlineStream(ctx, cfg.KlineTopics, klineReceived)
	if err != nil {
		log.Printf("Cannot start KlineStream. Err: %v", err)
	}
	return fe
}

func (fe *Engine) ProcessKline() {
	for klineDatapoint := range fe.klineReceived {
		if klineDatapoint.Confirm {
			fe.processClosedKline(klineDatapoint)
		} else {
			fe.processLiveKline(klineDatapoint)
		}
	}
}

func (fe *Engine) GetRSIClosed(symbol string) (float64, bool) {
	if metrics, exists := fe.Coins[symbol]; exists {
		return metrics.RSIClosed, true
	}
	return fe.getDefaultMissingDataValue(), false
}

func (fe *Engine) GetRSILive(symbol string) (float64, bool) {
	if metrics, exists := fe.Coins[symbol]; exists {
		return metrics.RSILive, true
	}
	return fe.getDefaultMissingDataValue(), false
}

func (fe *Engine) GetVolumeValueMedian(symbol string) (float64, bool) {
	if metrics, exists := fe.Coins[symbol]; exists {
		return metrics.VolumeValueMedian, true
	}
	return fe.getDefaultMissingDataValue(), false
}

func (fe *Engine) GetCurrentPrice(symbol string) (float64, bool) {
	if metrics, exists := fe.Coins[symbol]; exists {
		return metrics.ClosePriceClosed[len(metrics.ClosePriceClosed)-1], true
	}
	return fe.getDefaultMissingDataValue(), false
}

func (fe *Engine) processClosedKline(klineDatapoint model.WSInternalKline) {
	coinMetrics := fe.getOrCreateCoinMetrics(klineDatapoint.Symbol)

	mapMutex.Lock()
	defer mapMutex.Unlock()

	// Append new closed prices and volumes to their respective slices
	coinMetrics.ClosePriceClosed = append(coinMetrics.ClosePriceClosed, klineDatapoint.Close)
	coinMetrics.VolumeValueClosed = append(coinMetrics.VolumeValueClosed, klineDatapoint.Close*klineDatapoint.Volume)

	// Ensure the slices do not exceed the defined window size
	if len(coinMetrics.ClosePriceClosed) > fe.cfg.MarketConstants.MetricsWindowSize {
		coinMetrics.ClosePriceClosed = coinMetrics.ClosePriceClosed[len(coinMetrics.ClosePriceClosed)-fe.cfg.MarketConstants.MetricsWindowSize:]
		coinMetrics.VolumeValueClosed = coinMetrics.VolumeValueClosed[len(coinMetrics.VolumeValueClosed)-fe.cfg.MarketConstants.MetricsWindowSize:]

		fe.closedKlineReceived <- klineDatapoint.Symbol
	}
}

func (fe *Engine) processLiveKline(klineDatapoint model.WSInternalKline) {
	coinMetrics, ok := fe.Coins[klineDatapoint.Symbol]
	if ok && len(coinMetrics.ClosePriceClosed) == fe.cfg.MarketConstants.MetricsWindowSize {
		// Construct a dataset that includes the last MetricsWindowSize-1 closed prices and the live price
		liveData := append(coinMetrics.ClosePriceClosed[1:], klineDatapoint.Close)
		coinMetrics.RSILive = calculateRSI(liveData, fe.cfg.MarketConstants.KlineInterval-1)
	}
}

// getOrCreateCoinMetrics ensures that each coin's metric structure is initialized before use
func (fe *Engine) getOrCreateCoinMetrics(symbol string) *model.CoinMetrics {
	mapMutex.Lock()
	defer mapMutex.Unlock()

	metrics, exists := fe.Coins[symbol]
	if !exists {
		// Initialize the coin if it does not exist
		metrics = &model.CoinMetrics{
			ClosePriceClosed:  make([]float64, 0, fe.cfg.MarketConstants.MetricsWindowSize+1),
			VolumeValueClosed: make([]float64, 0, fe.cfg.MarketConstants.MetricsWindowSize+1),
		}
		fe.Coins[symbol] = metrics
	}
	return metrics
}

func (fe *Engine) ProcessRSI() {
	for symbol := range fe.closedKlineReceived {
		rsi, err := fe.calculateAndStoreMarketMetrics(symbol)
		if err != nil {
			log.Println("Error calculating RSI:", err)
			continue
		}
		fe.evaluateRSI(symbol, rsi)
	}
}

func (fe *Engine) calculateAndStoreMarketMetrics(symbol string) (float64, error) {
	mapMutex.Lock()
	defer mapMutex.Unlock()

	coinMetrics, exists := fe.Coins[symbol]
	if !exists || len(coinMetrics.ClosePriceClosed) < fe.cfg.MarketConstants.MetricsWindowSize {
		return 0, fmt.Errorf("insufficient data to calculate RSI for symbol: %s", symbol)
	}

	rsi := calculateRSI(coinMetrics.ClosePriceClosed, fe.cfg.MarketConstants.KlineInterval-1) //To get only latest RSI
	volumeValueMedian := findMedian(coinMetrics.VolumeValueClosed) * float64(fe.cfg.MarketConstants.MetricsWindowSize)

	fe.Coins[symbol].RSIClosed = rsi
	fe.Coins[symbol].VolumeValueMedian = volumeValueMedian

	return rsi, nil
}

func (fe *Engine) evaluateRSI(topic string, rsi float64) {
	if rsi <= fe.cfg.Trader.RSIBuyValue {
		fe.buyReady <- topic
	}
}

// initClosedCoinMetrics retrieves most recent n closed klines for a given trading symbol and initializes CoinMetrics.
func (fe *Engine) initClosedCoinMetrics(symbol string, n int) (*model.CoinMetrics, error) {
	startTime, endTime := determineTimeInterval(n, fe.cfg.MarketConstants.KlineInterval)

	params := map[string]string{
		"symbol":   symbol,
		"category": fe.cfg.MarketConstants.KlineCategory,
		"interval": strconv.Itoa(fe.cfg.MarketConstants.KlineInterval),
		"start":    strconv.FormatInt(startTime, 10),
		"end":      strconv.FormatInt(endTime, 10),
		"limit":    strconv.Itoa(n + 1),
	}

	requestUrl := buildUrl(fe.cfg.Broker.API.Host, fe.cfg.Broker.API.RequestPaths.Kline, params)
	req, err := http.NewRequest("GET", requestUrl, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request for kline data: %w", err)
	}

	response, err := fe.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error fetching kline data: %w", err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %w", err)
	}

	var apiResponse model.KlineApiResponse
	if err := json.Unmarshal(body, &apiResponse); err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	if len(apiResponse.Result.List) < 1 {
		return nil, fmt.Errorf("no kline data available for symbol: %s", symbol)
	}

	// Extract the 5th value (close price) from each list
	closePrices := make([]float64, 0, len(apiResponse.Result.List))
	volumeValues := make([]float64, 0, len(apiResponse.Result.List))
	for _, item := range apiResponse.Result.List {
		closePrice, _ := strconv.ParseFloat(strings.TrimSpace(item[4]), 64)
		volumeAmount, _ := strconv.ParseFloat(strings.TrimSpace(item[5]), 64)

		closePrices = append(closePrices, closePrice)
		volumeValues = append(volumeValues, volumeAmount*closePrice)
	}
	reverse(closePrices)
	reverse(volumeValues)

	// Eliminate live value, just return most recent closed values
	return &model.CoinMetrics{
		ClosePriceClosed:  closePrices[:len(closePrices)-1],
		VolumeValueClosed: volumeValues[:len(volumeValues)-1],
	}, nil
}

func (fe *Engine) loadRecentKlineData() {
	for _, symbol := range fe.cfg.Symbols {
		coinMetrics, err := fe.initClosedCoinMetrics(symbol, fe.cfg.MarketConstants.MetricsWindowSize)

		if err != nil {
			log.Println("Error fetching recent kline data for", symbol, ":", err)
			continue
		}
		fe.Coins[symbol] = coinMetrics

		_, err = fe.calculateAndStoreMarketMetrics(symbol)
		if err != nil {
			log.Println("Error calculating recent kline data for", symbol, ":", err)
			continue
		}
	}
}

func (fe *Engine) getDefaultMissingDataValue() float64 {
	return fe.cfg.Defaults.MissingDataValue
}
func (fe *Engine) Show() {

	// Extract and sort the keys (symbols) from the Coins map
	var symbols []string
	for symbol := range fe.Coins {
		symbols = append(symbols, symbol)
	}
	sort.Strings(symbols) // Sorts the slice of symbols in increasing order

	for {
		mapMutex.Lock()

		fmt.Print("\033[H\033[2J") // Move cursor to home and clear the screen
		for _, symbol := range symbols {
			metrics := fe.Coins[symbol]
			fmt.Printf("coin: %s live: %.2f closed: %.2f volume: %.2f\n", symbol, metrics.RSILive, metrics.RSIClosed, metrics.VolumeValueMedian)
		}

		mapMutex.Unlock()
		time.Sleep(1 * time.Second) // Update frequency; adjust as needed
	}
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

// findMedian calculates the median of a slice of float64 numbers.
func findMedian(values []float64) float64 {
	n := len(values)
	if n == 0 {
		return 0 // or return an error indicating no values are present
	}

	// Make a copy of the slice to avoid altering the original data
	sortedValues := make([]float64, n)
	copy(sortedValues, values)
	sort.Float64s(sortedValues)

	// Finding the median
	midpoint := n / 2 // Get the middle index
	if n%2 != 0 {
		// Odd number of elements
		return sortedValues[midpoint] // Middle element
	}
	// Even number of elements, return the average of the two middle values
	return (sortedValues[midpoint-1] + sortedValues[midpoint]) / 2
}
