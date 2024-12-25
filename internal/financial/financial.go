// Package financial has all logic to calculate every financial data needed for trader for any operation
package financial

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/broker_api"
	"github.com/nihadazimli/crypbot/internal/model"
)

var (
	mapMutex sync.Mutex
)

type MACache struct {
	MAShort     float64
	MALong      float64
	LastUpdated time.Time
}

type EngineInterface interface {
	ProcessKline()
	GetRSIClosed(symbol string) (float64, bool)
	GetRSILive(symbol string) (float64, bool)
	GetVolumeValueMedian(symbol string) (float64, bool)
	GetCurrentPrice(symbol string) (float64, bool)
	GetMovingAverage(symbol string) (model.MovingAverage, error)
	Show()
}

type Engine struct {
	httpClient    *http.Client
	cfg           *config.Config
	broker        broker_api.BrokerAPI
	buyReady      chan string
	klineReceived chan model.WSInternalKline
	Coins         map[string]*model.CoinMetrics
	maCache       map[string]*MACache
	maCacheTTL    time.Duration
	maCacheMutex  sync.RWMutex
}

func (fe *Engine) GetMovingAverage(symbol string) (model.MovingAverage, error) {

	maData, err := fe.getMovingAverage(symbol)
	if err != nil {
		return model.MovingAverage{}, fmt.Errorf("Get Moving Average - %s - Err: %v", err)
	}

	return model.MovingAverage{
		MA50:  maData.MAShort,
		MA100: maData.MALong,
	}, nil

}

// NewFinancialEngine creates a new Engine instance with specified channels and HTTP client timeout.
func NewFinancialEngine(
	cfg *config.Config,
	broker broker_api.BrokerAPI,
	klineReceived chan model.WSInternalKline,
	buyReady chan string,
	httpTimeoutSec int,
	maCacheTTL time.Duration,
) *Engine {
	return &Engine{
		httpClient:    &http.Client{Timeout: time.Duration(httpTimeoutSec) * time.Second},
		cfg:           cfg,
		Coins:         make(map[string]*model.CoinMetrics),
		maCache:       make(map[string]*MACache),
		maCacheTTL:    maCacheTTL,
		klineReceived: klineReceived,
		buyReady:      buyReady,
		broker:        broker,
	}
}

func Start(
	ctx context.Context,
	broker broker_api.BrokerAPI,
	buyReady chan string,
	timeout int,
) *Engine {
	cfg := config.GetConfig()
	klineReceived := make(chan model.WSInternalKline)
	maCacheTimeout := time.Duration(cfg.MarketConstants.ClosePriceMAInterval) * time.Minute

	fe := NewFinancialEngine(cfg, broker, klineReceived, buyReady, timeout, maCacheTimeout)
	fe.loadRecentKlineData(ctx)
	fe.startCacheCleanup(ctx)

	err := fe.broker.StartKlineStream(ctx, broker_api.KlineStreamRequestParams{
		Symbols: fe.cfg.KlineTopics, KlineWriteChan: fe.klineReceived,
	})
	if err != nil {
		log.Printf("Cannot start KlineStream. Err: %v\n", err)
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
	if metrics, exists := fe.Coins[symbol]; exists && metrics != nil {
		if metrics.RSILive != 0 {
			return metrics.RSILive, true
		}
		return fe.getDefaultMissingDataValue(), false
	}
	return fe.getDefaultMissingDataValue(), false
}

func (fe *Engine) GetVolumeValueMedian(symbol string) (float64, bool) {
	if metrics, exists := fe.Coins[symbol]; exists && metrics != nil {
		if metrics.VolumeValueMedian != 0 {
			return metrics.VolumeValueMedian, true
		}
		return fe.getDefaultMissingDataValue(), false
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
	coinMetrics.VolumeValueClosed = append(
		coinMetrics.VolumeValueClosed,
		klineDatapoint.Close*klineDatapoint.Volume,
	)

	// Ensure the slices do not exceed the defined window size
	if len(coinMetrics.ClosePriceClosed) > fe.cfg.MarketConstants.MetricsWindowSize {
		coinMetrics.ClosePriceClosed = coinMetrics.ClosePriceClosed[len(coinMetrics.ClosePriceClosed)-fe.cfg.MarketConstants.MetricsWindowSize:]
		coinMetrics.VolumeValueClosed = coinMetrics.VolumeValueClosed[len(coinMetrics.VolumeValueClosed)-fe.cfg.MarketConstants.MetricsWindowSize:]
		fe.processMarketDataAndStoreMetrics(klineDatapoint.Symbol)
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

func (fe *Engine) processMarketDataAndStoreMetrics(symbol string) {
	err := fe.calculateAndStoreMarketMetrics(symbol)
	if err != nil {
		log.Println("Error calculating RSI:", err)
		return
	}
	fe.evaluateRSI(symbol, fe.Coins[symbol].RSIClosed)
}

func (fe *Engine) calculateAndStoreMarketMetrics(symbol string) error {
	mapMutex.Lock()
	defer mapMutex.Unlock()

	coinMetrics, exists := fe.Coins[symbol]
	if !exists || len(coinMetrics.ClosePriceClosed) < fe.cfg.MarketConstants.MetricsWindowSize {
		return fmt.Errorf("insufficient data to calculate RSI for symbol: %s", symbol)
	}

	rsi := calculateRSI(
		coinMetrics.ClosePriceClosed,
		fe.cfg.MarketConstants.KlineInterval-1,
	) //To get only latest RSI
	volumeValueMedian := findMedian(
		coinMetrics.VolumeValueClosed,
	) * float64(
		fe.cfg.MarketConstants.MetricsWindowSize,
	)

	fe.Coins[symbol].RSIClosed = rsi
	fe.Coins[symbol].VolumeValueMedian = volumeValueMedian

	return nil
}

func (fe *Engine) evaluateRSI(topic string, rsi float64) {
	if rsi <= fe.cfg.Trader.RSIBuyValue {
		fe.buyReady <- topic
	}
}

// getRawCoinMetricsData retrieves most recent n closed klines for a given trading symbol and initializes CoinMetrics.
func (fe *Engine) getRawCoinMetricsData(ctx context.Context, symbol string) (*model.CoinMetrics, error) {

	params := broker_api.KlineHistoryParams{
		Symbol:   symbol,
		Interval: fe.cfg.MarketConstants.KlineInterval,
		Limit:    fe.cfg.MarketConstants.MetricsWindowSize,
	}

	klineHistoryMarket, err := fe.broker.GetKlineHistory(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("GetKlineHistory - %v", err)
	}

	return &model.CoinMetrics{
		ClosePriceClosed:  klineHistoryMarket.ClosePrices,
		VolumeValueClosed: klineHistoryMarket.Volumes,
	}, nil
}

func (fe *Engine) loadRecentKlineData(ctx context.Context) {
	for _, symbol := range fe.cfg.Symbols {
		coinMetrics, err := fe.getRawCoinMetricsData(ctx, symbol)

		if err != nil {
			log.Println("Error fetching recent kline data for", symbol, ":", err)
			continue
		}
		fe.Coins[symbol] = coinMetrics
		err = fe.calculateAndStoreMarketMetrics(symbol)
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
	//var prevClosedRsi float64
	// Extract and sort the keys (symbols) from the Coins map
	var symbols []string
	for symbol := range fe.Coins {
		symbols = append(symbols, symbol)
	}
	sort.Strings(symbols) // Sorts the slice of symbols in increasing order

	for {
		fmt.Print("\033[H\033[2J") // Move cursor to home and clear the screen
		for _, symbol := range symbols {
			metrics := fe.Coins[symbol]
			//if metrics.RSIClosed < 21 && prevClosedRsi != metrics.RSIClosed {
			log.Printf(
				"%s L: %.2f C: %.2f V: %.2f CP: %.2f MA100: %.2f MA50: % \n",
				symbol,
				metrics.RSILive,
				metrics.RSIClosed,
				metrics.VolumeValueMedian,
				metrics.ClosePriceClosed[len(metrics.ClosePriceClosed)-1],
			)
			//prevClosedRsi = metrics.RSIClosed
			//}
		}

		time.Sleep(10 * time.Second) // Update frequency; adjust as needed
	}
}

func (fe *Engine) getMovingAverage(symbol string) (*MACache, error) {
	now := time.Now()

	fe.maCacheMutex.RLock()
	cache, exists := fe.maCache[symbol]
	fe.maCacheMutex.RUnlock()

	if exists && now.Sub(cache.LastUpdated) < fe.maCacheTTL {
		return cache, nil
	}

	params := broker_api.KlineHistoryParams{
		Symbol:   symbol,
		Interval: fe.cfg.MarketConstants.ClosePriceMAInterval,
		Limit:    fe.cfg.MarketConstants.ClosePriceMAWindowSizeLong,
	}
	klineHistory, err := fe.broker.GetKlineHistory(context.Background(), params)
	if err != nil {
		return nil, err
	}

	maLong := movingAverage(klineHistory.ClosePrices, fe.cfg.MarketConstants.ClosePriceMAWindowSizeLong)
	maShort := movingAverage(klineHistory.ClosePrices, fe.cfg.MarketConstants.ClosePriceMAWindowSizeShort)

	fe.maCacheMutex.Lock()
	fe.maCache[symbol] = &MACache{
		MALong:      maLong,
		MAShort:     maShort,
		LastUpdated: now,
	}
	fe.maCacheMutex.Unlock()

	return fe.maCache[symbol], nil
}

func (fe *Engine) startCacheCleanup(ctx context.Context) {
	ticker := time.NewTicker(fe.maCacheTTL)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fe.cleanExpiredCache()
			}
		}
	}()
}

func (fe *Engine) cleanExpiredCache() {
	now := time.Now()
	fe.maCacheMutex.Lock()
	defer fe.maCacheMutex.Unlock()

	for symbol, cache := range fe.maCache {
		if now.Sub(cache.LastUpdated) >= fe.maCacheTTL {
			delete(fe.maCache, symbol)
		}
	}
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

// movingAverage calculates the Moving Average for a slice of float64.
func movingAverage(values []float64, period int) float64 {
	if len(values) < period {
		return 0
	}

	sum := 0.0
	for i := len(values) - period; i < len(values); i++ {
		sum += values[i]
	}

	return sum / float64(period)
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
