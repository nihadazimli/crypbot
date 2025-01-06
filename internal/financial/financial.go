// Package financial has all logic to calculate every financial data needed for trader for any operation
package financial

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/nihadazimli/crypbot/pkg/numutils"

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
	GetMovingAverages(symbol string) (model.MovingAverage, error)
	Show()
}

type Engine struct {
	httpClient             *http.Client
	cfg                    *config.Config
	broker                 broker_api.BrokerAPI
	buyReady               chan string
	klineReceived          chan model.WSInternalKline
	Coins                  map[string]*model.CoinMetrics
	maCache                map[string]*MACache
	maCacheTTL             time.Duration
	maCacheMutex           sync.RWMutex
	cycleMutex             sync.Mutex
	cycleTimer             *time.Timer
	cycleTimeout           time.Duration
	expectedCoinCount      int                // Total number of expected coin count in every cycle
	processedCoinCount     int                // Tracks all processed coins count in current cycle
	buyTradeCandidateCoins map[string]float64 // Stores coins with buy signal in current cycle with RSI value
}

func (fe *Engine) GetMovingAverages(symbol string) (model.MovingAverage, error) {

	maData, err := fe.getMovingAverage(symbol)
	if err != nil {
		return model.MovingAverage{}, fmt.Errorf("GetMovingAverages - %s - Err: %v", err)
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
	expectedCoinCount int,
) *Engine {
	return &Engine{
		httpClient:             &http.Client{Timeout: time.Duration(httpTimeoutSec) * time.Second},
		cfg:                    cfg,
		Coins:                  make(map[string]*model.CoinMetrics),
		maCache:                make(map[string]*MACache),
		maCacheTTL:             maCacheTTL,
		klineReceived:          klineReceived,
		buyReady:               buyReady,
		broker:                 broker,
		expectedCoinCount:      expectedCoinCount,
		buyTradeCandidateCoins: make(map[string]float64),
		cycleTimeout:           time.Duration(cfg.MarketConstants.CloseKlineTimeoutMinutes) * time.Minute,
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

	fe := NewFinancialEngine(cfg, broker, klineReceived, buyReady, timeout, maCacheTimeout, len(cfg.Symbols))

	err := fe.loadRecentKlineData(ctx, fe.cfg.Symbols)
	if err != nil {
		log.Printf("Cannot load recent data successfully. Err: %v\n", err)
	}
	fe.startCacheCleanup(ctx)

	err = fe.broker.StartKlineStream(ctx, broker_api.KlineStreamRequestParams{
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

	mapMutex.Lock()
	err := fe.storeRawMarketDataSlices(klineDatapoint.Symbol, klineDatapoint.Close, klineDatapoint.Volume)
	if err != nil {
		log.Printf("Process Closed Kline - error storing raw market data. Err:%v\n", err)
		return
	}
	err = fe.calculateAndStoreMarketMetrics(klineDatapoint.Symbol)
	if err != nil {
		log.Printf("Process Closed Kline - error calculating RSI. Err:%v\n", err)
		return
	}
	mapMutex.Unlock()
	fe.evaluateKlineSignal(klineDatapoint.Symbol)

}

func (fe *Engine) processLiveKline(klineDatapoint model.WSInternalKline) {
	coinMetrics, ok := fe.Coins[klineDatapoint.Symbol]
	if ok && len(coinMetrics.ClosePriceClosed) == fe.cfg.MarketConstants.MetricsWindowSize {
		// Construct a dataset that includes the last MetricsWindowSize-1 closed prices and the live price
		liveData := append(coinMetrics.ClosePriceClosed[1:], klineDatapoint.Close)
		coinMetrics.RSILive = numutils.CalculateRSI(liveData, fe.cfg.MarketConstants.KlineInterval-1)
	}
}

func (fe *Engine) storeRawMarketDataSlices(symbol string, closePrice float64, tradeVolume float64) error {
	coinMetrics, exists := fe.Coins[symbol]
	if !exists {
		return fmt.Errorf("storeRawMarketDataSlices - %s does not exist in coin map", symbol)
	}

	// Append new closed prices and volumes to their respective slices
	coinMetrics.ClosePriceClosed = append(coinMetrics.ClosePriceClosed, closePrice)
	coinMetrics.VolumeValueClosed = append(
		coinMetrics.VolumeValueClosed,
		closePrice*tradeVolume,
	)

	// Ensure the slices do not exceed the defined window size
	if len(coinMetrics.ClosePriceClosed) > fe.cfg.MarketConstants.MetricsWindowSize {
		coinMetrics.ClosePriceClosed = coinMetrics.ClosePriceClosed[len(coinMetrics.ClosePriceClosed)-fe.cfg.MarketConstants.MetricsWindowSize:]
		coinMetrics.VolumeValueClosed = coinMetrics.VolumeValueClosed[len(coinMetrics.VolumeValueClosed)-fe.cfg.MarketConstants.MetricsWindowSize:]
	}
	return nil
}

func (fe *Engine) calculateAndStoreMarketMetrics(symbol string) error {

	coinMetrics, exists := fe.Coins[symbol]
	if !exists || len(coinMetrics.ClosePriceClosed) < fe.cfg.MarketConstants.MetricsWindowSize {
		return fmt.Errorf("insufficient or missing data to calculate RSI for %s", symbol)
	}

	rsi := numutils.CalculateRSI(
		coinMetrics.ClosePriceClosed,
		fe.cfg.MarketConstants.KlineInterval-1,
	) //To get only latest RSI
	volumeValueMedian := numutils.FindMedian(
		coinMetrics.VolumeValueClosed,
	) * float64(
		fe.cfg.MarketConstants.MetricsWindowSize,
	)

	fe.Coins[symbol].RSIClosed = rsi
	fe.Coins[symbol].VolumeValueMedian = volumeValueMedian

	return nil
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
		return nil, fmt.Errorf("GetKlineHistory - symbol %s err: %v", symbol, err)
	}

	return &model.CoinMetrics{
		ClosePriceClosed:  klineHistoryMarket.ClosePrices,
		VolumeValueClosed: klineHistoryMarket.Volumes,
	}, nil
}

func (fe *Engine) loadRecentKlineData(ctx context.Context, symbols []string) error {
	for _, symbol := range symbols {
		coinMetrics, err := fe.getRawCoinMetricsData(ctx, symbol)

		if err != nil {
			return fmt.Errorf("load recent kline - %v", err)
		}
		fe.Coins[symbol] = coinMetrics
		err = fe.calculateAndStoreMarketMetrics(symbol)
		if err != nil {
			return fmt.Errorf("load recent kline - %v", err)
		}
	}
	return nil
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
				"%s L: %.2f C: %.2f V: %.2f CP: %.2f\n",
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

	maLong := numutils.MovingAverage(klineHistory.ClosePrices, fe.cfg.MarketConstants.ClosePriceMAWindowSizeLong)
	maShort := numutils.MovingAverage(klineHistory.ClosePrices, fe.cfg.MarketConstants.ClosePriceMAWindowSizeShort)

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
		defer ticker.Stop()
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

func (fe *Engine) evaluateKlineSignal(symbol string) {
	fe.cycleMutex.Lock()
	defer fe.cycleMutex.Unlock()

	// Start the timer if it is the first coin to process
	if fe.processedCoinCount == 0 {
		if fe.cycleTimer != nil {
			fe.cycleTimer.Stop()
		}

		fe.cycleTimer = time.AfterFunc(fe.cycleTimeout, func() {
			fe.handleCycleTimeout()
		})
	}

	fe.processedCoinCount++

	rsiClosed, exists := fe.GetRSIClosed(symbol)
	if !exists {
		log.Printf("evaluateKlineSignal - %s does not have RSI closed\n", symbol)
		return
	}

	if fe.hasSymbolBuySignal(symbol) {
		fe.buyTradeCandidateCoins[symbol] = rsiClosed
	}

	if fe.processedCoinCount >= fe.expectedCoinCount {
		fe.selectAndSignal()
		fe.resetCycle()
	}

}

func (fe *Engine) resetCycle() {
	fe.processedCoinCount = 0
	fe.buyTradeCandidateCoins = make(map[string]float64)
	if fe.cycleTimer != nil {
		fe.cycleTimer.Stop()
		fe.cycleTimer = nil
	}
	log.Printf("resetCycle - Cycle reset\n")
}

func (fe *Engine) selectAndSignal() {

	if len(fe.buyTradeCandidateCoins) == 0 {
		return // Nothing to buy in this cycle
	}

	var selectedSymbol string
	minRsi := math.MaxFloat64
	for symbol, rsiValue := range fe.buyTradeCandidateCoins {
		if rsiValue < minRsi {
			selectedSymbol = symbol
			minRsi = rsiValue
		}
	}

	log.Printf("selectAndSignal - EligibleCoins: %v, Selected Coin: %s with RSI: %.2f\n",
		fe.buyTradeCandidateCoins, selectedSymbol, minRsi)

	fe.buyReady <- selectedSymbol
}

func (fe *Engine) handleCycleTimeout() {
	fe.cycleMutex.Lock()
	defer fe.cycleMutex.Unlock()

	log.Printf("handleCycleTimeout - Cycle timeout reached. Processed %d out of %d coins\n",
		fe.processedCoinCount, fe.expectedCoinCount)
	fe.selectAndSignal()
	fe.resetCycle()
}

func (fe *Engine) hasSymbolBuySignal(symbol string) bool {
	rsiClosed, rsiOk := fe.GetRSIClosed(symbol)
	if !rsiOk {
		log.Println("No RSI data available")
		// QUESTION: Should I send construct signal to financial for missing coins? Isn't it coupling these 2 packages?
		return false
	}

	volumeMedian, volumeOk := fe.GetVolumeValueMedian(symbol)
	if !volumeOk {
		log.Println("No volume data available")
		return false
	}

	movingAverages, err := fe.GetMovingAverages(symbol)
	if err != nil {
		log.Printf("Could not retrieve MA - Err: %s\n", err)
		return false
	}

	if rsiClosed > fe.cfg.Trader.RSIBuyValue {
		return false
	}

	if volumeMedian < fe.cfg.Trader.MedianVolumeThreshold {
		return false
	}

	if movingAverages.MA50 < movingAverages.MA100 {
		log.Printf("Moving average short (%.2f) is smaller than moving average long (%.2f)\n", movingAverages.MA50, movingAverages.MA100)
		return false
	}

	return true
}
