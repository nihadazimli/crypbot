// Package financial has all logic to calculate every financial data needed for trader for any operation
package financial

import (
	"context"
	"fmt"
	"log"
	"math/rand"
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
	GetMovingAverage(symbol string) (model.MovingAverage, error)
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

func (fe *Engine) GetMovingAverage(symbol string) (model.MovingAverage, error) {

	maData, err := fe.getMovingAverage(symbol)
	if err != nil {
		return model.MovingAverage{}, fmt.Errorf("GetMovingAverage - %s - Err: %v", err)
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

	err := fe.loadRecentKlineData(ctx)
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
		coinMetrics.RSILive = calculateRSI(liveData, fe.cfg.MarketConstants.KlineInterval-1)
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

	rsi := calculateRSI(
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

func (fe *Engine) loadRecentKlineData(ctx context.Context) error {
	for _, symbol := range fe.cfg.Symbols {
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

	if rsiClosed <= fe.cfg.Trader.RSIBuyValue {
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

	var eligibleCoins []string
	for symbol := range fe.buyTradeCandidateCoins {
		eligibleCoins = append(eligibleCoins, symbol)
	}

	selectedSymbol := eligibleCoins[rand.Intn(len(eligibleCoins))]
	log.Printf("selectAndSignal - EligibleCoins: %v, Selected Coin: %s\n", fe.buyTradeCandidateCoins, selectedSymbol)
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
	avgGain := numutils.ExponentialMovingAverage(gains, n)
	avgLoss := numutils.ExponentialMovingAverage(losses, n)

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
