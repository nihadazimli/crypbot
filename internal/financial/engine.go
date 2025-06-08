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
	"github.com/nihadazimli/crypbot/internal/exchange"
	"github.com/nihadazimli/crypbot/internal/model"
	"github.com/nihadazimli/crypbot/pkg/numutils"
)

var (
	mapMutex sync.Mutex
)

type MACache struct {
	MA50        float64
	MA100       float64
	LastUpdated time.Time
}

type Engine struct {
	httpClient       *http.Client
	cfg              *config.Config
	exchange         exchange.MarketDataClient
	klineReceived    chan model.WSInternalKline
	marketEventsChan chan<- model.MarketEvent
	Coins            map[string]*model.CoinMetrics
	maCache          map[string]*MACache
	maCacheTTL       time.Duration
	maCacheMutex     sync.RWMutex
}

func (fe *Engine) GetMovingAverages(symbol string) (model.MovingAverage, error) {

	maData, err := fe.getMovingAverage(symbol)
	if err != nil {
		return model.MovingAverage{}, fmt.Errorf("GetMovingAverages - %s", err)
	}

	return model.MovingAverage{
		MA50:  maData.MA50,
		MA100: maData.MA100,
	}, nil

}

// NewFinancialEngine creates a new Engine instance with specified channels and HTTP client timeout.
func NewFinancialEngine(
	exchange exchange.MarketDataClient,
	marketEventsChan chan<- model.MarketEvent,
) *Engine {
	cfg := config.GetConfig()
	maCacheTimeout := time.Duration(cfg.MarketConstants.ClosePriceMAInterval) * time.Minute

	return &Engine{
		httpClient:       &http.Client{Timeout: cfg.Exchange.API.Timeout},
		cfg:              cfg,
		Coins:            make(map[string]*model.CoinMetrics),
		maCache:          make(map[string]*MACache),
		maCacheTTL:       maCacheTimeout,
		klineReceived:    make(chan model.WSInternalKline),
		marketEventsChan: marketEventsChan,
		exchange:         exchange,
	}
}

func (fe *Engine) Start(
	ctx context.Context,
) *Engine {
	err := fe.loadRecentKlineData(ctx, fe.cfg.Symbols)
	if err != nil {
		log.Printf("Cannot load recent data successfully. Err: %v\n", err)
	}
	fe.startCacheCleanup(ctx)

	err = fe.exchange.StartKlineStream(ctx, exchange.KlineStreamRequestParams{
		Symbols: fe.cfg.Symbols, KlineWriteChan: fe.klineReceived, Interval: fe.cfg.MarketConstants.KlineInterval,
	})
	if err != nil {
		log.Printf("Cannot start KlineStream. Err: %v\n", err)
	}
	go fe.processKline()
	//go fe.Show()
	log.Printf("Financial Engine initialized successfully for all %v symbols!\n", len(fe.cfg.Symbols))
	return fe
}

func (fe *Engine) processKline() {
	for klineDatapoint := range fe.klineReceived {
		if klineDatapoint.IsClosed {
			fe.processClosedKline(klineDatapoint)
		} else {
			fe.processLiveKline(klineDatapoint)
		}
		fe.sendMarketEvent(klineDatapoint)
	}
}

func (fe *Engine) GetZScore(symbol string) (float64, bool) {
	if metrics, exists := fe.Coins[symbol]; exists {
		return metrics.ZScore20, true
	}
	return fe.getDefaultMissingDataValue(), false
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
}

func (fe *Engine) sendMarketEvent(klineDatapoint model.WSInternalKline) {
	rsi, _ := fe.GetRSIClosed(klineDatapoint.Symbol)

	zScore, _ := fe.GetZScore(klineDatapoint.Symbol)

	marketEvent := model.MarketEvent{
		Symbol:    klineDatapoint.Symbol,
		Price:     klineDatapoint.Close,
		RSI:       rsi,
		ZScore20:  zScore,
		IsClosed:  klineDatapoint.IsClosed,
		Timestamp: float64(time.Now().Unix()),
	}

	select {
	case fe.marketEventsChan <- marketEvent:
	default:
		log.Printf("Financial Engine: Warning - market events channel is full, dropping event for symbol %s\n",
			klineDatapoint.Symbol)
	}
}

func (fe *Engine) processLiveKline(klineDatapoint model.WSInternalKline) {
	coinMetrics, ok := fe.Coins[klineDatapoint.Symbol]
	if ok && len(coinMetrics.ClosePriceClosed) == fe.cfg.MarketConstants.MetricsWindowSize {
		// Construct a dataset that includes the last MetricsWindowSize-1 closed prices and the live price
		liveData := append(coinMetrics.ClosePriceClosed[1:], klineDatapoint.Close)
		coinMetrics.RSILive = numutils.RSI(liveData, fe.cfg.MarketConstants.KlineInterval-1)
		coinMetrics.ZScore20 = numutils.ZScore(liveData, fe.cfg.MarketConstants.ZScorePeriod)
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

	rsi := numutils.RSI(
		coinMetrics.ClosePriceClosed,
		fe.cfg.MarketConstants.KlineInterval-1,
	) //To get only latest RSI
	volumeValueMedian := numutils.FindMedian(
		coinMetrics.VolumeValueClosed,
	) * float64(
		fe.cfg.MarketConstants.MetricsWindowSize,
	)

	zScore := numutils.ZScore(coinMetrics.ClosePriceClosed, fe.cfg.MarketConstants.ZScorePeriod)

	fe.Coins[symbol].RSIClosed = rsi
	fe.Coins[symbol].ZScore20 = zScore
	fe.Coins[symbol].VolumeValueMedian = volumeValueMedian

	return nil
}

// getRawCoinMetricsData retrieves most recent n closed klines for a given trading symbol and initializes CoinMetrics.
func (fe *Engine) getRawCoinMetricsData(ctx context.Context, symbol string) (*model.CoinMetrics, error) {

	params := exchange.KlineHistoryParams{
		Symbol:   symbol,
		Interval: fe.cfg.MarketConstants.KlineInterval,
		Limit:    fe.cfg.MarketConstants.MetricsWindowSize,
	}

	klineHistoryMarket, err := fe.exchange.GetKlineHistory(ctx, params)
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
	var symbols []string
	for symbol := range fe.Coins {
		symbols = append(symbols, symbol)
	}
	sort.Strings(symbols) // Sorts the slice of symbols in increasing order

	for {
		fmt.Print("\033[H\033[2J") // Move cursor to home and clear the screen
		for _, symbol := range symbols {
			metrics := fe.Coins[symbol]

			log.Printf(
				"Symbol: %s, ZScore: %v, RSIClosed: %v,RSILive: %v",
				symbol,
				metrics.ZScore20,
				metrics.RSIClosed,
				metrics.RSILive,
			)
		}

		time.Sleep(30 * time.Second) // Update frequency; adjust as needed
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

	params := exchange.KlineHistoryParams{
		Symbol:   symbol,
		Interval: fe.cfg.MarketConstants.ClosePriceMAInterval,
		Limit:    fe.cfg.MarketConstants.ClosePriceMA100WindowSize,
	}
	klineHistory, err := fe.exchange.GetKlineHistory(context.Background(), params)
	if err != nil {
		return nil, err
	}
	if len(klineHistory.ClosePrices) < fe.cfg.MarketConstants.ClosePriceMA100WindowSize {
		return nil, fmt.Errorf(
			"%s has not enough Kline datapoints for MA100, actual: %d required: %d\n",
			symbol,
			len(klineHistory.ClosePrices),
			fe.cfg.MarketConstants.ClosePriceMA100WindowSize,
		)
	}
	ma100 := numutils.MovingAverage(klineHistory.ClosePrices, fe.cfg.MarketConstants.ClosePriceMA100WindowSize)
	ma50 := numutils.MovingAverage(klineHistory.ClosePrices, fe.cfg.MarketConstants.ClosePriceMA50WindowSize)

	fe.maCacheMutex.Lock()
	fe.maCache[symbol] = &MACache{
		MA100:       ma100,
		MA50:        ma50,
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
