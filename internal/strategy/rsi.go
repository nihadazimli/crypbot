package strategy

import (
	"context"
	"log"
	"math"
	"sync"
	"time"

	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/model"
)

type StrategyState int

const (
	StateWatchingToBuy StrategyState = iota
	StateHolding
	StateInProgress
)

type Symbol string

type RSIStrategy struct {
	// Configuration
	subAccountID string
	rsiConfig    *config.RSITraderConfig

	// Dependencies
	financial    FinancialService
	trader       TraderService
	marketEvents <-chan model.MarketEvent

	// Strategy state
	state      StrategyState
	heldSymbol Symbol
	stateMutex sync.RWMutex

	// Cycle management (moved from financial engine)
	cycleMutex             sync.Mutex
	cycleTimer             *time.Timer
	cycleTimeout           time.Duration
	processedCoinsInCycle  map[string]bool    // Tracks processed coins in current cycle
	buyTradeCandidateCoins map[string]float64 // Stores coins with buy signal with RSI value
	symbols                []string

	// Control
	ctx    context.Context
	cancel context.CancelFunc
}

func NewRSIStrategy(
	subAccountID string,
	financial FinancialService,
	trader TraderService,
	marketEvents <-chan model.MarketEvent,
	symbols []string,
) *RSIStrategy {
	rsiConfig, err := config.GetRSITraderConfig()
	if err != nil {
		log.Printf("RSI Strategy - Cannot get rsi config, fix config file!")
		return nil
	}
	return &RSIStrategy{
		rsiConfig:              rsiConfig,
		subAccountID:           subAccountID,
		financial:              financial,
		trader:                 trader,
		marketEvents:           marketEvents,
		state:                  StateWatchingToBuy,
		symbols:                symbols,
		processedCoinsInCycle:  make(map[string]bool),
		buyTradeCandidateCoins: make(map[string]float64),
		cycleTimeout:           rsiConfig.CloseKlineCycleTimeout,
	}
}

func (rs *RSIStrategy) Start(ctx context.Context) {
	rs.ctx, rs.cancel = context.WithCancel(ctx)
	log.Printf("RSI Strategy started for sub-account: %s\n", rs.subAccountID)

	for {
		select {
		case event := <-rs.marketEvents:
			rs.handleMarketEvent(ctx, event)
		case <-ctx.Done():
			log.Printf("RSI Strategy stopping for sub-account: %s\n", rs.subAccountID)
			return
		}
	}
}

func (rs *RSIStrategy) Stop() {
	if rs.cancel != nil {
		rs.cancel()
	}
	if rs.cycleTimer != nil {
		rs.cycleTimer.Stop()
	}
}

func (rs *RSIStrategy) handleMarketEvent(ctx context.Context, event model.MarketEvent) {
	if !event.IsClosed {
		return
	}

	rs.stateMutex.RLock()
	state := rs.state
	rs.stateMutex.RUnlock()

	switch state {
	case StateWatchingToBuy:
		go rs.evaluateKlineSignalToBuy(ctx, event)
	case StateHolding:
		if Symbol(event.Symbol) != rs.heldSymbol {
			return
		}
		rs.evaluateSellSignal(ctx, event)
	case StateInProgress:
		return
	default:
		panic("RSI Strategy - unhandled state case")
	}
}

func (rs *RSIStrategy) evaluateKlineSignalToBuy(ctx context.Context, event model.MarketEvent) {
	hasBuySignal := rs.hasSymbolBuySignal(event)
	rs.cycleMutex.Lock()
	defer rs.cycleMutex.Unlock()

	// Start the timer if it is the first coin to process
	if len(rs.processedCoinsInCycle) == 0 {
		if rs.cycleTimer != nil {
			rs.cycleTimer.Stop()
		}

		rs.cycleTimer = time.AfterFunc(rs.cycleTimeout, func() {
			rs.handleCycleTimeout(ctx)
		})
	}

	rs.processedCoinsInCycle[event.Symbol] = true

	if hasBuySignal {
		rs.buyTradeCandidateCoins[event.Symbol] = event.RSI
		log.Printf("RSI Strategy: Buy candidate found - %s with RSI: %.2f\n", event.Symbol, event.RSI)
	}

	if len(rs.processedCoinsInCycle) >= len(rs.symbols) {
		rs.selectAndSignal(ctx)
		rs.resetCycle()
	}
}

func (rs *RSIStrategy) evaluateSellSignal(ctx context.Context, event model.MarketEvent) {
	rs.stateMutex.RLock()
	if rs.state != StateHolding {
		rs.stateMutex.RUnlock()
		return
	}
	rs.stateMutex.RUnlock()

	rsiClosed, ok := rs.financial.GetRSIClosed(event.Symbol)
	if !ok || rsiClosed < rs.rsiConfig.SellThreshold {
		return
	}

	log.Printf("RSI Strategy: RSI hit sell threshold. %s: %.2f\n", event.Symbol, rsiClosed)
	rs.stateMutex.Lock()
	rs.state = StateInProgress
	rs.stateMutex.Unlock()

	err := rs.trader.ExecuteSell(ctx, event.Symbol, model.OrderTypeLimit, rs.checkSellConditions)
	rs.stateMutex.Lock()
	if err != nil {
		log.Printf("RSI Strategy: Sell execution failed for %s. Err: %v\n", event.Symbol, err)
		rs.state = StateHolding // Back to holding on failure
	} else {
		rs.state = StateWatchingToBuy
		rs.heldSymbol = ""
		log.Printf("RSI Strategy: Successfully sold %s\n", event.Symbol)
	}
	rs.stateMutex.Unlock()
	//if result.Success {
	//	profit := (result.AvgPrice - rs.entryPrice) * result.FilledQty
	//	log.Printf("RSI Strategy: ✅ Successfully sold %f %s at %f (Profit: %f)\n",
	//		result.FilledQty, event.Symbol, result.AvgPrice, profit)
	//	rs.clearPosition()
	//} else {
	//	log.Printf("RSI Strategy: Sell failed for %s. Error: %v\n", event.Symbol, result.Error)
	//}
}

func (rs *RSIStrategy) hasSymbolBuySignal(event model.MarketEvent) bool {
	if event.RSI > rs.rsiConfig.BuyThreshold {
		return false
	}

	volumeMedian, volumeOk := rs.financial.GetVolumeValueMedian(event.Symbol)
	if !volumeOk {
		log.Printf("RSI Strategy: No volume data available for %s\n", event.Symbol)
		return false
	}

	if volumeMedian < rs.rsiConfig.MedianVolumeThreshold {
		return false
	}

	movingAverages, err := rs.financial.GetMovingAverages(event.Symbol)
	if err != nil {
		log.Printf("RSI Strategy: Could not retrieve MA for %s - Err: %s\n", event.Symbol, err)
		return false
	}

	if movingAverages.MA50 < movingAverages.MA100 {
		log.Printf("RSI Strategy: MA50 (%.2f) < MA100 (%.2f) for %s\n",
			movingAverages.MA50, movingAverages.MA100, event.Symbol)
		return false
	}

	return true
}

func (rs *RSIStrategy) selectAndSignal(ctx context.Context) {
	if len(rs.buyTradeCandidateCoins) == 0 {
		//log.Printf("RSI Strategy: No buy candidates in this cycle\n")
		return
	}
	rs.stateMutex.Lock()
	rs.state = StateInProgress
	rs.stateMutex.Unlock()

	// TODO: While waiting for all coins RSI values might have changed. Might be better to decide on RSI Live here
	var selectedSymbol string
	minRsi := math.MaxFloat64
	for symbol, rsiValue := range rs.buyTradeCandidateCoins {
		if rsiValue < minRsi {
			selectedSymbol = symbol
			minRsi = rsiValue
		}
	}

	log.Printf("RSI Strategy: Eligible coins: %v, Selected: %s with RSI: %.2f\n",
		rs.buyTradeCandidateCoins, selectedSymbol, minRsi)

	// Execute buy order
	err := rs.trader.ExecuteBuy(ctx, selectedSymbol, model.OrderTypeLimit, rs.checkBuyConditions)

	rs.stateMutex.Lock()
	if err != nil {
		log.Printf("RSI Strategy: Buy execution failed for %s. Err: %v\n", selectedSymbol, err)
		rs.state = StateWatchingToBuy
		rs.heldSymbol = ""
	} else {
		rs.state = StateHolding
		rs.heldSymbol = Symbol(selectedSymbol)
	}
	rs.stateMutex.Unlock()

	//if result.Success {
	//	rs.state = StateHolding
	//	rs.entryPrice = result.AvgPrice
	//	rs.position = result.FilledQty
	//	log.Printf("RSI Strategy: ✅ Successfully bought %f %s at %f\n",
	//		result.FilledQty, selectedSymbol, result.AvgPrice)
	//} else {
	//	log.Printf("RSI Strategy: Buy failed for %s. Error: %v\n", selectedSymbol, result.Error)
	//}
}

func (rs *RSIStrategy) checkBuyConditions(ctx context.Context, symbol string) model.OrderConditionResult {
	rsiLive, rsiLiveOk := rs.financial.GetRSILive(symbol)
	rsiClosed, rsiClosedOk := rs.financial.GetRSIClosed(symbol)
	volumeMedian, volumeOk := rs.financial.GetVolumeValueMedian(symbol)
	movingAverages, maErr := rs.financial.GetMovingAverages(symbol)

	if !rsiLiveOk || !rsiClosedOk || !volumeOk || maErr != nil {
		log.Printf("RSI Strategy: Insufficient data for %s", symbol)
		return model.ConditionCancel
	}

	if rsiClosed > rs.rsiConfig.BuyThreshold ||
		volumeMedian < rs.rsiConfig.MedianVolumeThreshold ||
		movingAverages.MA50 < movingAverages.MA100 {
		return model.ConditionCancel
	}

	if rsiLive > rs.rsiConfig.BuyThreshold {
		return model.ConditionWait
	}

	return model.ConditionMet
}

func (rs *RSIStrategy) checkSellConditions(ctx context.Context, symbol string) model.OrderConditionResult {
	return model.ConditionMet
}
func (rs *RSIStrategy) resetCycle() {
	rs.processedCoinsInCycle = make(map[string]bool)
	rs.buyTradeCandidateCoins = make(map[string]float64)
	if rs.cycleTimer != nil {
		rs.cycleTimer.Stop()
		rs.cycleTimer = nil
	}
	//log.Printf("RSI Strategy: Cycle reset\n")
}

func (rs *RSIStrategy) handleCycleTimeout(ctx context.Context) {
	rs.cycleMutex.Lock()
	defer rs.cycleMutex.Unlock()

	var missingCoins []string
	for _, symbol := range rs.symbols {
		if !rs.processedCoinsInCycle[symbol] {
			missingCoins = append(missingCoins, symbol)
		}
	}

	if len(missingCoins) > 0 {
		log.Printf("RSI Strategy: Cycle timeout. Missing coins: %v\n", missingCoins)
	}

	log.Printf("RSI Strategy: Cycle timeout reached. Processed %d out of %d coins\n",
		len(rs.processedCoinsInCycle), len(rs.symbols))

	rs.selectAndSignal(ctx)
	rs.resetCycle()
}

func (rs *RSIStrategy) clearPosition() {
	rs.state = StateWatchingToBuy
}
