package strategy

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"

	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/model"
)

type Position struct {
	Symbol        string
	TotalQuantity float64
	TotalInvested float64
	AvgPrice      float64
	State         PositionState
}

func (p *Position) addExecution(result *ExecutionResult) {
	executedValue := result.FilledQty * result.AvgPrice

	// Update totals
	p.TotalQuantity += result.FilledQty
	p.TotalInvested += executedValue

	// Recalculate average entry price
	if p.TotalQuantity > 0 {
		p.AvgPrice = p.TotalInvested / p.TotalQuantity
	}
}

type PositionState int

const (
	PositionHolding PositionState = iota
	PositionSelling
)

type BollingerStrategy struct {
	subAccountID     string
	traderConfig     *config.BollingerTraderConfig
	financial        FinancialService
	trader           TraderService
	marketEvents     <-chan model.MarketEvent
	positions        map[string]*Position
	sellingPositions map[string]bool
	stateMutex       sync.RWMutex
	positionMutex    sync.RWMutex
	minTradeAmount   float64
	availableBalance float64
	activeBuySymbol  string
}

func NewBollingerStrategy(
	subAccountID string,
	financial FinancialService,
	trader TraderService,
	marketEvents <-chan model.MarketEvent,
) (*BollingerStrategy, error) {
	traderConfig, err := config.GetBollingerTraderConfig()
	if err != nil {
		log.Printf("Bollinger - Cannot get bollinger config, fix config file!")
		return nil, err
	}
	balanceInfo, err := trader.GetBalance(context.Background())

	if err != nil {
		return nil, fmt.Errorf("Bollinger - %v", err)
	}

	return &BollingerStrategy{
		subAccountID:     subAccountID,
		traderConfig:     traderConfig,
		financial:        financial,
		trader:           trader,
		marketEvents:     marketEvents,
		positions:        make(map[string]*Position),
		sellingPositions: make(map[string]bool),
		minTradeAmount:   traderConfig.MinTradeAmount,
		availableBalance: balanceInfo.AvailableBalance,
	}, nil
}

func (bs *BollingerStrategy) Start(ctx context.Context) error {
	log.Printf("Bollinger started for sub-account: %s\n", bs.subAccountID)

	if bs.availableBalance < 0 {
		return fmt.Errorf("invalid available balance: %.2f", bs.availableBalance)
	}
	for {
		select {
		case event := <-bs.marketEvents:
			bs.handleMarketEvent(ctx, event)
		case <-ctx.Done():
			log.Printf("Bollinger - stopping for sub-account: %s\n", bs.subAccountID)
			return ctx.Err()
		}
	}
}

func (bs *BollingerStrategy) handleMarketEvent(ctx context.Context, event model.MarketEvent) {

	bs.handleBuySignal(ctx, event)
	bs.handleSellSignal(ctx, event)
}

func (bs *BollingerStrategy) handleBuySignal(ctx context.Context, event model.MarketEvent) {
	bs.stateMutex.RLock()
	canBuy := bs.availableBalance >= bs.minTradeAmount
	isBuying := bs.activeBuySymbol != ""
	bs.stateMutex.RUnlock()

	if !canBuy || isBuying {
		return
	}

	if bs.checkIndicatorsForBuy(event.ZScore20, event.Volume) == model.ConditionMet {
		go bs.executeBuySignal(ctx, event)
	}
}

func (bs *BollingerStrategy) executeBuySignal(ctx context.Context, event model.MarketEvent) {
	bs.stateMutex.Lock()
	if bs.activeBuySymbol != "" {
		bs.stateMutex.Unlock()
		return
	}
	bs.activeBuySymbol = event.Symbol
	bs.stateMutex.Unlock()

	log.Printf("Bollinger: starting buy for %s (Z=%.2f,price=%v balance=%.2f)",
		event.Symbol, event.ZScore20, event.Price, bs.availableBalance)

	result, err := bs.trader.ExecuteOrder(
		ctx,
		event.Symbol,
		model.OrderTypeLimit,
		model.OrderSideBuy,
		bs.buyConditionsChecker,
	)

	bs.stateMutex.Lock()
	bs.activeBuySymbol = ""
	bs.stateMutex.Unlock()

	if err != nil {
		log.Printf("Bollinger: Buy failed for %s: %v", event.Symbol, err)
	} else {
		log.Printf("Bollinger: Buy completed for %s: Price on hit:%v Result:%+v", event.Symbol, event.Price, result)
		bs.onBuyCompleted(event.Symbol, result)
	}

}
func (bs *BollingerStrategy) handleSellSignal(ctx context.Context, event model.MarketEvent) {

}

func (bs *BollingerStrategy) buyConditionsChecker(ctx context.Context, symbol string) model.OrderConditionResult {

	zScore, ok := bs.financial.GetZScore(symbol)
	if !ok {
		log.Printf("Bollinger - can't retrieve zscore for %s", symbol)
		return model.ConditionCancel
	}
	volume, ok := bs.financial.GetVolumeValueMedian(symbol)
	if !ok {
		log.Printf("Bollinger - can't retrieve volume for %s", symbol)
		return model.ConditionCancel
	}

	return bs.checkIndicatorsForBuy(zScore, volume)

}

func (bs *BollingerStrategy) checkIndicatorsForBuy(zScore float64, volume float64) model.OrderConditionResult {
	if zScore > bs.getBuyThreshold() {
		return model.ConditionCancel
	}
	if volume < bs.getVolumeThreshold() {
		return model.ConditionCancel
	}

	bs.stateMutex.RLock()
	hasBalance := bs.availableBalance >= bs.minTradeAmount
	bs.stateMutex.RUnlock()

	if !hasBalance {
		return model.ConditionCancel
	}

	return model.ConditionMet

}

func (bs *BollingerStrategy) onBuyCompleted(symbol string, result *ExecutionResult) {
	executedValue := result.FilledQty * result.AvgPrice

	bs.stateMutex.Lock()
	bs.availableBalance -= executedValue
	newBalance := bs.availableBalance
	bs.stateMutex.Unlock()

	balance, err := bs.trader.GetBalance(context.Background())
	if err != nil {
		log.Printf("onBuyCompleted - Error while retrieving balance")
	} else {
		if math.Abs(balance.AvailableBalance-bs.availableBalance) > bs.getBalanceVerificationThreshold() {
			log.Printf("Bollinger: Balance drift detected, corrected from %.2f to %.2f",
				bs.availableBalance, balance.AvailableBalance)
			bs.stateMutex.Lock()
			bs.availableBalance = balance.AvailableBalance
			bs.stateMutex.Unlock()
		}
	}

	bs.positionMutex.Lock()
	if position, exists := bs.positions[symbol]; exists {
		position.addExecution(result)
	} else {
		bs.positions[symbol] = &Position{
			Symbol:        symbol,
			TotalQuantity: result.FilledQty,
			TotalInvested: result.FilledQty * result.AvgPrice,
			AvgPrice:      result.AvgPrice,
			State:         PositionHolding,
		}
	}
	bs.positionMutex.Unlock()

	log.Printf("Bollinger: Buy completed for %s: %.6f @ %.4f (%.2f USDT), remaining: %.2f USDT",
		symbol, result.FilledQty, result.AvgPrice, executedValue, newBalance)

}

func (bs *BollingerStrategy) getVolumeThreshold() float64 {
	return bs.traderConfig.MedianVolumeThreshold
}

func (bs *BollingerStrategy) getBuyThreshold() float64 {
	return bs.traderConfig.BuyThreshold
}

func (bs *BollingerStrategy) getBalanceVerificationThreshold() float64 {
	return bs.traderConfig.BalanceVerificationThreshold
}
