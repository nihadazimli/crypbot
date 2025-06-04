package strategy

import (
	"context"

	"github.com/nihadazimli/crypbot/internal/model"
)

type ExecutionResult struct {
	Symbol       string
	Side         model.OrderSide
	Status       model.OrderStatus
	RequestedQty float64
	FilledQty    float64
	AvgPrice     float64
}

type BalanceInfo struct {
	TotalBalance     float64
	AvailableBalance float64
	//Positions        map[string]PositionBalance
}

type PositionBalance struct {
	Symbol        string
	TotalQuantity float64
}

type TraderService interface {
	// Common methods
	ExecuteOrder(
		ctx context.Context,
		symbol string,
		orderType model.OrderType,
		side model.OrderSide,
		checker model.ConditionChecker,
	) (*ExecutionResult, error)

	// Bollinger methods
	GetBalance(ctx context.Context) (*BalanceInfo, error)
	GetSymbolBalance(ctx context.Context, symbol string) (*PositionBalance, error)
}

type FinancialService interface {
	// RSI methods
	GetRSIClosed(symbol string) (float64, bool)
	GetRSILive(symbol string) (float64, bool)

	// Bollinger methods
	GetZScore(symbol string) (float64, bool)

	// Common methods
	GetVolumeValueMedian(symbol string) (float64, bool)
	GetMovingAverages(symbol string) (model.MovingAverage, error)
}
