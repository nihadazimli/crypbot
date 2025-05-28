// Package exchange is interface that implements interaction between exchange and application
package exchange

import (
	"context"

	"github.com/nihadazimli/crypbot/internal/model"
)

type MarketDataClient interface {
	StartKlineStream(ctx context.Context, params KlineStreamRequestParams) error
	GetOrderBook(ctx context.Context, params OrderBookRequestParams) (OrderBookResult, error)
	GetKlineHistory(ctx context.Context, params KlineHistoryParams) (KlineHistoryResult, error)
	GetPrecision(ctx context.Context, symbol string) (model.Precision, error)
	Shutdown()
}

type TradingClient interface {
	PlaceBuyOrder(ctx context.Context, params BuyOrderRequestParams) (BuyOrderResult, error)
	PlaceSellOrder(ctx context.Context, params SellOrderRequestParams) (SellOrderResult, error)
	GetAccountBalance(ctx context.Context) (AccountBalanceResult, error)
	AmendOrder(ctx context.Context, params AmendOrderRequestParams) (AmendOrderResult, error)
	CancelOrder(ctx context.Context, params CancelOrderRequestParams) (CancelOrderResult, error)
	CancelAllTPSLOrders(ctx context.Context, symbol string) error
	GetOrderStatus(ctx context.Context, params OrderStatusRequestParams) (OrderStatusResponse, error)
	GetSymbolBalance(ctx context.Context, symbol string) (SymbolBalanceResult, error)
	Shutdown()
}
