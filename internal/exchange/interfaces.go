// Package exchange is interface that implements interaction between exchange and application
package exchange

import (
	"context"
)

type MarketDataClient interface {
	StartKlineStream(ctx context.Context, params KlineStreamRequestParams) error
	GetKlineHistory(ctx context.Context, params KlineHistoryParams) (KlineHistoryResult, error)
	Shutdown()
}

type TradingClient interface {
	PlaceBuyOrder(ctx context.Context, params BuyOrderRequestParams) (BuyOrderResult, error)
	PlaceSellOrder(ctx context.Context, params SellOrderRequestParams) (SellOrderResult, error)
	GetAccountBalance(ctx context.Context) (AccountBalanceResult, error)
	GetOrderBook(ctx context.Context, params OrderBookRequestParams) (OrderBookResult, error)
	AmendOrder(ctx context.Context, params AmendOrderRequestParams) (AmendOrderResult, error)
	CancelOrder(ctx context.Context, params CancelOrderRequestParams) (CancelOrderResult, error)
	CancelAllTPSLOrders(ctx context.Context, symbol string) error
	GetOrderStatus(ctx context.Context, params OrderStatusRequestParams) (OrderStatusResponse, error)
	GetSymbolBalance(ctx context.Context, symbol string) (SymbolBalanceResult, error)
	Shutdown()
}
