// Package exchange is interface that implements interaction between exchange and application
package exchange

import (
	"context"
)

type Exchange interface {
	PlaceBuyOrder(ctx context.Context, params BuyOrderRequestParams) (BuyOrderResult, error)
	PlaceSellOrder(ctx context.Context, params SellOrderRequestParams) (SellOrderResult, error)
	GetOrderBook(ctx context.Context, params OrderBookRequestParams) (OrderBookResult, error)
	GetAccountBalance(ctx context.Context) (AccountBalanceResult, error)
	AmendOrder(ctx context.Context, params AmendOrderRequestParams) (AmendOrderResult, error)
	CancelOrder(ctx context.Context, params CancelOrderRequestParams) (CancelOrderResult, error)
	CancelAllTPSLOrders(ctx context.Context, symbol string) error
	StartKlineStream(ctx context.Context, params KlineStreamRequestParams) error
	GetSymbolBalance(ctx context.Context, symbol string) (SymbolBalanceResult, error)
	GetOrderStatus(ctx context.Context, orderId string) (OrderStatus, error)
	GetRemainingQtyForPartialOrder(
		ctx context.Context,
		params OpenOrderRequestParams,
	) (float64, error)
	GetKlineHistory(ctx context.Context, params KlineHistoryParams) (KlineHistoryResult, error)
	StartOrderStream() error
	StopOrderStream() error
}
