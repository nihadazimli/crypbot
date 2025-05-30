package trader

import (
	"context"
	"testing"
	"time"

	"github.com/nihadazimli/crypbot/internal/model"

	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/financial"

	"github.com/nihadazimli/crypbot/internal/exchange"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockFinancialEngine struct {
	mock.Mock
	financial.EngineInterface
}

func (fe *MockFinancialEngine) GetRSIClosed(symbol string) (float64, bool) {
	args := fe.Called(symbol)
	return args.Get(0).(float64), args.Bool(1)
}

func (fe *MockFinancialEngine) GetVolumeValueMedian(symbol string) (float64, bool) {
	args := fe.Called(symbol)
	return args.Get(0).(float64), args.Bool(1)
}

func (fe *MockFinancialEngine) GetCurrentPrice(symbol string) (float64, bool) {
	args := fe.Called(symbol)
	return args.Get(0).(float64), args.Bool(1)
}

func (fe *MockFinancialEngine) GetRSILive(symbol string) (float64, bool) {
	args := fe.Called(symbol)
	return args.Get(0).(float64), args.Bool(1)
}

type MockExchange struct {
	mock.Mock
	exchange.Exchange
}

func (b *MockExchange) GetRSIClosed(symbol string) (float64, bool) {
	//TODO implement me
	panic("implement me")
}

func (b *MockExchange) GetRSILive(symbol string) (float64, bool) {
	//TODO implement me
	panic("implement me")
}

func (b *MockExchange) GetVolumeValueMedian(symbol string) (float64, bool) {
	//TODO implement me
	panic("implement me")
}

func (b *MockExchange) GetCurrentPrice(symbol string) (float64, bool) {
	//TODO implement me
	panic("implement me")
}

func (b *MockExchange) GetMovingAverages(symbol string) (model.MovingAverage, error) {
	//TODO implement me
	panic("implement me")
}

func (b *MockExchange) Show() {
	//TODO implement me
	panic("implement me")
}

func (b *MockExchange) GetAccountBalance(ctx context.Context) (float64, error) {
	args := b.Called(ctx)
	return args.Get(0).(float64), args.Error(1)
}

func (b *MockExchange) GetOrderBook(
	ctx context.Context,
	params exchange.OrderBookRequestParams,
) (exchange.OrderBookResult, error) {
	args := b.Called(ctx, params)
	return args.Get(0).(exchange.OrderBookResult), args.Error(1)
}

func (b *MockExchange) PlaceBuyOrder(
	ctx context.Context,
	params exchange.BuyOrderRequestParams,
) (exchange.BuyOrderResult, error) {
	args := b.Called(ctx, params)
	return args.Get(0).(exchange.BuyOrderResult), args.Error(1)
}

func (b *MockExchange) PlaceSellOrder(
	ctx context.Context,
	params exchange.SellOrderRequestParams,
) (exchange.SellOrderResult, error) {
	args := b.Called(ctx, params)
	return args.Get(0).(exchange.SellOrderResult), args.Error(1)
}

func TestTrader_BuyConditionsMet(t *testing.T) {
	// Define test cases
	testCases := []struct {
		name         string
		symbol       string
		rsi          float64
		rsiExists    bool
		volume       float64
		volumeExists bool
		expected     bool
	}{
		{
			name:         "Valid RSI and volume - should buy",
			symbol:       "TESTCOIN",
			rsi:          19.9,
			rsiExists:    true,
			volume:       42000.0,
			volumeExists: true,
			expected:     true,
		},
		{
			name:         "Invalid RSI - should not buy",
			symbol:       "BADRSI",
			rsi:          21.0,
			rsiExists:    true,
			volume:       42000.0,
			volumeExists: true,
			expected:     false,
		},
		{
			name:         "Invalid volume - should not buy",
			symbol:       "BADVOLUME",
			rsi:          19.9,
			rsiExists:    true,
			volume:       30000.0,
			volumeExists: true,
			expected:     false,
		},
		{
			name:         "Missing data - should not buy",
			symbol:       "NODATA",
			rsi:          0.0,
			rsiExists:    false,
			volume:       0.0,
			volumeExists: false,
			expected:     false,
		},
	}

	// Set up the mock and configuration
	mockFE := &MockFinancialEngine{}
	mockExchange := &MockExchange{}
	cfg := &config.Config{
		Trader: config.TraderConfig{
			RSIBuyValue:           20,
			MedianVolumeThreshold: 40000,
		},
	}
	trader := NewTrader(make(chan string, 1), make(chan string, 1), cfg, mockExchange, mockFE)

	// Configure the mock based on test cases
	for _, tc := range testCases {
		mockFE.On("GetRSIClosed", tc.symbol).Return(tc.rsi, tc.rsiExists).Once()
		mockFE.On("GetVolumeValueMedian", tc.symbol).Return(tc.volume, tc.volumeExists).Once()
	}

	// Execute the test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := trader.checkBuyConditions(tc.symbol)
			assert.Equal(t, tc.expected, result)
		})
	}

}

func TestTrader_ProcessBuySignal(t *testing.T) {

	testCases := []struct {
		name              string
		symbol            string
		rsi               float64
		rsiExists         bool
		volume            float64
		volumeExists      bool
		currPrice         float64
		currPriceExists   bool
		orderBookBidPrice float64
		orderBookAskPrice float64
		accountBalance    float64
		orderAmount       float64
		accountBalanceErr error
		expected          bool
	}{
		{
			name:              "Valid RSI and volume,order placed successfully - should buy",
			symbol:            "TESTCOIN",
			rsi:               19.9,
			rsiExists:         true,
			volume:            42000.0,
			volumeExists:      true,
			currPrice:         340.44,
			currPriceExists:   true,
			orderBookBidPrice: 340.50,
			orderBookAskPrice: 340.30,
			accountBalance:    100.0,
			orderAmount:       3.405,
			accountBalanceErr: nil,
			expected:          true,
		},
	}

	mockFE := &MockFinancialEngine{}
	mockExchange := &MockExchange{}

	ctx := context.Background()
	cfg := &config.Config{
		Trader: config.TraderConfig{
			RSIBuyValue:           20,
			MedianVolumeThreshold: 40000,
		},
	}
	buyReady := make(chan string, 1)
	sellReady := make(chan string, 1)

	trader := NewTrader(buyReady, sellReady, cfg, mockExchange, mockFE)

	// Configure mocks for internal trader functions
	for _, tc := range testCases {
		mockFE.On("GetRSIClosed", tc.symbol).Return(tc.rsi, tc.rsiExists).Once()
		mockFE.On("GetVolumeValueMedian", tc.symbol).Return(tc.volume, tc.volumeExists).Once()
		mockFE.On("GetCurrentPrice", tc.symbol).Return(tc.currPrice, tc.currPriceExists).Once()
		mockExchange.On("GetAccountBalance", mock.Anything).
			Return(tc.accountBalance, tc.accountBalanceErr).
			Once()
		mockExchange.On("GetOrderBook", mock.Anything, mock.MatchedBy(func(params exchange.OrderBookRequestParams) bool {
			return params.Symbol == tc.symbol && params.Limit == 1
		})).
			Return(exchange.OrderBookResult{
				Ask: []exchange.OrderBookDetail{{Price: tc.orderBookAskPrice, Quantity: 1}},
				Bid: []exchange.OrderBookDetail{{Price: tc.orderBookBidPrice, Quantity: 1}},
			}, nil).
			Once()
		mockExchange.On("PlaceBuyOrder", mock.Anything, mock.MatchedBy(func(params exchange.BuyOrderRequestParams) bool {
			return params.Symbol == tc.symbol && params.OrderAmount == tc.accountBalance &&
				params.LimitPrice == tc.orderBookBidPrice
		})).
			Return(exchange.BuyOrderResult{Success: true, Details: exchange.BuyOrderResultDetails{OrderId: "1223", OrderLinkId: "23323", OrderAmount: tc.orderAmount}}, nil)
	}

	// Execute tests
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buyReady <- tc.symbol
			go trader.ProcessBuySignal(ctx)
			time.Sleep(1 * time.Second)
			assert.Equal(t, trader.activeOrders[tc.symbol], tc.orderAmount)
			assert.Equal(t, len(sellReady), 1)
		})

	}
}

func TestTrader_ProcessSellSignal(t *testing.T) {
	testCases := []struct {
		name               string
		symbol             string
		bestAskPrice       float64
		currentPrice       float64
		currentPriceExists bool
		limitPrice         float64
		currentPosition    float64
		rsiLive            float64
		rsiLiveStatus      bool
		positionExists     bool
	}{
		{
			name:               "Valid Sell Signal",
			symbol:             "TESTCOIN",
			bestAskPrice:       330.4,
			currentPrice:       330.4,
			currentPriceExists: true,
			limitPrice:         330.4,
			currentPosition:    1,
			rsiLive:            40.4,
			rsiLiveStatus:      true,
			positionExists:     false,
		},
	}
	mockFE := &MockFinancialEngine{}
	mockExchange := &MockExchange{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &config.Config{
		Trader: config.TraderConfig{
			RSISellValue:          40,
			MedianVolumeThreshold: 40000,
			SellCheckInterval:     1,
		},
	}

	sellReady := make(chan string, 1)

	trader := NewTrader(make(chan string, 1), sellReady, cfg, mockExchange, mockFE)

	//Configure the mocks
	for _, tc := range testCases {
		mockFE.On("GetRSILive", tc.symbol).Return(tc.rsiLive, tc.rsiLiveStatus)
		mockExchange.On("GetOrderBook", mock.Anything, mock.MatchedBy(func(params exchange.OrderBookRequestParams) bool {
			return params.Symbol == tc.symbol && params.Limit == 1
		})).
			Return(exchange.OrderBookResult{
				Ask: []exchange.OrderBookDetail{{Price: tc.bestAskPrice, Quantity: 1}},
				Bid: []exchange.OrderBookDetail{{Price: 0, Quantity: 1}},
			}, nil).
			Once()
		mockFE.On("GetCurrentPrice", tc.symbol).
			Return(tc.currentPrice, tc.currentPriceExists).
			Once()
		mockExchange.On("PlaceSellOrder", mock.Anything, mock.MatchedBy(func(params exchange.SellOrderRequestParams) bool {
			return params.Symbol == tc.symbol && params.OrderAmount == tc.currentPosition &&
				params.LimitPrice == tc.limitPrice
		})).
			Return(exchange.SellOrderResult{
				Success: true,
				Details: exchange.SellOrderResultDetails{
					OrderId:     "123",
					OrderLinkId: "123",
					OrderAmount: tc.currentPosition}}, nil).
			Once()
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sellReady <- tc.symbol
			trader.activeOrders[tc.symbol] = tc.currentPosition
			go trader.ProcessSellSignal(ctx)
			time.Sleep(3 * time.Second)
			_, exists := trader.activeOrders[tc.symbol]
			assert.Equal(t, tc.positionExists, exists)
		})
	}
}
