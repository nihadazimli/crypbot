package trader

import (
	"context"
	"testing"
	"time"

	"github.com/nihadazimli/crypbot/internal/broker_api"
	"github.com/nihadazimli/crypbot/internal/config"
	"github.com/nihadazimli/crypbot/pkg/financial"
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

type MockBroker struct {
	mock.Mock
	broker_api.BrokerAPI
}

func (b *MockBroker) GetAccountBalance(ctx context.Context) (float64, error) {
	args := b.Called(ctx)
	return args.Get(0).(float64), args.Error(1)
}

func (b *MockBroker) GetOrderBook(ctx context.Context, params broker_api.OrderBookRequestParams) (broker_api.OrderBookResponse, error) {
	args := b.Called(ctx, params)
	return args.Get(0).(broker_api.OrderBookResponse), args.Error(1)
}

func (b *MockBroker) PlaceBuyOrder(ctx context.Context, params broker_api.BuyOrderRequestParams) (broker_api.BuyOrderResult, error) {
	args := b.Called(ctx, params)
	return args.Get(0).(broker_api.BuyOrderResult), args.Error(1)
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
	mockBroker := &MockBroker{}
	cfg := &config.Config{
		Trader: config.TraderConfig{
			RSIBuyValue:           20,
			MedianVolumeThreshold: 40000,
		},
	}
	trader := NewTrader(make(chan string, 1), make(chan string, 1), cfg, mockBroker, mockFE)

	// Configure the mock based on test cases
	for _, tc := range testCases {
		mockFE.On("GetRSIClosed", tc.symbol).Return(tc.rsi, tc.rsiExists).Once()
		mockFE.On("GetVolumeValueMedian", tc.symbol).Return(tc.volume, tc.volumeExists).Once()
	}

	// Execute the test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := trader.buyConditionsMet(tc.symbol)
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
			accountBalanceErr: nil,
			expected:          true,
		},
	}

	mockFE := &MockFinancialEngine{}
	mockBroker := &MockBroker{}

	ctx := context.Background()
	cfg := &config.Config{
		Trader: config.TraderConfig{
			RSIBuyValue:           20,
			MedianVolumeThreshold: 40000,
		},
	}
	buyReady := make(chan string, 1)
	sellReady := make(chan string, 1)

	trader := NewTrader(buyReady, sellReady, cfg, mockBroker, mockFE)

	// Configure mocks for internal trader functions
	for _, tc := range testCases {
		mockFE.On("GetRSIClosed", tc.symbol).Return(tc.rsi, tc.rsiExists).Once()
		mockFE.On("GetVolumeValueMedian", tc.symbol).Return(tc.volume, tc.volumeExists).Once()
		mockFE.On("GetCurrentPrice", tc.symbol).Return(tc.currPrice, tc.currPriceExists).Once()
		mockBroker.On("GetAccountBalance", mock.Anything).Return(tc.accountBalance, tc.accountBalanceErr).Once()
		mockBroker.On("GetOrderBook", mock.Anything, mock.MatchedBy(func(params broker_api.OrderBookRequestParams) bool {
			return params.Symbol == tc.symbol && params.Limit == 1
		})).Return(broker_api.OrderBookResponse{
			Ask: []broker_api.OrderBookDetail{{Price: tc.orderBookAskPrice, Quantity: 1}},
			Bid: []broker_api.OrderBookDetail{{Price: tc.orderBookBidPrice, Quantity: 1}},
		}, nil).Once()
		mockBroker.On("PlaceBuyOrder", mock.Anything, mock.MatchedBy(func(params broker_api.BuyOrderRequestParams) bool {
			return params.Symbol == tc.symbol && params.OrderAmount == tc.accountBalance && params.LimitPrice == tc.orderBookBidPrice
		})).Return(broker_api.BuyOrderResult{Success: true, Details: broker_api.BuyResultDetails{OrderId: "1223", OrderLinkId: "23323"}}, nil)
	}

	// Execute tests
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buyReady <- tc.symbol
			go trader.ProcessBuySignal(ctx)
			time.Sleep(1 * time.Second)
			assert.Equal(t, trader.positions[tc.symbol], tc.expected)
			assert.Equal(t, len(sellReady), 1)
		})

	}
}
