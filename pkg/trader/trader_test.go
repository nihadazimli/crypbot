package trader

import (
	"github.com/nihadazimli/crypbot/internal/broker_api"
	"github.com/nihadazimli/crypbot/internal/config"
	"github.com/nihadazimli/crypbot/pkg/financial"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

type MockFinancialEngine struct {
	mock.Mock
	*financial.Engine
}

type MockMarket struct {
	mock.Mock
	broker_api.BrokerAPI
}

func (fe *MockFinancialEngine) GetRSIClosed(symbol string) (float64, bool) {
	args := fe.Called(symbol)
	return args.Get(0).(float64), args.Bool(1)
}

func (fe *MockFinancialEngine) GetVolumeValueMedian(symbol string) (float64, bool) {
	args := fe.Called(symbol)
	return args.Get(0).(float64), args.Bool(1)
}

func TestBuyConditionsMet(t *testing.T) {
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

	// Setup the mock and configuration
	mockFE := &MockFinancialEngine{}
	mockMarket := &MockMarket{}
	cfg := &config.Config{
		Trader: config.TraderConfig{
			RSIBuyValue:           20,
			MedianVolumeThreshold: 40000,
		},
	}
	trader := NewTrader(make(chan string, 10), cfg, mockMarket, mockFE)

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
