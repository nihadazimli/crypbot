package trader

import (
	"github.com/nihadazimli/crypbot/internal/config"
	"github.com/nihadazimli/crypbot/internal/market_api"
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
	market_api.MarketAPI
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
	coinSymbol := "TESTCOIN"
	mockFE := &MockFinancialEngine{}
	mockMarket := &MockMarket{}
	mockFE.On("GetRSIClosed", coinSymbol).Return(19.9, true)
	mockFE.On("GetVolumeValueMedian", coinSymbol).Return(42000.0, true)

	cfg := &config.Config{
		Trader: config.TraderConfig{
			RSIBuyValue:           20,
			MedianVolumeThreshold: 40000,
		},
	}

	traderClient := NewTraderClient(make(chan string, 10), cfg, mockMarket)

	assert.True(t, traderClient.buyConditionsMet(mockFE, coinSymbol))
}
