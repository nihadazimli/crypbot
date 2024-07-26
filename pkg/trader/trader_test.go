package trader

import (
	"github.com/nihadazimli/crypbot/internal/config"
	"github.com/nihadazimli/crypbot/pkg/financial"
	"github.com/nihadazimli/crypbot/pkg/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	bybit "github.com/wuhewuhe/bybit.go.api"
	"testing"
)

type baseTestSuite struct {
	suite.Suite
	bybitClient            *MockedBybitClient
	financialEngine        *MockFinancialEngine
	apiKey                 string
	apiSecret              string
	baseURL                string
	dummyChan              chan string
	dummyInternalKlineChan chan model.WSInternalKline
}

func (s *baseTestSuite) SetupTest() {
	s.apiKey = "dummy"
	s.apiSecret = "dummySecret"
	s.baseURL = "http://test.com"
	s.bybitClient = newMockedBybitClient(s.apiKey, s.apiSecret, s.baseURL)
	cfg := &config.Config{}
	s.dummyChan = make(chan string)
	s.dummyInternalKlineChan = make(chan model.WSInternalKline)
	s.financialEngine = newMockedFinancialEngine(cfg, s.dummyInternalKlineChan, s.dummyChan, s.dummyChan, 1)
}

type MockFinancialEngine struct {
	mock.Mock
	*financial.Engine
}

func newMockedFinancialEngine(cfg *config.Config, klineReceived chan model.WSInternalKline, closedKlineReceived chan string, buyReady chan string, httpTimeoutSec int) *MockFinancialEngine {
	m := new(MockFinancialEngine)
	m.Engine = financial.NewFinancialEngine(cfg, klineReceived, closedKlineReceived, buyReady, httpTimeoutSec)
	return m
}

type MockedBybitClient struct {
	mock.Mock
	*bybit.Client
}

func newMockedBybitClient(apiKey, apiSecret, baseURL string) *MockedBybitClient {
	m := new(MockedBybitClient)
	m.Client = bybit.NewBybitHttpClient(apiKey, apiSecret, bybit.WithBaseURL(baseURL))
	return m
}

func (fe *MockFinancialEngine) GetRSIClosed(symbol string) (float64, bool) {
	args := fe.Called(symbol)
	return args.Get(0).(float64), args.Bool(1)
}

func (fe *MockFinancialEngine) GetVolumeValueMedian(symbol string) (float64, bool) {
	args := fe.Called(symbol)
	return args.Get(0).(float64), args.Bool(1)
}

func (s *baseTestSuite) TestBuyConditionsMet() {
	coinSymbol := "TESTCOIN"
	s.financialEngine.On("GetRSIClosed", coinSymbol).Return(19.9, true)
	s.financialEngine.On("GetVolumeValueMedian", coinSymbol).Return(42000, true)

	cfg := &config.Config{
		Trader: config.TraderConfig{
			RSIBuyValue:           20,
			MedianVolumeThreshold: 40000,
		},
	}

	traderClient := NewTraderClient(make(chan string, 10), cfg, s.bybitClient.Client)

	assert.True(s.T(), traderClient.buyConditionsMet(s.financialEngine.Engine, coinSymbol))
}

func TestRunner(t *testing.T) {
	suite.Run(t, new(baseTestSuite))
}

//func (tc *Client) buyConditionsMet(fc *financial.Engine, symbol string) bool {
//
//	rsiClosed, ok := fc.GetRSIClosed(symbol)
//	if !ok {
//		log.Printf("No RSI data available for BTCUSDT")
//		return false
//	}
//
//	volumeMedian, ok := fc.GetVolumeValueMedian(symbol)
//	if !ok {
//		log.Printf("No volume data available for BTCUSDT")
//		return false
//	}
//
//	if rsiClosed > tc.cfg.Trader.RSIBuyValue {
//		log.Printf("RSIClosed too big")
//		return false
//	}
//
//	if tc.positions[symbol] {
//		log.Printf("Already has position for %s", symbol)
//		return false
//	}
//
//	if volumeMedian < tc.cfg.Trader.MedianVolumeThreshold {
//		log.Printf("Median Volume below threshold")
//		return false
//	}
//
//	return true
//}
