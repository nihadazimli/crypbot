package trader

import (
	"fmt"
	"github.com/nihadazimli/crypbot/internal/config"
	"github.com/nihadazimli/crypbot/pkg/financial"
	"log"
)

type Client struct {
	buyReady  chan string
	positions map[string]bool
	cfg       *config.Config
}

func NewTraderClient(buyReady chan string) *Client {
	return &Client{
		buyReady:  buyReady,
		cfg:       config.GetConfig(),
		positions: make(map[string]bool),
	}
}

func (tc *Client) ProcessBuySignal(fc *financial.Engine) {
	for symbol := range tc.buyReady {
		if fc.RSIDataClosed[symbol] > tc.cfg.Trader.RSIBuyValue {
			log.Fatal("RSIDataClosed too big")
		}

		go tc.buy(symbol)
		go tc.ProcessSell(fc, symbol, 1)

	}
}
func (tc *Client) buy(coinSymbol string) {
	if !(tc.positions[coinSymbol]) {
		fmt.Println("Process buy: ", coinSymbol) // I will implement buying
		tc.positions[coinSymbol] = true
	}

}

func (tc *Client) ProcessSell(fc *financial.Engine, coinSymbol string, stop_loss float64) {
	//if !(slices.Contains(tc.positions, coinSymbol)) {
	//
	//}
	//
	//for fc.RSIDataClosed[coinSymbol] < 40 || fc.TickerDataMap[coinSymbol] != stop_loss {
	//
	//}
	//
	if tc.positions[coinSymbol] {
		fmt.Println("Process sell: ", coinSymbol)
		delete(tc.positions, coinSymbol)
	}
}

func (tc *Client) sell(coinKlineSymbol string) {

}
