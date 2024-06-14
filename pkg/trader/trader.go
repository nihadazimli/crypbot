package trader

import (
	"fmt"
	"github.com/nihadazimli/crypbot/pkg/financial"
	"log"
	"strings"
)

type Client struct {
	buyReady  chan string
	positions map[string]bool
}

func NewTraderClient(buyReady chan string) *Client {
	return &Client{
		buyReady:  buyReady,
		positions: make(map[string]bool),
	}
}

func (tc *Client) ProcessBuySignal(fc *financial.Engine) {
	for topic := range tc.buyReady {
		if fc.RSIDataMap[topic] > 20.0 {
			log.Fatal("RSIDataMap too big")
		}
		coinSymbol := KlineSymbolToCoinSymbol(topic)

		go tc.buy(coinSymbol)
		go tc.ProcessSell(fc, coinSymbol, 1)

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
	//for fc.RSIDataMap[coinSymbol] < 40 || fc.TickerDataMap[coinSymbol] != stop_loss {
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

func KlineSymbolToCoinSymbol(coinKlineSymbol string) string {
	return coinKlineSymbol[strings.LastIndex(coinKlineSymbol, ".")+1:]
}
