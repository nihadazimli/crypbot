package main

import (
	"github.com/nihadazimli/crypbot/internal/config"
	"github.com/nihadazimli/crypbot/pkg/financial"
	"github.com/nihadazimli/crypbot/pkg/model"
	"github.com/nihadazimli/crypbot/pkg/trader"
	"github.com/nihadazimli/crypbot/pkg/websocket"
)

func main() {
	cfg := config.GetConfig()

	klineReceived := make(chan model.InternalKline)
	rsiReady := make(chan string, len(cfg.Topics)+1)
	buyReady := make(chan string, len(cfg.Topics)+1)

	go websocket.Serve(cfg, klineReceived)
	fc := financial.NewFinancialClient(klineReceived, rsiReady, buyReady)

	go fc.ProcessKline()
	go fc.CalculateRSI()

	tc := trader.NewTraderClient(buyReady)

	go tc.ProcessBuySignal(fc)
	select {}

}
