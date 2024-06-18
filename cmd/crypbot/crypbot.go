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
	rsiReady := make(chan string, len(cfg.Symbols)+1)
	buyReady := make(chan string, len(cfg.Symbols)+1)

	go websocket.Serve(cfg, klineReceived)

	fe := financial.Start(klineReceived, rsiReady, buyReady, cfg.Broker.API.Timeout)

	go fe.ProcessKline()
	go fe.ProcessRSI()

	tc := trader.NewTraderClient(buyReady)

	go tc.ProcessBuySignal(fe)

	select {}
}
