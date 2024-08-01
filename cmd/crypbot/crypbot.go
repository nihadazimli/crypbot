package main

import (
	"context"
	"github.com/nihadazimli/crypbot/internal/broker_api/bybit"
	"github.com/nihadazimli/crypbot/internal/config"
	"github.com/nihadazimli/crypbot/pkg/financial"
	"github.com/nihadazimli/crypbot/pkg/model"
	"github.com/nihadazimli/crypbot/pkg/trader"
	"github.com/nihadazimli/crypbot/pkg/websocket"
	bybitAPI "github.com/wuhewuhe/bybit.go.api"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("Shutting down with full of grace...")
		cancel()
	}()

	cfg := config.GetConfig()

	klineReceived := make(chan model.WSInternalKline)
	closedKlineReceived := make(chan string, len(cfg.Symbols)+1)
	buyReady := make(chan string, len(cfg.Symbols)+1)
	sellReady := make(chan string, len(cfg.Symbols)+1)

	bybitClient := bybitAPI.NewBybitHttpClient(cfg.Broker.API.Key, cfg.Broker.API.Secret, bybitAPI.WithBaseURL(bybitAPI.TESTNET))
	bybitAdapter := &bybit.BybitAdapter{Client: bybitClient}

	go websocket.Serve(cfg, klineReceived)

	fe := financial.Start(klineReceived, closedKlineReceived, buyReady, cfg.Broker.API.Timeout)
	go fe.ProcessKline()
	go fe.ProcessRSI()
	go fe.Show()

	tc := trader.Start(buyReady, sellReady, bybitAdapter, fe)
	go tc.ProcessBuySignal(ctx)

	<-ctx.Done() // Block until context is cancelled
}
