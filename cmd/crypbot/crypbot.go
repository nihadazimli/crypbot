package main

import (
	"context"
	"fmt"
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
	bybitClient := bybitAPI.NewBybitHttpClient(cfg.Broker.API.Key, cfg.Broker.API.Secret, bybitAPI.WithBaseURL(bybitAPI.MAINNET))
	bybitAdapter := &bybit.BybitAdapter{HTTPClient: bybitClient}

	fe := financial.Start(klineReceived, closedKlineReceived, buyReady, cfg.Broker.API.Timeout)
	tc := trader.Start(buyReady, sellReady, bybitAdapter, fe)
	go websocket.Serve(cfg, klineReceived)

	go fe.ProcessKline()
	go fe.ProcessRSI()
	//go fe.Show()

	balance, err := bybitAdapter.GetAccountBalance(ctx)
	if err != nil {
		log.Printf("Cannot get balance. err: %v\n", err)
	}
	fmt.Println(balance)
	//limitPrice := 54100.10
	//stopLimit := 54000.4
	symbol := "BTCUSDT"
	//buyParams := broker_api.BuyOrderRequestParams{
	//	Symbol:      symbol,
	//	LimitPrice:  limitPrice,
	//	OrderAmount: (balance.Total - 0.3) / limitPrice,
	//	StopLimit:   stopLimit,
	//}
	//buyParams, err := tc.CalculateBuyOrderDetails(ctx, symbol)
	//if err != nil {
	//	log.Printf("Cannot get order details. err: %v\n", err)
	//}
	//fmt.Println(buyParams)
	sellParams, err := tc.CalculateSellOrderDetails(ctx, symbol)
	if err != nil {
		log.Printf("Cannot get order details. err: %v\n", err)
	}
	order, err := bybitAdapter.PlaceSellOrder(ctx, sellParams)

	if err != nil {
		log.Printf("Cannot place buy order %v\n", err)
	}

	log.Println(order)
	//go tc.ProcessBuySignal(ctx)

	<-ctx.Done() // Block until context is cancelled
}
