// Package main is where application starts
package main

import (
	"context"
	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/broker_api/bybit"
	"github.com/nihadazimli/crypbot/internal/financial"
	"github.com/nihadazimli/crypbot/internal/trader"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
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

	closedKlineReceived := make(chan string, len(cfg.Symbols)+1)
	buyReady := make(chan string, len(cfg.Symbols)+1)
	sellReady := make(chan string, len(cfg.Symbols)+1)

	bybitAdapter := bybit.NewBybitAdapter()

	//params := broker_api.SellOrderRequestParams{Symbol: "BTCUSDT", LimitPrice: 70000, OrderAmount: 0.000110}
	//order, err := bybitAdapter.PlaceSellOrder(ctx, params)
	//if err != nil {
	//	log.Printf("cannot place order: err: %s", err)
	//	ctx.Done()
	//}
	//orderId, _ := strconv.Atoi(order.Details.OrderId)
	//log.Println(orderId)

	//paramsCancel := broker_api.CancelOrderRequestParams{OrderId: orderId, Symbol: "BTCUSDT"}
	//cancelStatus, err := bybitAdapter.CancelOrder(ctx, paramsCancel)
	//if err != nil {
	//	log.Printf("Cannot cancel sell order. Err: %s", err)
	//}
	//log.Println(cancelStatus.OrderId)

	fe := financial.Start(ctx, bybitAdapter, closedKlineReceived, buyReady, cfg.Broker.API.Timeout)
	go fe.ProcessKline()
	go fe.ProcessRSI()
	//go fe.Show()
	//
	tc := trader.Start(buyReady, sellReady, bybitAdapter, fe)
	go tc.ProcessBuySignal(ctx)
	go tc.ProcessSellSignal(ctx)
	symbol := "BEAMUSDT"

	time.Sleep(2 * time.Second)
	buyReady <- symbol

	//sellDetails, err := tc.CalculateSellOrderDetails(ctx, symbol)
	//if err != nil {
	//	log.Printf("Cannot calculate sell details. Err: %v", err)
	//}
	//log.Printf("sell details:%v", sellDetails)
	//order, err := bybitAdapter.PlaceSellOrder(ctx, sellDetails)
	//if err != nil {
	//	log.Printf("Cannot place sell order. Err: %v", err)
	//}
	//log.Printf("Order placed successfully. Order id: %s", order.Details.OrderId)

	<-ctx.Done() // Block until context is cancelled
}
