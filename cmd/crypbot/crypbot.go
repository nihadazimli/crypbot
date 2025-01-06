// Package main is where application starts
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/broker_api/bybit"
	"github.com/nihadazimli/crypbot/internal/financial"
	"github.com/nihadazimli/crypbot/internal/trader"
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

	buyReady := make(chan string, len(cfg.Symbols)+1)
	sellReady := make(chan string, len(cfg.Symbols)+1)

	bybitAdapter := bybit.NewBybitAdapter(ctx)

	fe := financial.Start(ctx, bybitAdapter, buyReady)

	tc := trader.Start(buyReady, sellReady, bybitAdapter, fe)
	go tc.ProcessBuySignal(ctx)
	go tc.ProcessSellSignal(ctx)

	<-ctx.Done()
}
