package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nihadazimli/crypbot/internal/order"

	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/exchange"
	"github.com/nihadazimli/crypbot/internal/financial"
	"github.com/nihadazimli/crypbot/internal/trader"
)

func main() {
	// Define command-line flags
	symbolFlag := flag.String("symbol", "BTCUSDT", "Trading symbol (e.g., BTCUSDT)")
	sellCheckFlag := flag.Bool("check", true, "Enable sell with check")

	// Parse command-line flags
	flag.Parse()

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
	cfg.SetSymbols([]string{*symbolFlag})

	buyReady := make(chan string, len(cfg.Symbols)+1)
	sellReady := make(chan string, len(cfg.Symbols)+1)

	bybitAdapter := exchange.NewBybitAdapter(ctx)

	fe := financial.Start(ctx, bybitAdapter, buyReady)
	orderManager := order.NewOrderManager(bybitAdapter)

	tc := trader.NewTrader(buyReady, sellReady, bybitAdapter, fe, orderManager)

	err := tc.SellCoin(ctx, *symbolFlag, *sellCheckFlag)

	if err != nil {
		log.Println(err)
	}
	<-ctx.Done()

}
