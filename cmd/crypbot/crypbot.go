// Package main is where application starts
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nihadazimli/crypbot/internal/order"
	"github.com/nihadazimli/crypbot/internal/router"
	"github.com/nihadazimli/crypbot/internal/strategy"
	"github.com/nihadazimli/crypbot/internal/trader"

	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/exchange/bybit"

	"github.com/nihadazimli/crypbot/internal/model"

	"github.com/nihadazimli/crypbot/internal/financial"
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
		time.Sleep(30 * time.Second)
		log.Println("Forcing exit after timeout")
		os.Exit(1)
	}()
	cfg := config.GetConfig()

	marketEventsChan := make(chan model.MarketEvent, len(cfg.Symbols)*4)
	eventRouter := router.NewRouter(ctx, marketEventsChan)
	eventRouter.Start(ctx)

	marketClient := bybit.NewMarketDataClient()
	financialEngine := financial.NewFinancialEngine(marketClient, marketEventsChan)
	enabledStrategies := config.GetEnabledStrategies()

	for strategyName := range enabledStrategies {
		switch strategyName {
		case model.StrategyRSI:
			rsiTradingClient, err := bybit.NewTradingClientForStrategy(model.StrategyRSI)
			if err != nil {
				log.Printf("Failed to start exchange for RSI. Err:%v", err)
				return
			}
			rsiConfig, err := config.GetRSIConfig()
			if err != nil {
				log.Printf("Failed to load config for RSI. Err:%v", err)
				return
			}
			rsiOrderManager := order.NewOrderManager(rsiTradingClient)
			rsiTrader := trader.NewTrader(
				&rsiConfig.Trader.CommonTraderConfig,
				rsiTradingClient,
				financialEngine,
				rsiOrderManager,
			)

			rsiEventsChan := make(chan model.MarketEvent, len(cfg.Symbols)*4)
			eventRouter.RegisterStrategy(model.StrategyRSI, rsiEventsChan)

			rsiStrategy := strategy.NewRSIStrategy(
				rsiConfig.Credentials.SubAccountID,
				financialEngine,
				rsiTrader,
				rsiEventsChan,
				cfg.Symbols,
			)

			go rsiStrategy.Start(ctx)
			log.Println("RSI Strategy started successfully!")

		case model.StrategyBollinger:
			_, err := bybit.NewTradingClientForStrategy("bollinger")
			if err != nil {
				log.Printf("Failed to start exchange for bolinger. Err:%v", err)
				return
			}
			log.Printf("Not implemented yet!")
		default:
			log.Printf("Unknown strategy: %s", strategyName)
		}
	}
	financialEngine.Start(ctx)
	<-ctx.Done()
}
