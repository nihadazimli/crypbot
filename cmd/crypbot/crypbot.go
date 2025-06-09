// Package main is where application starts
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/exchange/bybit"
	"github.com/nihadazimli/crypbot/internal/financial"
	"github.com/nihadazimli/crypbot/internal/model"
	"github.com/nihadazimli/crypbot/internal/router"
	"github.com/nihadazimli/crypbot/internal/strategy"
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
			rsiTrader, err := trader.NewTrader(financialEngine, model.StrategyRSI)
			if err != nil {
				log.Printf("Failed to create RSI trader. Err:%v", err)
				return
			}
			rsiEventsChan := make(chan model.MarketEvent, len(cfg.Symbols)*4)
			eventRouter.RegisterStrategy(model.StrategyRSI, rsiEventsChan)

			rsiStrategy := strategy.NewRSIStrategy(
				financialEngine,
				rsiTrader,
				rsiEventsChan,
				cfg.Symbols,
			)

			go rsiStrategy.Start(ctx)
			log.Println("RSI Strategy started successfully!")

		case model.StrategyBollinger:
			bollingerTrader, err := trader.NewTrader(financialEngine, model.StrategyBollinger)
			if err != nil {
				log.Printf("Failed to create Bollinger trader. Err:%v", err)
				return
			}
			bollingerEventsChan := make(chan model.MarketEvent, len(cfg.Symbols)*4)
			eventRouter.RegisterStrategy(model.StrategyBollinger, bollingerEventsChan)

			bollingerStrategy, err := strategy.NewBollingerStrategy(
				financialEngine,
				bollingerTrader,
				bollingerEventsChan,
			)
			if err != nil {
				log.Printf("Failed to create Bollinger strategy. Err:%v", err)
				return
			}
			go func() {
				err := bollingerStrategy.Start(ctx)
				if err != nil {
					log.Printf("main - %v", err)
				}
			}()
			log.Println("Bollinger Strategy started successfully!")
		case model.StrategyDummy:
			dummyTrader, err := trader.NewTrader(
				financialEngine,
				model.StrategyDummy,
			)
			if err != nil {
				log.Printf("Failed to create Dummy trader. Err:%v", err)
				return
			}
			dummyStrategy := strategy.NewInteractiveDummyStrategy(dummyTrader, financialEngine, cfg.Symbols[0])
			go func() {
				err := dummyStrategy.Start(ctx)
				if err != nil {
					log.Printf("main - %v", err)
				}
			}()

		default:
			log.Printf("Unknown strategy: %s", strategyName)
		}
	}
	financialEngine.Start(ctx)
	<-ctx.Done()
}
