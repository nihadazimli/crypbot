// internal/strategy/interactive_dummy.go
package strategy

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/nihadazimli/crypbot/internal/model"
)

type InteractiveDummyStrategy struct {
	trader    TraderService
	financial FinancialService

	// Interactive control
	reader *bufio.Reader
	mutex  sync.Mutex

	testSymbol string
	testSide   model.OrderSide
}

func NewInteractiveDummyStrategy(
	trader TraderService,
	financial FinancialService,
	symbol string,
) *InteractiveDummyStrategy {
	return &InteractiveDummyStrategy{
		trader:     trader,
		financial:  financial,
		reader:     bufio.NewReader(os.Stdin),
		testSymbol: symbol,
		testSide:   model.OrderSideBuy,
	}
}

func (ids *InteractiveDummyStrategy) Start(ctx context.Context) error {
	log.Printf("Interactive Dummy Strategy: Starting manual test mode")
	log.Printf("Commands: 'buy' - test buy order, 'sell' - test sell order, 'balance' - check balance, 'exit' - quit")

	for {
		fmt.Print("Enter command (buy/sell/balance/exit): ")
		input, err := ids.reader.ReadString('\n')
		if err != nil {
			return err
		}

		command := strings.TrimSpace(strings.ToLower(input))

		switch command {
		case "buy":
			ids.testSide = model.OrderSideBuy
			if err := ids.runInteractiveTest(ctx); err != nil {
				log.Printf("Buy test failed: %v", err)
			}
		case "sell":
			ids.testSide = model.OrderSideSell
			if err := ids.runInteractiveTest(ctx); err != nil {
				log.Printf("Sell test failed: %v", err)
			}
		case "balance":
			if err := ids.checkBalance(ctx); err != nil {
				log.Printf("Balance check failed: %v", err)
			}
		case "exit":
			log.Printf("Exiting interactive test mode")
			return nil
		default:
			log.Printf("Unknown command: %s", command)
		}
	}
}

func (ids *InteractiveDummyStrategy) runInteractiveTest(ctx context.Context) error {
	log.Printf("Starting %s order test. You'll be prompted for each condition check.", ids.testSide)

	var result *ExecutionResult
	var err error

	result, err = ids.trader.ExecuteOrder(
		ctx,
		ids.testSymbol,
		model.OrderTypeLimit,
		ids.testSide,
		ids.makeInteractiveChecker(),
	)

	if err != nil {
		log.Printf("Order failed: %v", err)
	} else {
		log.Printf("Order completed: %+v", result)
	}

	return nil
}

func (ids *InteractiveDummyStrategy) makeInteractiveChecker() model.ConditionChecker {
	return func(ctx context.Context, symbol string) model.OrderConditionResult {
		ids.mutex.Lock()
		defer ids.mutex.Unlock()

		for {
			fmt.Printf("Condition check for %s %s - Enter 'met', 'wait', or 'cancel': ", ids.testSide, symbol)
			input, err := ids.reader.ReadString('\n')
			if err != nil {
				log.Printf("Error reading input: %v", err)
				return model.ConditionCancel
			}

			command := strings.TrimSpace(strings.ToLower(input))

			switch command {
			case "met", "m":
				log.Printf("Returning ConditionMet")
				return model.ConditionMet
			case "wait", "w":
				log.Printf("Returning ConditionWait")
				return model.ConditionWait
			case "cancel", "c":
				log.Printf("Returning ConditionCancel")
				return model.ConditionCancel
			default:
				fmt.Printf("Invalid input '%s'. Use 'met', 'wait', or 'cancel'\n", command)
			}
		}
	}
}

func (ids *InteractiveDummyStrategy) checkBalance(ctx context.Context) error {
	balance, err := ids.trader.GetBalance(ctx)
	if err != nil {
		return err
	}

	log.Printf("Account Balance - Total: %.2f, Available: %.2f",
		balance.TotalBalance, balance.AvailableBalance)

	symbolBalance, err := ids.trader.GetSymbolBalance(ctx, ids.testSymbol)
	if err != nil {
		return err
	}

	log.Printf("%s Balance - Quantity: %.6f, USD Value: %.2f",
		ids.testSymbol, symbolBalance.TotalQuantity, symbolBalance.UsdValue)

	return nil
}
