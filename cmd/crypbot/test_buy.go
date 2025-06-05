package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nihadazimli/crypbot/internal/exchange/bybit"
	"github.com/nihadazimli/crypbot/internal/model"
	"github.com/nihadazimli/crypbot/internal/order"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("Shutting down gracefully...")
		cancel()
		time.Sleep(10 * time.Second)
		log.Println("Forcing exit after timeout")
		os.Exit(1)
	}()
	bybitAdapter, _ := bybit.NewTradingClientForStrategy(model.StrategyRSI)
	orderManager := order.NewOrderManager(bybitAdapter)

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Println("Select action: [create/get/update/cancel]")
		scanner.Scan()
		action := strings.ToLower(scanner.Text())
		switch action {
		case "create":
			fmt.Print("Symbol: ")
			scanner.Scan()
			symbol := scanner.Text()

			fmt.Print("Side (buy/sell): ")
			scanner.Scan()
			sideStr := scanner.Text()
			side := model.ParseOrderSide(strings.Title(sideStr))

			fmt.Print("Type (limit/market): ")
			scanner.Scan()
			typeStr := scanner.Text()
			orderType := model.ParseOrderType(strings.Title(typeStr))

			fmt.Print("Quantity: ")
			scanner.Scan()
			quantity, _ := strconv.ParseFloat(scanner.Text(), 64)

			var price *float64
			if orderType == model.OrderTypeLimit {
				fmt.Print("Price: ")
				scanner.Scan()
				priceVal, _ := strconv.ParseFloat(scanner.Text(), 64)
				price = &priceVal
			}

			params := order.CreateParams{
				Symbol:   symbol,
				Side:     side,
				Type:     orderType,
				Price:    price,
				Quantity: quantity,
			}

			createdOrder, err := orderManager.Create(ctx, params)
			if err != nil {
				log.Printf("Create failed: %v\n", err)
			}
			fmt.Printf("Order created successfully: %+v\n", createdOrder)

		case "get":
			fmt.Print("Order ID: ")
			scanner.Scan()
			orderID := scanner.Text()

			currOrder, err := orderManager.GetById(ctx, orderID)
			if err != nil {
				log.Printf("Get failed: %v\n", err)
			}
			fmt.Printf("Order details: %+v\n", currOrder)

		case "update":
			fmt.Print("Order ID: ")
			scanner.Scan()
			orderID := scanner.Text()

			fmt.Print("Symbol: ")
			scanner.Scan()
			symbol := scanner.Text()

			updateParams := order.UpdateParams{
				OrderID: orderID,
				Symbol:  symbol,
			}

			fmt.Print("New Price (leave empty to skip): ")
			scanner.Scan()
			priceText := scanner.Text()
			var price float64
			if priceText != "" {
				priceVal, _ := strconv.ParseFloat(priceText, 64)
				price = priceVal
				updateParams.Price = &price
			}

			fmt.Print("New Quantity (leave empty to skip): ")
			scanner.Scan()
			qtyText := scanner.Text()
			var qty float64
			if qtyText != "" {
				qtyVal, _ := strconv.ParseFloat(qtyText, 64)
				qty = qtyVal
				updateParams.Quantity = &qty
			}
			updatedOrder, err := orderManager.Update(ctx, updateParams)
			if err != nil {
				log.Printf("Update failed: %v", err)
			}
			fmt.Printf("Order updated successfully: %+v\n", updatedOrder)

		case "cancel":
			fmt.Print("Order ID: ")
			scanner.Scan()
			orderID := scanner.Text()

			err := orderManager.Cancel(ctx, orderID)
			if err != nil {
				log.Printf("Cancel failed: %v", err)
			}
			fmt.Println("Order canceled successfully.")
		case "exit":
			return
		default:
			fmt.Println("Invalid action selected.")
		}
	}
}
