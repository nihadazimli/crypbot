package websocket

import (
	"encoding/json"
	"fmt"
	"github.com/nihadazimli/crypbot/pkg/model"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nihadazimli/crypbot/internal/config"
)

type WebSocketClient struct {
	conn           *websocket.Conn
	klineWriteChan chan model.InternalKline
}

func Serve(cfg *config.Config, klineReceived chan model.InternalKline) {
	conn, _, err := websocket.DefaultDialer.Dial(cfg.Broker.WebSocket.Address, nil)

	if err != nil {
		log.Fatal("Failed to dial:", err)
	}

	c := &WebSocketClient{conn: conn, klineWriteChan: klineReceived}

	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer c.conn.Close()

	fmt.Println("Connected.")

	klineTopics := generateKlineTopicsFromSymbols(cfg.Symbols, strconv.Itoa(cfg.MarketConstants.KlineInterval))

	go c.keepAlive()
	c.subscribe(klineTopics)
	c.readPump()

}

func (c *WebSocketClient) subscribe(topic []string) {
	subscribeRequest := map[string]interface{}{
		"op":   "subscribe",
		"args": topic,
	}
	message, err := json.Marshal(subscribeRequest)
	if err != nil {
		log.Println("Failed to marshal JSON:", err)
		return
	}
	err = c.conn.WriteMessage(websocket.TextMessage, message)
	if err != nil {
		log.Println("Failed to send subscribe message:", err)
		return
	}
}

func (c *WebSocketClient) readPump() {

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			log.Println("Failed to read message:", err)
			return
		}
		go c.handle(message)
	}
}

func (c *WebSocketClient) keepAlive() {
	for {
		err := c.conn.WriteMessage(websocket.PingMessage, nil)
		if err != nil {
			log.Println("Ping failed:", err)
			return
		}
		time.Sleep(time.Second * 20)
	}
}

func (c *WebSocketClient) handle(message []byte) {
	var WSKline model.WSRawKline
	err := json.Unmarshal(message, &WSKline)
	if err != nil {
		log.Println("Failed to unmarshal JSON:", err)
		return
	}
	for _, kline := range WSKline.Data {
		internalKline := model.InternalKline{
			Symbol:  getSymbolFromKlineTopic(WSKline.Topic),
			Close:   kline.Close,
			Confirm: kline.Confirm,
		}
		c.klineWriteChan <- internalKline
	}
}
func generateKlineTopicsFromSymbols(symbols []string, klineInterval string) []string {
	var topics []string

	for _, symbol := range symbols {
		topic := "kline." + klineInterval + "." + symbol
		topics = append(topics, topic)
	}
	return topics
}

func getSymbolFromKlineTopic(topic string) string {
	return topic[strings.LastIndex(topic, ".")+1:]
}
