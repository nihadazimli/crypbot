package websocket

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nihadazimli/crypbot/internal/config"
	"github.com/nihadazimli/crypbot/pkg/handler"
)

type WebSocketClient struct {
	conn *websocket.Conn
}

func Serve() {
	cfg := config.GetConfig()

	conn, _, err := websocket.DefaultDialer.Dial(cfg.BybitCreds.WSAddress, nil)

	if err != nil {
		log.Fatal("Failed to dial:", err)
	}

	c := &WebSocketClient{conn: conn}

	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer c.conn.Close()

	fmt.Println("Connected.")

	c.subscribe(cfg.Topics)
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
		go handler.Receive(message)
	}
}

func (c *WebSocketClient) keepAlive() {
	for {
		err := c.conn.WriteMessage(websocket.PingMessage, nil)
		if err != nil {
			log.Println("Ping failed:", err)
			return
		}
		time.Sleep(time.Second * 10)
	}
}
