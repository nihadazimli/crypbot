package websocket

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/nihadazimli/crypbot/pkg/model"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

type WebSocketClient struct {
	conn           *websocket.Conn
	url            string
	apiKey         string
	apiSecret      string
	handler        MessageHandler
	klineWriteChan chan model.WSInternalKline
	isConnected    bool
	pingInterval   int
	isPrivate      bool
	ctx            context.Context
	cancel         context.CancelFunc
}

type MessageHandler func(message []byte) error

func NewWebSocketClientPublic(ctx context.Context, url string, pingInterval int, handler MessageHandler) *WebSocketClient {

	c := &WebSocketClient{
		url:          url,
		handler:      handler,
		pingInterval: pingInterval,
		isPrivate:    false,
	}
	c.ctx, c.cancel = context.WithCancel(ctx)

	return c
}

func NewWebSocketClientPrivate(ctx context.Context, url string, apiKey string, apiSecret string, pingInterval int, handler MessageHandler) *WebSocketClient {

	c := &WebSocketClient{
		url:          url,
		handler:      handler,
		pingInterval: pingInterval,
		isPrivate:    true,
		apiKey:       apiKey,
		apiSecret:    apiSecret,
	}
	c.ctx, c.cancel = context.WithCancel(ctx)

	return c
}

func (c *WebSocketClient) ConnectAndStart(topics []string) error {
	var err error
	c.conn, _, err = websocket.DefaultDialer.Dial(c.url, nil)

	if err != nil {
		log.Printf("Failed to dial:%v", err)
		c.isConnected = false
		return err
	}

	if c.isPrivate {
		if err := c.setupPrivateConnection(); err != nil {
			log.Printf("Authentication could not be completed. Err:%v", err)
			return err
		}
	}

	c.isConnected = true

	c.subscribe(topics)
	go c.keepAlive(c.pingInterval)
	go c.readPump()
	go c.monitorConnection(topics)

	return nil

}

func (c *WebSocketClient) monitorConnection(topics []string) {
	ticker := time.NewTicker(time.Second * time.Duration(c.pingInterval))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !c.isConnected {
				log.Printf("Attempting to reconnect for %s...", c.url)
				err := c.ConnectAndStart(topics)
				if err != nil {
					log.Printf("Cannot reconnect to web socket...")
				}
				return

			}
		case <-c.ctx.Done(): // respect cancellation
			log.Println("Monitor connection is stopping due to cancellation.")
			return
		}
	}
}

func (c *WebSocketClient) Shutdown() {
	c.cancel()
	err := c.conn.Close()
	if err != nil {
		return
	}
	log.Println("WebSocket client has been shut down.")
}

func (c *WebSocketClient) subscribe(topic []string) {
	subscribeRequest := map[string]interface{}{
		"op":   "subscribe",
		"args": topic,
	}
	err := c.sendMessage(subscribeRequest)
	if err != nil {
		log.Printf("Cannot subsribe. Err: %v", err)
	}
}

func (c *WebSocketClient) readPump() {
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			log.Println("Failed to read message:", err)
			return
		}
		err = c.handler(message)
		if err != nil {
			log.Println("Error handling message:", err)
			return
		}
	}
}

func (c *WebSocketClient) keepAlive(pingInterval int) {
	for {
		err := c.conn.WriteMessage(websocket.PingMessage, nil)
		if err != nil {
			log.Println("Ping failed:", err)
			return
		}
		time.Sleep(time.Second * time.Duration(pingInterval))
	}
}

func (c *WebSocketClient) setupPrivateConnection() error {
	// Get current Unix time in milliseconds
	expires := time.Now().UnixNano()/1e6 + 10000
	val := fmt.Sprintf("GET/realtime%d", expires)

	h := hmac.New(sha256.New, []byte(c.apiSecret))
	h.Write([]byte(val))

	// Convert to hexadecimal instead of base64
	signature := hex.EncodeToString(h.Sum(nil))
	fmt.Println("signature generated : " + signature)

	authMessage := map[string]interface{}{
		"req_id": uuid.New(),
		"op":     "auth",
		"args":   []interface{}{c.apiKey, expires, signature},
	}

	err := c.sendMessage(authMessage)
	if err != nil {
		return err
	}
	return nil
}

func (c *WebSocketClient) sendMessage(message interface{}) error {
	encodedMessage, err := json.Marshal(message)
	if err != nil {
		return err
	}
	err = c.conn.WriteMessage(websocket.TextMessage, encodedMessage)
	if err != nil {
		return err
	}
	return nil
}
