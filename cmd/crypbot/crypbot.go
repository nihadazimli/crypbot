package main

import (
	"github.com/nihadazimli/crypbot/internal/config"
	"github.com/nihadazimli/crypbot/pkg/websocket"
)

func main() {
	cfg := config.GetConfig()
	websocket.Serve(cfg)
}
