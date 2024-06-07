package handler

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

func Receive(message string) {
	var data map[string]interface{}
	err := json.Unmarshal([]byte(message), &data)
	if err != nil {
		log.Println("Failed to unmarshal JSON:", err)
		return
	}

	// Convert time fields to human-readable format
	if ts, ok := data["ts"].(float64); ok {
		data["ts"] = time.Unix(0, int64(ts)*int64(time.Millisecond)).Format(time.RFC3339)
	}

	if klineData, ok := data["data"].([]interface{}); ok {
		for _, item := range klineData {
			if itemMap, ok := item.(map[string]interface{}); ok {
				if start, ok := itemMap["start"].(float64); ok {
					itemMap["start"] = time.Unix(0, int64(start)*int64(time.Millisecond)).Format(time.RFC3339)
				}
				if end, ok := itemMap["end"].(float64); ok {
					itemMap["end"] = time.Unix(0, int64(end)*int64(time.Millisecond)).Format(time.RFC3339)
				}
				if timestamp, ok := itemMap["timestamp"].(float64); ok {
					itemMap["timestamp"] = time.Unix(0, int64(timestamp)*int64(time.Millisecond)).Format(time.RFC3339)
				}
			}
		}
	}

	fmt.Println(data)
}
