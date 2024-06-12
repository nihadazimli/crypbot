package handler

import (
	"encoding/json"
	"fmt"
	"github.com/nihadazimli/crypbot/pkg/model"
	"log"
)

func Receive(message []byte) {
	var data model.WSRawKline
	err := json.Unmarshal(message, &data)
	if err != nil {
		log.Println("Failed to unmarshal JSON:", err)
		return
	}

	if len(data.Data) > 0 {
		fmt.Println(data.Topic, data.Data[0].Close)
	} else {
		fmt.Println(data.Topic)
	}
}
