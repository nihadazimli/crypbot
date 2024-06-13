package handler

//
//import (
//	"encoding/json"
//	"github.com/nihadazimli/crypbot/pkg/financial"
//	"github.com/nihadazimli/crypbot/pkg/model"
//	"log"
//)
//
//func Receive(message []byte) {
//	var WSKline model.WSRawKline
//	err := json.Unmarshal(message, &WSKline)
//	if err != nil {
//		log.Println("Failed to unmarshal JSON:", err)
//		return
//	}
//	for _, kline := range WSKline.Data {
//		internalKline := model.InternalKline{
//			Topic:   WSKline.Topic,
//			Close:   kline.Close,
//			Confirm: kline.Confirm,
//		}
//		go financial.ProcesssKline(internalKline)
//	}
//}
