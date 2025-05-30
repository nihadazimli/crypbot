package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	bybit "github.com/wuhewuhe/bybit.go.api"

	"github.com/nihadazimli/crypbot/config"

	"github.com/nihadazimli/crypbot/pkg/websocket"

	"github.com/nihadazimli/crypbot/internal/exchange"
	"github.com/nihadazimli/crypbot/internal/model"
)

type MarketClient struct {
	cfg            *config.Config
	klineWriteChan chan model.WSInternalKline
	BybitClient    *bybit.Client
}

func (m *MarketClient) StartKlineStream(ctx context.Context, params exchange.KlineStreamRequestParams) error {
	klineTopics := m.createKlineTopics(params.Symbols, params.Interval)
	symbolChunks := chunkSymbols(klineTopics, m.cfg.Exchange.WebSocket.MaxSymbolsPerConnection)
	var webSockets []*websocket.Client

	for _, chunk := range symbolChunks {
		ws := websocket.NewWebSocketClientPublic(
			m.cfg.Exchange.WebSocket.AddressPublic,
			m.cfg.Exchange.WebSocket.PingIntervalDuration,
			m.handleKlineMessages,
		)
		m.klineWriteChan = params.KlineWriteChan

		err := ws.ConnectAndStart(ctx, chunk)
		if err != nil {
			for _, ws := range webSockets {
				ws.Shutdown()
			}
			return err
		}
		webSockets = append(webSockets, ws)
	}
	return nil
}

func (m *MarketClient) GetKlineHistory(
	ctx context.Context,
	params exchange.KlineHistoryParams,
) (exchange.KlineHistoryResult, error) {
	startTime, endTime := determineTimeInterval(params.Limit, params.Interval)

	requestParams := map[string]string{
		"symbol":   params.Symbol,
		"category": m.cfg.MarketConstants.KlineCategory,
		"interval": strconv.Itoa(params.Interval),
		"start":    strconv.FormatInt(startTime, 10),
		"end":      strconv.FormatInt(endTime, 10),
		"limit":    strconv.Itoa(params.Limit + 1),
	}

	requestUrl := buildUrl(m.cfg.Exchange.API.Host, m.cfg.Exchange.API.RequestPaths.Kline, requestParams)
	req, err := http.NewRequestWithContext(ctx, "GET", requestUrl, nil)
	if err != nil {
		return exchange.KlineHistoryResult{}, fmt.Errorf("error creating request for kline data: %w", err)
	}

	response, err := m.BybitClient.HTTPClient.Do(req)
	if err != nil {
		return exchange.KlineHistoryResult{}, fmt.Errorf("error fetching kline data: %w", err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return exchange.KlineHistoryResult{}, fmt.Errorf("error reading response body: %w", err)
	}

	var apiResponse model.KlineApiResponse
	if err := json.Unmarshal(body, &apiResponse); err != nil {
		return exchange.KlineHistoryResult{}, fmt.Errorf("error unmarshaling JSON. Respomse: %v Err: %w",
			apiResponse, err)
	}

	if len(apiResponse.Result.List) < 1 {
		return exchange.KlineHistoryResult{}, fmt.Errorf("no kline data available for symbol: %s", params.Symbol)
	}

	// Extract the 5th value (close price) from each list
	closePrices := make([]float64, 0, len(apiResponse.Result.List))
	volumeValues := make([]float64, 0, len(apiResponse.Result.List))
	for _, item := range apiResponse.Result.List {
		closePrice, _ := strconv.ParseFloat(strings.TrimSpace(item[4]), 64)
		volumeAmount, _ := strconv.ParseFloat(strings.TrimSpace(item[5]), 64)

		closePrices = append(closePrices, closePrice)
		volumeValues = append(volumeValues, volumeAmount*closePrice)
	}
	reverse(closePrices)
	reverse(volumeValues)

	// Eliminate live value, just return most recent closed values
	return exchange.KlineHistoryResult{
		ClosePrices: closePrices[:len(closePrices)-1],
		Volumes:     volumeValues[:len(volumeValues)-1],
	}, nil
}

func (m *MarketClient) Shutdown() {
	//TODO implement me
	panic("implement me")
}

func (m *MarketClient) createKlineTopics(symbols []string, interval int) []string {
	var klineTopics []string
	for _, symbol := range symbols {
		klineTopics = append(klineTopics, constructKlineTopic(symbol, interval))
	}
	return klineTopics
}

func (m *MarketClient) handleKlineMessages(message []byte) error {
	var WSKline model.WSRawKline
	err := json.Unmarshal(message, &WSKline)
	if err != nil {
		log.Println("Failed to unmarshal JSON:", err)
		return err
	}

	for _, kline := range WSKline.Data {
		internalKline := model.WSInternalKline{
			Symbol:   getSymbolFromKlineTopic(WSKline.Topic),
			Close:    kline.Close,
			IsClosed: kline.Confirm,
			Volume:   kline.Volume,
		}
		m.klineWriteChan <- internalKline

	}
	return nil
}

func buildUrl(host string, path string, params map[string]string) string {
	baseUrl, err := url.Parse(host)

	if err != nil {
		log.Fatal(err)
	}
	baseUrl.Path, _ = url.JoinPath(baseUrl.Path, path)

	queryParams := url.Values{}
	for k, v := range params {
		queryParams.Add(k, v)
	}
	baseUrl.RawQuery = queryParams.Encode()

	return baseUrl.String()
}

// reverse given float64 numbers slice this function reverses every value in place
func reverse(numbers []float64) {
	for i, j := 0, len(numbers)-1; i < j; i, j = i+1, j-1 {
		numbers[i], numbers[j] = numbers[j], numbers[i]
	}
}

// determineTimeInterval determines for a given interval how much time needed to get n number of latest Klines
// it returns startTime and endTime in milliseconds since the epoch.
func determineTimeInterval(n int, interval int) (int64, int64) {
	now := time.Now()
	pastDuration := time.Duration(n*interval) * time.Minute
	pastTime := now.Add(-pastDuration)

	return pastTime.UnixMilli(), now.UnixMilli()
}

func chunkSymbols(symbols []string, chunkSize int) [][]string {
	var allChunks [][]string

	for i := 0; i < len(symbols); i += chunkSize {
		end := i + chunkSize
		if end > len(symbols) {
			end = len(symbols)
		}

		allChunks = append(allChunks, symbols[i:end])
	}

	return allChunks
}

func constructKlineTopic(symbol string, klineInterval int) string {
	return "kline." + strconv.Itoa(klineInterval) + "." + symbol
}

func getSymbolFromKlineTopic(topic string) string {
	return topic[strings.LastIndex(topic, ".")+1:]
}
