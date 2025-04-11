FROM golang:1.23-alpine

ENV ENVIRONMENT=production

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o crypbot cmd/crypbot/crypbot.go
RUN go build -o sell_bot cmd/crypbot/crypbot_trader_order.go

CMD ["./crypbot"]