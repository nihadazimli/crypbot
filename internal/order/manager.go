package order

import (
	"context"

	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/exchange"
)

type Manager struct {
	cfg            *config.Config
	exchange       exchange.Exchange
	hasActiveOrder bool
}

func NewOrderManager(exchange exchange.Exchange) *Manager {
	cfg := config.GetConfig()

	return &Manager{
		cfg:            cfg,
		exchange:       exchange,
		hasActiveOrder: false,
	}
}

func (m Manager) Create(ctx context.Context, params CreateParams) (*Order, error) {

}

func (m Manager) Update(ctx context.Context, params UpdateParams) (*Order, error) {
	//TODO implement me
	panic("implement me")
}

func (m Manager) Cancel(ctx context.Context, orderID string) error {
	//TODO implement me
	panic("implement me")
}

func (m Manager) Get(ctx context.Context, orderID string) (*Order, error) {
	//TODO implement me
	panic("implement me")
}
