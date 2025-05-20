package strategy

import (
	"sync"

	"github.com/nihadazimli/crypbot/config"
	"github.com/nihadazimli/crypbot/internal/financial"
)

type RSIStrategy struct {
	name       Name
	financial  financial.EngineInterface
	buySignal  chan string
	sellSignal chan string
	config     *config.TraderConfig
	mu         sync.Mutex
}

func (s *RSIStrategy) Name() Name {
	return s.name
}

func (s *RSIStrategy) ProcessKline(symbol string) {

}

func (s *RSIStrategy) CycleCompleted() {

}
