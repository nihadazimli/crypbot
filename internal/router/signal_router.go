// Package router provides signal routing functionality
package router

import (
	"context"
	"log"
	"sync"

	"github.com/nihadazimli/crypbot/internal/model"
)

// Router handles fan-out distribution of market events to strategies
type Router struct {
	// Input channel from Financial Engine
	inputChannel <-chan model.MarketEvent

	// Registered strategy channels
	strategies    map[model.StrategyID]chan<- model.MarketEvent
	strategiesMux sync.RWMutex

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewRouter creates a new signal router
func NewRouter(parentCtx context.Context, inputChannel <-chan model.MarketEvent) *Router {
	ctx, cancel := context.WithCancel(parentCtx)

	return &Router{
		inputChannel: inputChannel,
		strategies:   make(map[model.StrategyID]chan<- model.MarketEvent),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// RegisterStrategy registers a strategy to receive market events
func (r *Router) RegisterStrategy(name model.StrategyID, channel chan<- model.MarketEvent) {
	r.strategiesMux.Lock()
	defer r.strategiesMux.Unlock()

	r.strategies[name] = channel

	log.Printf("Signal Router: Registered strategy '%s'", name)
}

// UnregisterStrategy removes a strategy from receiving market events
func (r *Router) UnregisterStrategy(name model.StrategyID) {
	r.strategiesMux.Lock()
	defer r.strategiesMux.Unlock()

	delete(r.strategies, name)
	log.Printf("Signal Router: Unregistered strategy '%s'", name)
}

// Start begins routing market events to registered strategies
func (r *Router) Start(ctx context.Context) {
	r.wg.Add(1)
	go r.routeEvents(ctx)
	log.Println("Signal Router: Started routing market events")
}

// Stop gracefully stops the router
func (r *Router) Stop() {
	r.cancel()
	r.wg.Wait()
	log.Println("Signal Router: Stopped")
}

// routeEvents is the main routing loop
func (r *Router) routeEvents(ctx context.Context) {
	defer r.wg.Done()

	for {
		select {
		case event, ok := <-r.inputChannel:
			if !ok {
				log.Println("Signal Router: Input channel closed, stopping")
				return
			}

			// Broadcast to all registered strategies
			r.broadcastEvent(event)

		case <-ctx.Done():
			log.Println("Signal Router: Context cancelled, stopping")
			return
		case <-r.ctx.Done():
			log.Println("Signal Router: Router context cancelled, stopping")
			return
		}
	}
}

// broadcastEvent sends the market event to all registered strategies
func (r *Router) broadcastEvent(event model.MarketEvent) {
	r.strategiesMux.RLock()
	defer r.strategiesMux.RUnlock()

	if len(r.strategies) == 0 {
		return
	}

	for strategyID, strategyChannel := range r.strategies {
		if r.shouldRouteToStrategy(strategyID, event) {
			select {
			case strategyChannel <- event:
			default:
				log.Printf("Signal Router: Warning - Strategy '%s' channel is full, dropping market event for symbol %s",
					strategyID, event.Symbol)
			}
		}
	}
}

// GetRegisteredStrategies returns the names of all registered strategies
func (r *Router) GetRegisteredStrategies() []model.StrategyID {
	r.strategiesMux.RLock()
	defer r.strategiesMux.RUnlock()

	names := make([]model.StrategyID, 0, len(r.strategies))
	for name := range r.strategies {
		names = append(names, name)
	}
	return names
}

// GetStrategyCount returns the number of registered strategies
func (r *Router) GetStrategyCount() int {
	r.strategiesMux.RLock()
	defer r.strategiesMux.RUnlock()
	return len(r.strategies)
}

func (r *Router) shouldRouteToStrategy(strategyID model.StrategyID, event model.MarketEvent) bool {
	switch strategyID {
	case model.StrategyRSI:
		return event.IsClosed
	case model.StrategyBollinger:
		return true
	default:
		return event.IsClosed
	}
}
