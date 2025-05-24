// Package strategy provides signal routing functionality
package strategy

import (
	"context"
	"log"
	"sync"

	"github.com/nihadazimli/crypbot/internal/model"
)

// StrategyChannel holds a strategy's information and its channel
type StrategyChannel struct {
	Name    string
	Channel chan<- model.MarketEvent
}

// Router handles fan-out distribution of market events to strategies
type Router struct {
	// Input channel from Financial Engine
	inputChannel <-chan model.MarketEvent

	// Registered strategy channels
	strategies    map[string]StrategyChannel
	strategiesMux sync.RWMutex

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewRouter creates a new signal router
func NewRouter(ctx context.Context, inputChannel <-chan model.MarketEvent) *Router {
	ctx, cancel := context.WithCancel(ctx)

	return &Router{
		inputChannel: inputChannel,
		strategies:   make(map[string]StrategyChannel),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// RegisterStrategy registers a strategy to receive market events
func (r *Router) RegisterStrategy(name string, channel chan<- model.MarketEvent) {
	r.strategiesMux.Lock()
	defer r.strategiesMux.Unlock()

	r.strategies[name] = StrategyChannel{
		Name:    name,
		Channel: channel,
	}

	log.Printf("Signal Router: Registered strategy '%s'", name)
}

// UnregisterStrategy removes a strategy from receiving market events
func (r *Router) UnregisterStrategy(name string) {
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

	for name, strategyChannel := range r.strategies {
		select {
		case strategyChannel.Channel <- event:
		default:
			log.Printf("Signal Router: Warning - Strategy '%s' channel is full, dropping market event for symbol %s",
				name, event.Symbol)
		}
	}
}

// GetRegisteredStrategies returns the names of all registered strategies
func (r *Router) GetRegisteredStrategies() []string {
	r.strategiesMux.RLock()
	defer r.strategiesMux.RUnlock()

	names := make([]string, 0, len(r.strategies))
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
