package financial

import (
	"github.com/nihadazimli/crypbot/internal/model"
)

type EngineInterface interface {
	GetRSIClosed(symbol string) (float64, bool)
	GetRSILive(symbol string) (float64, bool)
	GetVolumeValueMedian(symbol string) (float64, bool)
	GetCurrentPrice(symbol string) (float64, bool)
	GetMovingAverages(symbol string) (model.MovingAverage, error)
	Show()
}
