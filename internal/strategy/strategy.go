package strategy

import "github.com/nihadazimli/crypbot/internal/model"

type Name string

type Strategy interface {
	Name() Name
	ProcessKline(kline model.WSInternalKline)
	CycleCompleted()
}
