package trader

import "errors"

var (
	BuyDetailsErrCannotCalculate  = errors.New("buy order details cannot be calculated")
	SellDetailsErrCannotCalculate = errors.New("sell order details cannot be calculated")
)
