package numutils

import (
	"math"
	"strconv"
	"strings"
)

// RoundDownToPrecision rounds the value down to the same decimal precision as the target.
func RoundDownToPrecision(value float64, target float64) float64 {
	precision := DecimalPlaces(target)
	factor := math.Pow(10, float64(precision))
	return math.Floor(value*factor) / factor
}

// RoundUpToPrecision rounds the value up to the same decimal precision as the target.
func RoundUpToPrecision(value float64, target float64) float64 {
	precision := DecimalPlaces(target)
	factor := math.Pow(10, float64(precision))
	return math.Ceil(value*factor) / factor
}

// DecimalPlaces counts the number of decimal places in a float.
func DecimalPlaces(f float64) int {
	s := strconv.FormatFloat(f, 'f', -1, 64)
	i := strings.IndexByte(s, '.')
	if i > -1 {
		return len(s) - i - 1
	}
	return 0
}
