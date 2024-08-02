package numutils

import (
	"fmt"
	"math"
	"strings"
)

// RoundDownToSamePrecision rounds the value down to the same decimal precision as the target.
func RoundDownToSamePrecision(value float64, target float64) float64 {
	precision := decimalPlaces(target)
	factor := math.Pow(10, float64(precision))
	return math.Floor(value*factor) / factor
}

// decimalPlaces counts the number of decimal places in a float.
func decimalPlaces(f float64) int {
	// Convert float to string to count decimal places
	s := fmt.Sprintf("%f", f)
	i := strings.LastIndexByte(s, '.')
	if i > -1 {
		return len(s) - i - 1
	}
	return 0 // no decimal point, so no decimal places
}
