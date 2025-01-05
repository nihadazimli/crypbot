// Package numutils includes all numerical utility functions
package numutils

import (
	"fmt"
	"math"
	"sort"
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

// CalculatePrecision returns the number of decimal places in the value
func CalculatePrecision(value string) (int, error) {
	parts := strings.Split(value, ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("value %s is not a valid decimal", value)
	}
	return len(parts[1]), nil
}

// ExponentialMovingAverage returns the full series of EMA so that the calling function can select the last one.
func ExponentialMovingAverage(values []float64, n int) []float64 {
	alpha := 1.0 / float64(n)
	emaValues := make([]float64, len(values))
	emaValues[0] = values[0] // Initialize the first EMA value to the first price

	for i := 1; i < len(values); i++ {
		emaValues[i] = values[i]*alpha + emaValues[i-1]*(1-alpha)
	}

	return emaValues
}

// MovingAverage calculates the Moving Average for a slice of float64.
func MovingAverage(values []float64, period int) float64 {
	if len(values) < period {
		return 0
	}

	sum := 0.0
	for i := len(values) - period; i < len(values); i++ {
		sum += values[i]
	}

	return sum / float64(period)
}

// FindMedian calculates the median of a slice of float64 numbers.
func FindMedian(values []float64) float64 {
	n := len(values)
	if n == 0 {
		return 0 // or return an error indicating no values are present
	}

	// Make a copy of the slice to avoid altering the original data
	sortedValues := make([]float64, n)
	copy(sortedValues, values)
	sort.Float64s(sortedValues)

	// Finding the median
	midpoint := n / 2 // Get the middle index
	if n%2 != 0 {
		// Odd number of elements
		return sortedValues[midpoint] // Middle element
	}
	// Even number of elements, return the average of the two middle values
	return (sortedValues[midpoint-1] + sortedValues[midpoint]) / 2
}
