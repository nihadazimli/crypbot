// Package numutils includes all numerical utility functions
package numutils

import (
	"fmt"
	"math"
	"slices"
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
		return 0
	}

	// Make a copy of the slice to avoid altering the original data
	sortedValues := make([]float64, n)
	copy(sortedValues, values)
	slices.Sort(sortedValues)

	// Finding the median
	midpoint := n / 2 // Get the middle index
	if n%2 != 0 {
		// Odd number of elements
		return sortedValues[midpoint] // Middle element
	}
	// Even number of elements, return the average of the two middle values
	return (sortedValues[midpoint-1] + sortedValues[midpoint]) / 2
}

func RSI(prices []float64, n int) float64 {
	if len(prices) < n+1 {
		return 0 // Not enough data to compute RSI
	}

	//Compute changes for gains and losses
	changes := make([]float64, len(prices)-1)
	for i := 1; i < len(prices); i++ {
		changes[i-1] = prices[i] - prices[i-1]
	}

	// We need gains and losses to calculate RSI, if gain or loss smaller than 0 we take it as 0
	gains := make([]float64, len(changes))
	losses := make([]float64, len(changes))
	for i, change := range changes {
		if change > 0 {
			gains[i] = change
			losses[i] = 0.0
		} else {
			losses[i] = -change
			gains[i] = 0.0
		}
	}

	//Calculate exponential moving average for gains and losses
	avgGain := ExponentialMovingAverage(gains, n)
	avgLoss := ExponentialMovingAverage(losses, n)

	// We only need the last RSI value
	if avgLoss[len(avgLoss)-1] == 0 {
		if avgGain[len(avgGain)-1] > 0 {
			return 100 // Avoid division by zero and define RSI as 100 when there are no losses but gains
		}
		return 0 // No gains and no losses
	}

	// Calculate RSI
	rs := avgGain[len(avgGain)-1] / avgLoss[len(avgLoss)-1]
	rsi := 100 - (100 / (1 + rs))

	return rsi
}

func StandardDeviation(values []float64, period int) float64 {
	if len(values) < period {
		return 0
	}

	data := values[len(values)-period:]
	mean := 0.0
	for _, v := range data {
		mean += v
	}
	mean /= float64(len(data))

	variance := 0.0
	for _, v := range data {
		variance += math.Pow(v-mean, 2)
	}
	variance /= float64(len(data) - 1)

	return math.Sqrt(variance)
}

func ZScore(prices []float64, period int) float64 {
	if len(prices) < period {
		return 0
	}

	sd := StandardDeviation(prices, period)
	if sd == 0 {
		return 0
	}

	latestPrice := prices[len(prices)-1]
	zScore := (latestPrice - MovingAverage(prices, period)) / sd
	return zScore
}
