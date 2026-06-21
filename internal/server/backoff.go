package server

import (
	"math"
	"time"
)

func retryDelay(retryCount int) time.Duration {
	return time.Duration(math.Pow(2, float64(retryCount))) * time.Second
}
