package server

import (
	"testing"
	"time"
)

func TestRetryDelay(t *testing.T) {
	cases := []struct {
		retryCount int
		want       time.Duration
	}{
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
	}
	for _, c := range cases {
		got := retryDelay(c.retryCount)
		if got != c.want {
			t.Errorf("retryDelay(%d) = %v, want %v", c.retryCount, got, c.want)
		}
	}
}
