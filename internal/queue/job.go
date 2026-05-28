package queue

type Job struct {
	ID         int `json:"id"`
	RetryCount int `json:"retry_count"`
	MaxRetries int `json:"max_retries"`
}
