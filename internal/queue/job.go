package queue

type Job struct {
	ID         int    `json:"id"`
	RetryCount int    `json:"retry_count"`
	MaxRetries int    `json:"max_retries"`
	Type       string `json:"type"`
	Payload    string `json:"payload"`
}
