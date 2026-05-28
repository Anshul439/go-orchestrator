package logger

import (
	"log/slog"
	"os"
)

func NewLogger() *slog.Logger {

	handler := slog.NewTextHandler(
		os.Stdout,
		nil,
	)

	return slog.New(handler)
}
