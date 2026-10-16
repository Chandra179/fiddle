package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
)

var _ Logger = (*AppLogger)(nil)

type AppLogger struct {
	logger *slog.Logger
}

// NewLogger returns a JSON logger writing to stdout. Debug level is enabled
// unless env is "production".
func NewLogger(env string) *AppLogger {
	return newLogger(os.Stdout, env)
}

func newLogger(w io.Writer, env string) *AppLogger {
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	if env == "production" {
		opts.Level = slog.LevelInfo
	}
	return &AppLogger{logger: slog.New(slog.NewJSONHandler(w, opts))}
}

func (l *AppLogger) Info(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, slog.LevelInfo, msg, fields...)
}

func (l *AppLogger) Debug(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, slog.LevelDebug, msg, fields...)
}

func (l *AppLogger) Warn(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, slog.LevelWarn, msg, fields...)
}

func (l *AppLogger) Error(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, slog.LevelError, msg, fields...)
}

func (l *AppLogger) log(ctx context.Context, level slog.Level, msg string, fields ...Field) {
	// Skip building attributes for disabled levels (e.g. Debug in production).
	if !l.logger.Enabled(ctx, level) {
		return
	}
	attrs := make([]slog.Attr, len(fields))
	for i, f := range fields {
		attrs[i] = slog.Any(f.Key, f.Value)
	}
	l.logger.LogAttrs(ctx, level, msg, attrs...)
}

// Logger returns the underlying slog.Logger for use with libraries that require it.
func (l *AppLogger) Logger() *slog.Logger {
	return l.logger
}
