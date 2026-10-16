package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

func TestAppLogger_WritesFields(t *testing.T) {
	var buf bytes.Buffer
	l := newLogger(&buf, "development")

	l.Info(context.Background(), "hello", Field{Key: "user_id", Value: 42})

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("invalid JSON %q: %v", buf.String(), err)
	}
	if entry["msg"] != "hello" || entry["level"] != "INFO" || entry["user_id"] != float64(42) {
		t.Fatalf("unexpected entry: %v", entry)
	}
}

func TestAppLogger_ProductionDropsDebug(t *testing.T) {
	var buf bytes.Buffer
	l := newLogger(&buf, "production")

	l.Debug(context.Background(), "noisy", Field{Key: "k", Value: "v"})
	if buf.Len() != 0 {
		t.Fatalf("debug must be dropped in production, got %q", buf.String())
	}

	l.Warn(context.Background(), "kept")
	if buf.Len() == 0 {
		t.Fatal("warn must be logged in production")
	}
}

func TestAppLogger_LoggerReturnsSameInstance(t *testing.T) {
	l := NewLogger("development")
	if l.Logger() != l.Logger() {
		t.Fatal("Logger() must return a stable *slog.Logger")
	}
}

func BenchmarkAppLogger_DisabledDebug(b *testing.B) {
	var buf bytes.Buffer
	l := newLogger(&buf, "production")
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		l.Debug(ctx, "noisy", Field{Key: "k", Value: "v"})
	}
}
