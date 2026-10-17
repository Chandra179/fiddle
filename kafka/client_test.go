package kafka

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestSlogShim_LevelFollowsLogger(t *testing.T) {
	var lv slog.LevelVar
	shim := &SlogShim{L: slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: &lv}))}

	cases := []struct {
		level slog.Level
		want  kgo.LogLevel
	}{
		{slog.LevelDebug, kgo.LogLevelDebug},
		{slog.LevelInfo, kgo.LogLevelInfo},
		{slog.LevelWarn, kgo.LogLevelWarn},
		{slog.LevelError, kgo.LogLevelError},
		{slog.LevelError + 4, kgo.LogLevelNone},
	}
	for _, tc := range cases {
		lv.Set(tc.level) // changes at runtime must be picked up
		assert.Equal(t, tc.want, shim.Level(), "slog level %v", tc.level)
	}
}
