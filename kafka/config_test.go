package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateSASLPlain(t *testing.T) {
	newCfg := func() *Config {
		return DefaultConfig([]string{"localhost:9092"}, "g").WithSASLPlain("user", "pass")
	}

	t.Run("rejected without TLS", func(t *testing.T) {
		err := newCfg().Validate()
		assert.ErrorContains(t, err, "SASL PLAIN requires TLS")
	})

	t.Run("allowed with TLS", func(t *testing.T) {
		assert.NoError(t, newCfg().WithTLS(nil).Validate())
	})

	t.Run("allowed with explicit opt-in", func(t *testing.T) {
		cfg := newCfg()
		cfg.AllowPlaintextSASL = true
		assert.NoError(t, cfg.Validate())
	})

	t.Run("SCRAM does not require TLS", func(t *testing.T) {
		cfg := DefaultConfig([]string{"localhost:9092"}, "g").WithSASLScram("SCRAM-SHA-256", "user", "pass")
		assert.NoError(t, cfg.Validate())
	})
}
