package i18n

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalizerCache_BoundedBySupportedLocales(t *testing.T) {
	tr, err := NewTranslator(Config{
		DefaultLocale:  "en",
		FallbackLocale: "en",
		MessageFiles:   []string{"example/testdata/en.json", "example/testdata/es.json"},
	})
	require.NoError(t, err)

	// Simulate attacker-controlled Accept-Language values: every distinct but
	// valid tag must collapse onto one of the loaded languages.
	ctx := context.Background()
	for i := range 10_000 {
		locale := fmt.Sprintf("en-x-r%d", i)
		_, err := tr.T(ctx, locale, "greeting", nil, -1)
		require.NoError(t, err)
	}
	for _, locale := range []string{"es-MX", "es-419", "fr", "de-CH"} {
		_, err := tr.T(ctx, locale, "greeting", nil, -1)
		require.NoError(t, err)
	}

	impl := tr.(*translator)
	impl.localizersMu.RLock()
	defer impl.localizersMu.RUnlock()
	assert.LessOrEqual(t, len(impl.localizers), 2)
}

func TestTranslate_RegionalVariantMatchesBase(t *testing.T) {
	tr, err := NewTranslator(Config{
		DefaultLocale: "en",
		MessageFiles:  []string{"example/testdata/en.json", "example/testdata/es.json"},
	})
	require.NoError(t, err)

	es, err := tr.T(context.Background(), "es", "greeting", nil, -1)
	require.NoError(t, err)
	esMX, err := tr.T(context.Background(), "es-MX", "greeting", nil, -1)
	require.NoError(t, err)
	assert.Equal(t, es, esMX)
}
