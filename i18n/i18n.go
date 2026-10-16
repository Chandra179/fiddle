package i18n

import (
	"context"
	"errors"
)

var (
	ErrLocaleNotFound  = errors.New("locale not found")
	ErrMessageNotFound = errors.New("message not found")
	ErrBundleLoad      = errors.New("failed to load message bundle")
)

// Config controls bundle loading and locale resolution.
type Config struct {
	// DefaultLocale is used when the requested locale has no message file. Required.
	DefaultLocale string
	// FallbackLocale is used when a key is missing in DefaultLocale. Optional.
	FallbackLocale string
	// MessageFiles are paths to JSON message files to load. At least one required.
	MessageFiles []string
	// Format is the message file format. Only "json" (the default) is supported.
	Format string
}

// Translator resolves localised messages.
type Translator interface {
	// T translates a message key for the given locale.
	// templateData may be nil. pluralCount < 0 means "not a plural message".
	T(ctx context.Context, locale, messageID string, templateData map[string]any, pluralCount int) (string, error)
	// Locales returns all loaded locales.
	Locales() []string
}
