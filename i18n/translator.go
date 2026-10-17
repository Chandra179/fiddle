package i18n

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"

	gotext "github.com/nicksnyder/go-i18n/v2/i18n"
	"golang.org/x/text/language"
)

type translator struct {
	bundle         *gotext.Bundle
	locales        []string
	fallbackLocale string

	// matcher maps any requested locale onto one of the loaded languages, so
	// the localizer cache is keyed by a bounded set of supported tags rather
	// than by caller-supplied strings (e.g. raw Accept-Language values).
	matcher   language.Matcher
	supported []language.Tag

	// Localizers are cached to avoid rebuilding them on every call.
	localizers   map[string]*gotext.Localizer
	localizersMu sync.RWMutex
}

// NewTranslator creates a Translator that loads messages from the provided files.
func NewTranslator(cfg Config) (Translator, error) {
	if cfg.DefaultLocale == "" {
		return nil, fmt.Errorf("%w: DefaultLocale is required", ErrBundleLoad)
	}
	if len(cfg.MessageFiles) == 0 {
		return nil, fmt.Errorf("%w: at least one MessageFile is required", ErrBundleLoad)
	}

	if cfg.Format == "" {
		cfg.Format = "json"
	}
	if cfg.Format != "json" {
		return nil, fmt.Errorf("%w: unsupported format %q (only 'json' is supported)", ErrBundleLoad, cfg.Format)
	}

	defaultLang, err := language.Parse(cfg.DefaultLocale)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid DefaultLocale %q: %w", ErrBundleLoad, cfg.DefaultLocale, err)
	}

	bundle := gotext.NewBundle(defaultLang)
	bundle.RegisterUnmarshalFunc("json", json.Unmarshal)

	var locales []string
	for _, file := range cfg.MessageFiles {
		mf, err := bundle.LoadMessageFile(file)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to load %q: %w", ErrBundleLoad, file, err)
		}
		if tag := mf.Tag.String(); !slices.Contains(locales, tag) {
			locales = append(locales, tag)
		}
	}

	// LanguageTags lists the default language first, which makes it the
	// matcher's answer for unsupported locales.
	supported := bundle.LanguageTags()

	return &translator{
		bundle:         bundle,
		locales:        locales,
		fallbackLocale: cfg.FallbackLocale,
		matcher:        language.NewMatcher(supported),
		supported:      supported,
		localizers:     make(map[string]*gotext.Localizer, len(supported)),
	}, nil
}

// getLocalizer handles the thread-safe retrieval or creation of a localizer.
// The cache key is the supported language the requested locale matches, so
// the cache never holds more entries than there are loaded languages.
func (t *translator) getLocalizer(locale string) (*gotext.Localizer, error) {
	lang, err := language.Parse(locale)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid locale %q: %w", ErrLocaleNotFound, locale, err)
	}
	_, idx, _ := t.matcher.Match(lang)
	key := t.supported[idx].String()

	// Fast path: check cache with a read lock
	t.localizersMu.RLock()
	loc, exists := t.localizers[key]
	t.localizersMu.RUnlock()

	if exists {
		return loc, nil
	}

	// Slow path: create with a write lock
	t.localizersMu.Lock()
	defer t.localizersMu.Unlock()

	// Check again: another goroutine may have created it while we waited.
	if loc, exists := t.localizers[key]; exists {
		return loc, nil
	}

	// The localizer tries the matched language first, then the fallback.
	langs := []string{key}
	if t.fallbackLocale != "" && t.fallbackLocale != key {
		langs = append(langs, t.fallbackLocale)
	}

	newLoc := gotext.NewLocalizer(t.bundle, langs...)
	t.localizers[key] = newLoc
	return newLoc, nil
}

func (t *translator) T(_ context.Context, locale, messageID string, templateData map[string]any, pluralCount int) (string, error) {
	localizer, err := t.getLocalizer(locale)
	if err != nil {
		return "", err
	}

	lc := &gotext.LocalizeConfig{
		MessageID:    messageID,
		TemplateData: templateData,
	}
	if pluralCount >= 0 {
		lc.PluralCount = pluralCount
	}

	msg, err := localizer.Localize(lc)
	if err != nil {
		if _, ok := errors.AsType[*gotext.MessageNotFoundErr](err); ok {
			return "", fmt.Errorf("%w: %w", ErrMessageNotFound, err)
		}
		return "", fmt.Errorf("%w: %w", ErrBundleLoad, err)
	}

	return msg, nil
}

func (t *translator) Locales() []string {
	return slices.Clone(t.locales)
}
