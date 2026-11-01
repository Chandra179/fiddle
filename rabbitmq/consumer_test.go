package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestJitter(t *testing.T) {
	d := time.Second
	for range 1000 {
		j := jitter(d)
		if j < d/2 || j >= d {
			t.Fatalf("jitter(%v) = %v, want in [%v, %v)", d, j, d/2, d)
		}
	}
	if got := jitter(1); got != 1 {
		t.Fatalf("jitter(1ns) = %v, want 1ns", got)
	}
}

func TestUpsert(t *testing.T) {
	byName := func(name string) func(QueueConfig) bool {
		return func(q QueueConfig) bool { return q.Name == name }
	}
	var qs []QueueConfig
	qs = upsert(qs, QueueConfig{Name: "a"}, byName("a"))
	qs = upsert(qs, QueueConfig{Name: "b"}, byName("b"))
	qs = upsert(qs, QueueConfig{Name: "a", Durable: true}, byName("a"))
	if len(qs) != 2 || !qs[0].Durable {
		t.Fatalf("unexpected cache: %+v", qs)
	}
}

func TestValidate_ChannelMax(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.URL = "amqp://localhost"
	cfg.ChannelMax = 70000
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for ChannelMax > 65535")
	}
	cfg.ChannelMax = 2047
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConfig_ApplyDefaults(t *testing.T) {
	cfg := &Config{URL: "amqp://localhost"}
	cfg.ApplyDefaults()

	want := NewDefaultConfig()
	if cfg.ConnectionName != want.ConnectionName ||
		cfg.ChannelPoolSize != want.ChannelPoolSize ||
		cfg.PrefetchCount != want.PrefetchCount ||
		cfg.RetryBackoff != want.RetryBackoff ||
		cfg.QueueType != want.QueueType ||
		cfg.MaxRetries != want.MaxRetries ||
		cfg.ReconnectInitialInterval != want.ReconnectInitialInterval ||
		cfg.ReconnectMaxInterval != want.ReconnectMaxInterval {
		t.Fatalf("zero fields not defaulted: %+v", cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("defaulted config must validate: %v", err)
	}

	custom := &Config{URL: "u", ConnectionName: "svc", ChannelPoolSize: 3, MaxRetries: 9, QueueType: QueueTypeClassic}
	custom.ApplyDefaults()
	if custom.ConnectionName != "svc" || custom.ChannelPoolSize != 3 || custom.MaxRetries != 9 || custom.QueueType != QueueTypeClassic {
		t.Fatalf("explicit values must be kept: %+v", custom)
	}
}

func TestConfig_Validate(t *testing.T) {
	if err := (&Config{}).Validate(); !errors.Is(err, ErrURLRequired) {
		t.Fatalf("expected ErrURLRequired, got %v", err)
	}

	// Validate never fills in defaults.
	if err := (&Config{URL: "amqp://localhost"}).Validate(); err == nil {
		t.Fatal("unset required fields must fail Validate")
	}

	// Negative values are not "unset": ApplyDefaults leaves them for Validate.
	cfg := &Config{URL: "amqp://localhost", ChannelPoolSize: -1}
	cfg.ApplyDefaults()
	if cfg.ChannelPoolSize != -1 {
		t.Fatalf("ApplyDefaults must not touch negative values, got %d", cfg.ChannelPoolSize)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("negative pool size must fail Validate")
	}
}

func TestNewClient_DoesNotModifyConfig(t *testing.T) {
	cfg := &Config{} // no URL: fails validation before any dial
	if _, err := NewClient(cfg, nil); !errors.Is(err, ErrURLRequired) {
		t.Fatalf("expected ErrURLRequired, got %v", err)
	}
	if cfg.ConnectionName != "" || cfg.ChannelPoolSize != 0 {
		t.Fatalf("NewClient must not write defaults into the caller's config: %+v", cfg)
	}
	if _, err := NewClient(nil, nil); err == nil {
		t.Fatal("nil config must fail")
	}
}

// ---- failure settlement -----------------------------------------------------

type ackCall struct {
	op       string // ack | nack | reject
	requeue  bool
	multiple bool
}

// fakeAcker records how a delivery was settled.
type fakeAcker struct{ calls []ackCall }

func (f *fakeAcker) Ack(_ uint64, multiple bool) error {
	f.calls = append(f.calls, ackCall{op: "ack", multiple: multiple})
	return nil
}
func (f *fakeAcker) Nack(_ uint64, _, requeue bool) error {
	f.calls = append(f.calls, ackCall{op: "nack", requeue: requeue})
	return nil
}
func (f *fakeAcker) Reject(_ uint64, requeue bool) error {
	f.calls = append(f.calls, ackCall{op: "reject", requeue: requeue})
	return nil
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newDelivery(acker *fakeAcker, headers amqp.Table) amqp.Delivery {
	return amqp.Delivery{Acknowledger: acker, Body: []byte(`{"id":1}`), Headers: headers}
}

// ---- handlers ----------------------------------------------------------------

type order struct {
	ID    int64  `json:"id"`
	Total string `json:"total"`
}

func TestJSONHandler(t *testing.T) {
	var got order
	h := JSONHandler(func(_ context.Context, o order, _ amqp.Delivery) error {
		got = o
		return nil
	})

	// 2^53+1 is not representable as float64; a typed target keeps it exact.
	body := `{"id":9007199254740993,"total":"12.50"}`
	if err := h(context.Background(), amqp.Delivery{Body: []byte(body)}); err != nil {
		t.Fatal(err)
	}
	if got.ID != 9007199254740993 || got.Total != "12.50" {
		t.Fatalf("decoded %+v", got)
	}

	err := h(context.Background(), amqp.Delivery{Body: []byte("not json")})
	if !errors.Is(err, ErrPoisonMessage) {
		t.Fatalf("undecodable body must be a poison message, got %v", err)
	}

	handlerErr := errors.New("db down")
	failing := JSONHandler(func(context.Context, order, amqp.Delivery) error { return handlerErr })
	if err := failing(context.Background(), amqp.Delivery{Body: []byte(body)}); !errors.Is(err, handlerErr) || errors.Is(err, ErrPoisonMessage) {
		t.Fatalf("handler errors pass through unchanged, got %v", err)
	}
}

func TestRawHandlerAcceptsNonJSON(t *testing.T) {
	var got []byte
	var h MessageHandler = func(_ context.Context, d amqp.Delivery) error {
		got = d.Body
		return nil
	}
	payload := []byte{0x00, 0xff, 0x10}
	if err := h(context.Background(), amqp.Delivery{Body: payload, ContentType: "application/octet-stream"}); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("raw body not passed through: %v", got)
	}
}

func TestJSONBatchHandler(t *testing.T) {
	var got []order
	h := JSONBatchHandler(func(_ context.Context, msgs []order, ds []amqp.Delivery) error {
		if len(msgs) != len(ds) {
			t.Fatalf("msgs/deliveries length mismatch: %d vs %d", len(msgs), len(ds))
		}
		got = msgs
		return nil
	})

	batch := []amqp.Delivery{{Body: []byte(`{"id":1}`)}, {Body: []byte(`{"id":2}`)}}
	if err := h(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != 1 || got[1].ID != 2 {
		t.Fatalf("decoded %+v", got)
	}

	bad := []amqp.Delivery{{Body: []byte(`{"id":1}`)}, {Body: []byte(`{`)}}
	err := h(context.Background(), bad)
	if !errors.Is(err, ErrPoisonMessage) || !strings.Contains(err.Error(), "message 1") {
		t.Fatalf("expected a poison error naming message 1, got %v", err)
	}
}

func TestValidJSON(t *testing.T) {
	if err := ValidJSON(amqp.Delivery{Body: []byte(`{"a":1}`)}); err != nil {
		t.Fatal(err)
	}
	if err := ValidJSON(amqp.Delivery{Body: []byte(`nope`)}); !errors.Is(err, ErrPoisonMessage) {
		t.Fatalf("expected ErrPoisonMessage, got %v", err)
	}
}

func TestWithBatchValidator(t *testing.T) {
	bc := NewBatchConsumer(nil, discardLogger(), 10, time.Second)
	if bc.validate != nil {
		t.Fatal("no validator by default: payloads are not assumed to be JSON")
	}
	bc = NewBatchConsumer(nil, discardLogger(), 10, time.Second, WithBatchValidator(ValidJSON))
	if bc.validate == nil {
		t.Fatal("validator not installed")
	}
}

func TestApplyDefaults_ExplicitZeroMaxRetriesIsTheDefault(t *testing.T) {
	cfg := &Config{MaxRetries: 0, RetryBackoff: -1}
	cfg.ApplyDefaults()
	if cfg.MaxRetries != DefaultMaxRetries {
		t.Fatalf("zero is unset and becomes the default, got %d", cfg.MaxRetries)
	}
	if cfg.RetryBackoff != -1 {
		t.Fatalf("a negative RetryBackoff (no delay) must be kept, got %v", cfg.RetryBackoff)
	}
}

// ---- republished copies ------------------------------------------------------------

func TestPublishingFrom_KeepsMessageProperties(t *testing.T) {
	ts := time.Now()
	d := amqp.Delivery{
		Headers:         amqp.Table{"k": "v"},
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		DeliveryMode:    amqp.Persistent,
		Priority:        5,
		CorrelationId:   "corr",
		ReplyTo:         "reply",
		Expiration:      "60000",
		MessageId:       "id-1",
		Timestamp:       ts,
		Type:            "order.created",
		UserId:          "producer-user",
		AppId:           "app",
		Body:            []byte("body"),
	}
	p := publishingFrom(d)

	if p.Expiration != "60000" {
		t.Errorf("Expiration = %q: a per-message TTL must survive a retry", p.Expiration)
	}
	if p.UserId != "" {
		t.Errorf("UserId = %q: the broker rejects a user-id that differs from the connection's user", p.UserId)
	}
	if p.ContentType != d.ContentType || p.ContentEncoding != d.ContentEncoding || p.DeliveryMode != d.DeliveryMode ||
		p.Priority != d.Priority || p.CorrelationId != d.CorrelationId || p.ReplyTo != d.ReplyTo ||
		p.MessageId != d.MessageId || !p.Timestamp.Equal(ts) || p.Type != d.Type || p.AppId != d.AppId ||
		string(p.Body) != "body" || p.Headers["k"] != "v" {
		t.Errorf("properties not carried over: %+v", p)
	}
}

// ---- topology restore ----------------------------------------------------------------

func TestIsBrokerRejection(t *testing.T) {
	rejected := &amqp.Error{Code: amqp.PreconditionFailed, Reason: "inequivalent arg", Server: true}
	if !isBrokerRejection(rejected) || !isBrokerRejection(fmt.Errorf("declare: %w", rejected)) {
		t.Error("a server-initiated channel exception is a broker rejection, wrapped or not")
	}
	if isBrokerRejection(&amqp.Error{Code: amqp.ChannelError, Reason: "EOF", Server: false}) {
		t.Error("a client-side error is not a rejection")
	}
	if isBrokerRejection(errors.New("EOF")) || isBrokerRejection(amqp.ErrClosed) {
		t.Error("transport errors are not rejections")
	}
}
