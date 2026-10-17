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

func TestGetRetryCount(t *testing.T) {
	cases := []struct {
		name    string
		headers amqp.Table
		want    int
	}{
		{"no headers", nil, 0},
		{
			// One entry per queue+reason; the retry count lives in "count".
			name: "x-death count for this queue",
			headers: amqp.Table{"x-death": []any{
				amqp.Table{"queue": "orders.retry", "reason": "expired", "count": int64(4)},
				amqp.Table{"queue": "orders", "reason": "rejected", "count": int64(3)},
			}},
			want: 3,
		},
		{
			name: "x-death for other queue only",
			headers: amqp.Table{"x-death": []any{
				amqp.Table{"queue": "other", "reason": "rejected", "count": int64(7)},
			}},
			want: 0,
		},
		{
			name:    "quorum x-delivery-count fallback",
			headers: amqp.Table{"x-delivery-count": int64(2)},
			want:    2,
		},
		{
			name:    "client-side x-retry-count",
			headers: amqp.Table{"x-retry-count": int64(2)},
			want:    2,
		},
		{
			name: "largest of the counters wins",
			headers: amqp.Table{
				"x-retry-count":    int64(1),
				"x-delivery-count": int64(4),
				"x-death":          []any{amqp.Table{"queue": "orders", "count": int64(2)}},
			},
			want: 4,
		},
		{
			name: "malformed entries ignored",
			headers: amqp.Table{"x-death": []any{
				"garbage",
				amqp.Table{"queue": "orders", "count": "nope"},
			}},
			want: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := getRetryCount(amqp.Delivery{Headers: tc.headers}, "orders")
			if got != tc.want {
				t.Fatalf("getRetryCount = %d, want %d", got, tc.want)
			}
		})
	}
}

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
	op      string // ack | nack | reject
	requeue bool
}

// fakeAcker records how a delivery was settled.
type fakeAcker struct{ calls []ackCall }

func (f *fakeAcker) Ack(uint64, bool) error {
	f.calls = append(f.calls, ackCall{op: "ack"})
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

type republished struct {
	queue string
	d     amqp.Delivery
}

// fakeRepublisher records republished copies; err makes every publish fail.
type fakeRepublisher struct {
	sent []republished
	err  error
}

func (f *fakeRepublisher) publish(_ context.Context, queue string, d amqp.Delivery) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, republished{queue: queue, d: d})
	return nil
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newDelivery(acker *fakeAcker, headers amqp.Table) amqp.Delivery {
	return amqp.Delivery{Acknowledger: acker, Body: []byte(`{"id":1}`), Headers: headers}
}

func expectCalls(t *testing.T, got []ackCall, want ...ackCall) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("settlement calls = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("settlement calls = %+v, want %+v", got, want)
		}
	}
}

func TestHandleFailedDelivery(t *testing.T) {
	ctx := context.Background()

	t.Run("RetryEnabled rejects into the DLX retry queue", func(t *testing.T) {
		acker, pub := &fakeAcker{}, &fakeRepublisher{}
		cfg := &Config{RetryEnabled: true, MaxRetries: 3}
		handleFailedDelivery(ctx, cfg, discardLogger(), pub.publish, newDelivery(acker, nil), "orders")

		expectCalls(t, acker.calls, ackCall{op: "reject"})
		if len(pub.sent) != 0 {
			t.Fatalf("nothing may be republished, got %d", len(pub.sent))
		}
	})

	t.Run("without RetryEnabled the attempt is counted on a republished copy", func(t *testing.T) {
		acker, pub := &fakeAcker{}, &fakeRepublisher{}
		cfg := &Config{MaxRetries: 3}
		handleFailedDelivery(ctx, cfg, discardLogger(), pub.publish, newDelivery(acker, nil), "orders")

		expectCalls(t, acker.calls, ackCall{op: "ack"})
		if len(pub.sent) != 1 || pub.sent[0].queue != "orders" {
			t.Fatalf("expected one copy republished to orders, got %+v", pub.sent)
		}
		if got := pub.sent[0].d.Headers["x-retry-count"]; got != int64(1) {
			t.Fatalf("x-retry-count = %v, want 1", got)
		}
		if string(pub.sent[0].d.Body) != `{"id":1}` {
			t.Fatalf("body must be copied unchanged, got %q", pub.sent[0].d.Body)
		}
	})

	t.Run("retry copy keeps the caller's headers intact and drops x-delivery-count", func(t *testing.T) {
		acker, pub := &fakeAcker{}, &fakeRepublisher{}
		orig := amqp.Table{"x-delivery-count": int64(1), "tenant": "a"}
		handleFailedDelivery(ctx, &Config{MaxRetries: 3}, discardLogger(), pub.publish, newDelivery(acker, orig), "orders")

		copyHeaders := pub.sent[0].d.Headers
		if copyHeaders["tenant"] != "a" || copyHeaders["x-retry-count"] != int64(2) {
			t.Fatalf("unexpected copy headers %v", copyHeaders)
		}
		if _, ok := copyHeaders["x-delivery-count"]; ok {
			t.Fatalf("x-delivery-count is broker-managed and must not be republished: %v", copyHeaders)
		}
		if _, ok := orig["x-retry-count"]; ok || len(orig) != 2 {
			t.Fatalf("the delivery's own headers were mutated: %v", orig)
		}
	})

	t.Run("failed retry publish requeues instead of losing the message", func(t *testing.T) {
		acker, pub := &fakeAcker{}, &fakeRepublisher{err: errors.New("broker down")}
		handleFailedDelivery(ctx, &Config{MaxRetries: 3}, discardLogger(), pub.publish, newDelivery(acker, nil), "orders")

		expectCalls(t, acker.calls, ackCall{op: "nack", requeue: true})
	})

	t.Run("max retries moves to the parking lot", func(t *testing.T) {
		acker, pub := &fakeAcker{}, &fakeRepublisher{}
		cfg := &Config{MaxRetries: 3, DeadLetterEnabled: true}
		d := newDelivery(acker, amqp.Table{"x-retry-count": int64(3)})
		handleFailedDelivery(ctx, cfg, discardLogger(), pub.publish, d, "orders")

		expectCalls(t, acker.calls, ackCall{op: "ack"})
		if len(pub.sent) != 1 || pub.sent[0].queue != "orders.parking-lot" {
			t.Fatalf("expected a copy in orders.parking-lot, got %+v", pub.sent)
		}
		if pub.sent[0].d.Headers["x-retry-count"] != int64(3) {
			t.Fatalf("parked copy must keep its history unchanged: %v", pub.sent[0].d.Headers)
		}
	})

	t.Run("max retries without DeadLetterEnabled rejects", func(t *testing.T) {
		acker, pub := &fakeAcker{}, &fakeRepublisher{}
		d := newDelivery(acker, amqp.Table{"x-retry-count": int64(3)})
		handleFailedDelivery(ctx, &Config{MaxRetries: 3}, discardLogger(), pub.publish, d, "orders")

		expectCalls(t, acker.calls, ackCall{op: "reject"})
		if len(pub.sent) != 0 {
			t.Fatalf("nothing may be republished, got %d", len(pub.sent))
		}
	})

	t.Run("failed parking lot publish requeues", func(t *testing.T) {
		acker, pub := &fakeAcker{}, &fakeRepublisher{err: errors.New("broker down")}
		cfg := &Config{MaxRetries: 3, DeadLetterEnabled: true}
		d := newDelivery(acker, amqp.Table{"x-retry-count": int64(3)})
		handleFailedDelivery(ctx, cfg, discardLogger(), pub.publish, d, "orders")

		expectCalls(t, acker.calls, ackCall{op: "nack", requeue: true})
	})
}

// A handler that always fails must reach the parking lot after MaxRetries
// retries on a queue with no broker-side counter and no DLX: the loop that
// RetryEnabled=false used to leave unbounded.
func TestHandleFailedDelivery_BoundsUnboundedRequeueLoop(t *testing.T) {
	cfg := &Config{MaxRetries: 2, DeadLetterEnabled: true}
	pub := &fakeRepublisher{}
	headers := amqp.Table(nil)

	attempts, parked := 0, false
	for attempts < 100 && !parked {
		attempts++
		acker := &fakeAcker{}
		handleFailedDelivery(context.Background(), cfg, discardLogger(), pub.publish, newDelivery(acker, headers), "orders")

		last := pub.sent[len(pub.sent)-1]
		parked = last.queue == "orders.parking-lot"
		headers = last.d.Headers // the broker would deliver the copy next
	}

	if !parked || attempts != 3 {
		t.Fatalf("parked=%v after %d attempts, want parked after 3 (1 initial + MaxRetries)", parked, attempts)
	}
}

func TestConsumer_SettleFailure_PoisonSkipsRetries(t *testing.T) {
	// No DLX and no broker connection: a normal failure tries to republish (and
	// requeues when that fails), a poison message is settled straight away.
	c := &Consumer{client: &Client{config: &Config{MaxRetries: 3}}, logger: discardLogger()}
	ctx := context.Background()

	normal := &fakeAcker{}
	c.settleFailure(ctx, newDelivery(normal, nil), "orders", errors.New("db down"))
	expectCalls(t, normal.calls, ackCall{op: "nack", requeue: true})

	poison := &fakeAcker{}
	c.settleFailure(ctx, newDelivery(poison, nil), "orders", ErrPoisonMessage)
	expectCalls(t, poison.calls, ackCall{op: "reject"})

	wrapped := &fakeAcker{}
	c.settleFailure(ctx, newDelivery(wrapped, nil), "orders", errors.Join(errors.New("ctx"), ErrPoisonMessage))
	expectCalls(t, wrapped.calls, ackCall{op: "reject"})
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

// ---- retry backoff ------------------------------------------------------------

func TestRetryDelay(t *testing.T) {
	cases := []struct {
		base  time.Duration
		count int
		want  time.Duration
	}{
		{time.Second, 0, time.Second},
		{time.Second, 1, 2 * time.Second},
		{time.Second, 2, 4 * time.Second},
		{time.Second, 4, 16 * time.Second},
		{time.Second, 5, maxRetryDelay},   // 32s capped
		{time.Second, 500, maxRetryDelay}, // no overflow for huge counts
		{time.Minute, 0, maxRetryDelay},   // base above the cap
		{0, 3, 0},
		{-time.Second, 3, 0},
	}
	for _, tc := range cases {
		if got := retryDelay(tc.base, tc.count); got != tc.want {
			t.Errorf("retryDelay(%v, %d) = %v, want %v", tc.base, tc.count, got, tc.want)
		}
	}
}

func TestWaitBeforeRetry(t *testing.T) {
	ctx := context.Background()
	fresh := amqp.Delivery{}
	spent := amqp.Delivery{Headers: amqp.Table{"x-retry-count": int64(3)}}

	elapsed := func(cfg *Config, ds ...amqp.Delivery) (time.Duration, error) {
		start := time.Now()
		err := waitBeforeRetry(ctx, cfg, "orders", ds...)
		return time.Since(start), err
	}

	t.Run("waits the backoff for a counted retry", func(t *testing.T) {
		d, err := elapsed(&Config{MaxRetries: 3, RetryBackoff: 40 * time.Millisecond}, fresh)
		if err != nil || d < 40*time.Millisecond {
			t.Fatalf("waited %v (err %v), want at least 40ms", d, err)
		}
	})

	t.Run("no wait when RetryEnabled, the retry queue TTL is the delay", func(t *testing.T) {
		d, err := elapsed(&Config{MaxRetries: 3, RetryEnabled: true, RetryBackoff: time.Hour}, fresh)
		if err != nil || d > 100*time.Millisecond {
			t.Fatalf("waited %v (err %v), want none", d, err)
		}
	})

	t.Run("no wait when disabled or when nothing will be retried", func(t *testing.T) {
		for _, cfg := range []*Config{
			{MaxRetries: 3, RetryBackoff: -1},
			{MaxRetries: 3, RetryBackoff: time.Hour}, // used with spent below
		} {
			ds := []amqp.Delivery{fresh}
			if cfg.RetryBackoff > 0 {
				ds = []amqp.Delivery{spent}
			}
			d, err := elapsed(cfg, ds...)
			if err != nil || d > 100*time.Millisecond {
				t.Fatalf("cfg %+v: waited %v (err %v), want none", cfg, d, err)
			}
		}
	})

	t.Run("a batch waits once, for its longest need", func(t *testing.T) {
		twice := amqp.Delivery{Headers: amqp.Table{"x-retry-count": int64(1)}} // needs 2*base
		d, err := elapsed(&Config{MaxRetries: 3, RetryBackoff: 30 * time.Millisecond}, fresh, twice, fresh, spent)
		if err != nil || d < 60*time.Millisecond || d > 500*time.Millisecond {
			t.Fatalf("waited %v (err %v), want about 60ms", d, err)
		}
	})

	t.Run("returns when the context ends", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		time.AfterFunc(20*time.Millisecond, cancel)
		start := time.Now()
		err := waitBeforeRetry(cctx, &Config{MaxRetries: 3, RetryBackoff: time.Hour}, "orders", fresh)
		if !errors.Is(err, context.Canceled) || time.Since(start) > time.Second {
			t.Fatalf("err = %v after %v, want context.Canceled promptly", err, time.Since(start))
		}
	})
}

func TestSettleFailure_InterruptedWaitLeavesMessageUnacked(t *testing.T) {
	c := &Consumer{
		client: &Client{config: &Config{MaxRetries: 3, RetryBackoff: time.Hour}},
		logger: discardLogger(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	acker := &fakeAcker{}
	c.settleFailure(ctx, newDelivery(acker, nil), "orders", errors.New("db down"))
	expectCalls(t, acker.calls) // untouched: closing the channel redelivers it
}

// ---- MaxRetries semantics --------------------------------------------------------

func TestNegativeMaxRetriesMeansNoRetries(t *testing.T) {
	cfg := &Config{URL: "amqp://localhost", MaxRetries: -1}
	cfg.ApplyDefaults()
	if cfg.MaxRetries != -1 {
		t.Fatalf("ApplyDefaults must keep an explicit negative MaxRetries, got %d", cfg.MaxRetries)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("negative MaxRetries must validate: %v", err)
	}

	acker, pub := &fakeAcker{}, &fakeRepublisher{}
	handleFailedDelivery(context.Background(), cfg, discardLogger(), pub.publish, newDelivery(acker, nil), "orders")
	expectCalls(t, acker.calls, ackCall{op: "reject"}) // first failure is already final
	if len(pub.sent) != 0 {
		t.Fatalf("nothing may be republished, got %d", len(pub.sent))
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

// ---- mixed retry headers ----------------------------------------------------------------

// RetryEnabled may be toggled between deploys, leaving a message with several
// counters. The largest wins, so a message is never retried more than
// MaxRetries times in total, and the republished copy continues from it.
func TestRetryCount_MixedHeadersNeverUndercount(t *testing.T) {
	mixed := amqp.Table{
		"x-death":          []any{amqp.Table{"queue": "orders", "count": int64(1)}},
		"x-retry-count":    int64(2),
		"x-delivery-count": int64(1),
	}
	if got := getRetryCount(amqp.Delivery{Headers: mixed}, "orders"); got != 2 {
		t.Fatalf("getRetryCount = %d, want 2 (the largest)", got)
	}

	acker, pub := &fakeAcker{}, &fakeRepublisher{}
	handleFailedDelivery(context.Background(), &Config{MaxRetries: 4}, discardLogger(), pub.publish, newDelivery(acker, mixed), "orders")
	next := pub.sent[0].d.Headers
	if next["x-retry-count"] != int64(3) {
		t.Fatalf("copy continues from the largest counter: x-retry-count = %v, want 3", next["x-retry-count"])
	}
	if _, ok := next["x-delivery-count"]; ok {
		t.Fatal("x-delivery-count is broker-managed and must not be republished")
	}
	if _, ok := next["x-death"]; !ok {
		t.Fatal("x-death history must be kept")
	}

	// At the limit it parks, whichever counter got it there.
	at := amqp.Table{"x-delivery-count": int64(4), "x-retry-count": int64(1)}
	acker, pub = &fakeAcker{}, &fakeRepublisher{}
	handleFailedDelivery(context.Background(), &Config{MaxRetries: 4, DeadLetterEnabled: true}, discardLogger(), pub.publish, newDelivery(acker, at), "orders")
	if len(pub.sent) != 1 || pub.sent[0].queue != "orders.parking-lot" {
		t.Fatalf("expected parking, got %+v", pub.sent)
	}
}
