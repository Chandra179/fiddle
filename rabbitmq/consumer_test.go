package rabbitmq

import (
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

func TestApplyDefaults_ChannelMax(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.URL = "amqp://localhost"
	cfg.ChannelMax = 70000
	if err := cfg.ApplyDefaults(); err == nil {
		t.Fatal("expected error for ChannelMax > 65535")
	}
	cfg.ChannelMax = 2047
	if err := cfg.ApplyDefaults(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
