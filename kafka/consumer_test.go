package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
)

// recordingDLQ is a Producer that only records SendToDLQ calls.
type recordingDLQ struct {
	calls []error
}

func (*recordingDLQ) Produce(context.Context, *Message) error        { return nil }
func (*recordingDLQ) SendMessage(context.Context, string, any) error { return nil }
func (d *recordingDLQ) SendToDLQ(_ context.Context, _ string, _, _ []byte, err error) error {
	d.calls = append(d.calls, err)
	return nil
}

func newTestConsumer(dlq Producer) *consumer {
	return &consumer{options: ConsumerOptions{
		DLQProducer:  dlq,
		MaxRetries:   2,
		RetryBackoff: time.Millisecond,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}}
}

func TestHandleRecord_PassesContextToHandler(t *testing.T) {
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "v")

	var got context.Context
	err := newTestConsumer(nil).handleRecord(ctx, &kgo.Record{Topic: "t"}, func(ctx context.Context, _ *kgo.Record) error {
		got = ctx
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, "v", got.Value(key{}))
}

func TestHandleRecord_HandlerSeesShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dlq := &recordingDLQ{}

	err := newTestConsumer(dlq).handleRecord(ctx, &kgo.Record{Topic: "t"}, func(ctx context.Context, _ *kgo.Record) error {
		cancel() // shutdown arrives while the handler runs
		<-ctx.Done()
		return ctx.Err()
	})

	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, dlq.calls, "a record interrupted by shutdown must not be dead-lettered")
}

func TestHandleRecord_DeadLettersAfterRetries(t *testing.T) {
	dlq := &recordingDLQ{}
	boom := errors.New("boom")
	attempts := 0

	err := newTestConsumer(dlq).handleRecord(context.Background(), &kgo.Record{Topic: "t"}, func(context.Context, *kgo.Record) error {
		attempts++
		return boom
	})

	require.NoError(t, err)
	assert.Equal(t, 3, attempts, "1 attempt + MaxRetries retries")
	assert.Equal(t, []error{boom}, dlq.calls)
}

func TestRecordContext_CarriesRecordSpan(t *testing.T) {
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1},
		SpanID:     trace.SpanID{2},
		TraceFlags: trace.FlagsSampled,
	})
	rec := &kgo.Record{Context: trace.ContextWithSpanContext(context.Background(), sc)}

	parent, cancel := context.WithCancel(context.Background())
	ctx := recordContext(parent, rec)
	assert.Equal(t, sc.TraceID(), trace.SpanContextFromContext(ctx).TraceID())

	cancel()
	assert.ErrorIs(t, ctx.Err(), context.Canceled, "cancellation must come from the Start context")

	assert.Equal(t, parent, recordContext(parent, &kgo.Record{}), "no record span leaves ctx untouched")
}
