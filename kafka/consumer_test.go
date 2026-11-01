package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
)

type recordingIO struct {
	committed  []*kgo.Record
	published  []*kgo.Record
	publishErr error
	commitErr  error
}

func (r *recordingIO) CommitRecords(_ context.Context, records ...*kgo.Record) error {
	r.committed = append(r.committed, records...)
	return r.commitErr
}
func (r *recordingIO) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	var results kgo.ProduceResults
	for _, record := range records {
		r.published = append(r.published, record)
		results = append(results, kgo.ProduceResult{Record: record, Err: r.publishErr})
	}
	return results
}
func newTestConsumer(records *recordingIO) *consumer {
	if records == nil {
		records = &recordingIO{}
	}
	return &consumer{records: records, policy: quickPolicy(UnknownFailure), options: ConsumerOptions{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}}
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
	dlq := &recordingIO{}

	err := newTestConsumer(dlq).handleRecord(ctx, &kgo.Record{Topic: "t"}, func(ctx context.Context, _ *kgo.Record) error {
		cancel() // shutdown arrives while the handler runs
		<-ctx.Done()
		return ctx.Err()
	})

	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, dlq.published, "a record interrupted by shutdown must not be dead-lettered")
}

func TestHandleRecord_UnknownFailureDoesNotPublishOrCommit(t *testing.T) {
	records := &recordingIO{}
	c := newTestConsumer(records)
	c.options.DeadLetter = (&DeadLetterConfig{Topic: "failed"}).normalized(nil)
	boom := errors.New("service failure")
	calls := 0
	err := c.handleRecord(context.Background(), &kgo.Record{Topic: "orders"}, func(context.Context, *kgo.Record) error { calls++; return boom })
	require.ErrorIs(t, err, ErrConsumerPaused)
	require.ErrorIs(t, err, boom)
	assert.Equal(t, 3, calls)
	assert.Empty(t, records.published)
	assert.Empty(t, records.committed)
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
