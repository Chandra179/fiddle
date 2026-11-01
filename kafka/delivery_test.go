package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func fetchRecords(records ...*kgo.Record) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "orders", Partitions: []kgo.FetchPartition{{Partition: 0, Records: records}}}}}}
}

func TestFailedRecordStopsOffsetCommitBoundary(t *testing.T) {
	for _, kind := range []FailureKind{UnknownFailure, PermanentFailure, DependencyUnavailable, ConfigurationFailure} {
		t.Run(kind.String(), func(t *testing.T) {
			io := &recordingIO{}
			c := newTestConsumer(io)
			c.policy = quickPolicy(kind)
			source := []*kgo.Record{{Topic: "orders", Offset: 10}, {Topic: "orders", Offset: 11}, {Topic: "orders", Offset: 12}}
			boom := errors.New("failed to read cards")
			var seen []int64
			err := c.processFetches(context.Background(), fetchRecords(source...), func(_ context.Context, r *kgo.Record) error {
				seen = append(seen, r.Offset)
				if r.Offset == 11 {
					return boom
				}
				return nil
			})
			require.ErrorIs(t, err, boom)
			require.ErrorIs(t, err, ErrConsumerPaused)
			require.Equal(t, source[:1], io.committed)
			require.NotContains(t, seen, int64(12))
			require.Empty(t, io.published)
		})
	}
}

func TestDeadLetterFailureDoesNotCommitOriginal(t *testing.T) {
	for _, publishErr := range []error{nil, errors.New("DLQ unavailable")} {
		io := &recordingIO{publishErr: publishErr}
		c := newTestConsumer(io)
		c.policy = quickPolicy(PermanentFailure)
		c.options.DeadLetter = (&DeadLetterConfig{Topic: "cards.failed"}).normalized(nil)
		source := &kgo.Record{Topic: "cards", Offset: 5, Key: []byte("id"), Value: []byte("raw body"), Headers: []kgo.RecordHeader{{Key: "trace", Value: []byte("trace-id")}}}
		err := c.processFetches(context.Background(), fetchRecords(source), func(context.Context, *kgo.Record) error { return errors.New("invalid card") })
		require.Len(t, io.published, 1)
		require.Equal(t, "cards.failed", io.published[0].Topic)
		require.Equal(t, source.Value, io.published[0].Value)
		require.Equal(t, source.Key, io.published[0].Key)
		require.Contains(t, io.published[0].Headers, source.Headers[0])
		if publishErr == nil {
			require.NoError(t, err)
			require.Equal(t, []*kgo.Record{source}, io.committed)
		} else {
			require.ErrorIs(t, err, ErrDLQPublish)
			require.ErrorIs(t, err, publishErr)
			require.Empty(t, io.committed)
		}
	}
}

func TestCommitFailurePreservesProcessingCause(t *testing.T) {
	commitErr, handlerErr := errors.New("commit unavailable"), errors.New("handler failed")
	io := &recordingIO{commitErr: commitErr}
	c := newTestConsumer(io)
	c.policy = quickPolicy(DependencyUnavailable)
	err := c.processFetches(context.Background(), fetchRecords(&kgo.Record{Offset: 1}, &kgo.Record{Offset: 2}), func(_ context.Context, r *kgo.Record) error {
		if r.Offset == 2 {
			return handlerErr
		}
		return nil
	})
	require.ErrorIs(t, err, handlerErr)
	require.ErrorIs(t, err, commitErr)
}

func TestDeadLetterConfigValidationAndCopy(t *testing.T) {
	original := &DeadLetterConfig{Configs: map[string]string{"retention.ms": "86400000"}}
	d := original.normalized([]string{"orders"})
	require.Equal(t, "orders.dlq", d.Topic)
	require.NoError(t, d.validate([]string{"orders"}))
	d.Configs["retention.ms"] = "1000"
	require.Equal(t, "86400000", original.Configs["retention.ms"])
	d.Topic = "orders"
	require.Error(t, d.validate([]string{"orders"}))
	d.Topic = "failed"
	d.Provision = Declare
	require.Error(t, d.validate(nil))
	d.Partitions = 3
	d.ReplicationFactor = 3
	require.NoError(t, d.validate(nil))
	require.Error(t, NewConsumer(nil, ConsumerOptions{RetryPolicy: &RetryPolicy{OnExhausted: DeadLetter}}).Start(context.Background(), nil))
}
