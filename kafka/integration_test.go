package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Optional broker test; no Docker is required. This creates/deletes a unique
// topic on the disposable cluster named by FIDDLE_KAFKA_BROKERS.
func TestIntegrationRestartRedeliversFailedRecord(t *testing.T) {
	brokers := os.Getenv("FIDDLE_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("set FIDDLE_KAFKA_BROKERS for broker verification")
	}
	topic := fmt.Sprintf("fiddle-test-%d", time.Now().UnixNano())
	group := topic + "-group"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	producer, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	admin := kadm.NewClient(producer)
	created, err := admin.CreateTopics(ctx, 1, 1, nil, topic)
	if err != nil {
		t.Fatal(err)
	}
	if result, ok := created[topic]; !ok || result.Err != nil {
		t.Fatalf("create topic: %+v", created)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = admin.DeleteTopics(cleanup, topic)
	}()
	for _, value := range []string{"ok", "failed", "later"} {
		if err = producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(value)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	newClient := func() *kgo.Client {
		client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic),
			kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	client := newClient()
	var seen []string
	err = NewConsumer(client, ConsumerOptions{RetryPolicy: &RetryPolicy{Classify: func(error) FailureKind { return DependencyUnavailable }}}).Start(ctx, func(_ context.Context, r *kgo.Record) error {
		seen = append(seen, string(r.Value))
		if string(r.Value) == "failed" {
			return errors.New("service down")
		}
		return nil
	})
	client.Close()
	if !errors.Is(err, ErrConsumerPaused) || len(seen) != 2 {
		t.Fatalf("err=%v processed=%v", err, seen)
	}
	client = newClient()
	defer client.Close()
	var redelivered string
	err = NewConsumer(client, ConsumerOptions{RetryPolicy: &RetryPolicy{}}).Start(ctx, func(_ context.Context, r *kgo.Record) error {
		redelivered = string(r.Value)
		return errors.New("stop after observing first record")
	})
	if !errors.Is(err, ErrConsumerPaused) || redelivered != "failed" {
		t.Fatalf("err=%v first_redelivered=%q", err, redelivered)
	}
}
