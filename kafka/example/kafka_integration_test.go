//go:build integration

package example

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"kafka"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tc_kafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestMessage is a sample message type for testing.
type TestMessage struct {
	ID      string    `json:"id"`
	Content string    `json:"content"`
	Created time.Time `json:"created"`
}

var (
	sharedBrokers []string
	sharedClient  *kgo.Client
)

// TestMain starts one Kafka container for the whole suite, runs the tests,
// then terminates the container.
func TestMain(m *testing.M) {
	ctx := context.Background()

	kafkaContainer, err := tc_kafka.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		tc_kafka.WithClusterID("test-cluster"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "true",
		}),
	)
	if err != nil {
		log.Fatalf("start kafka container: %v", err)
	}

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		log.Fatalf("get brokers: %v", err)
	}
	sharedBrokers = brokers

	cfg := kafka.DefaultConfig(brokers, "")
	sharedClient, err = kafka.NewKafkaClientFromConfig(ctx, cfg)
	if err != nil {
		log.Fatalf("create kafka client: %v", err)
	}

	code := m.Run()

	sharedClient.Close()
	if err := kafkaContainer.Terminate(ctx); err != nil {
		log.Printf("warn: terminate container: %v", err)
	}

	os.Exit(code)
}

// newClient creates a Kafka client for testing with automatic cleanup.
func newClient(t *testing.T, brokers []string, groupID string, topics ...string) *kgo.Client {
	t.Helper()
	cfg := kafka.DefaultConfig(brokers, groupID)
	if len(topics) > 0 {
		cfg = cfg.WithTopics(topics...)
	}
	client, err := kafka.NewKafkaClientFromConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	return client
}

// ensureTopicExists creates a Kafka topic if it doesn't exist.
func ensureTopicExists(t *testing.T, topic string) {
	t.Helper()
	adminClient := kadm.NewClient(sharedClient)
	_, err := adminClient.CreateTopics(context.Background(), 1, 1, nil, topic)
	require.NoError(t, err, "failed to create topic %s", topic)
}

// testLogger returns a slog.Logger that writes to t.Log.
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(testWriter{t}, nil))
}

type testWriter struct {
	t *testing.T
}

func (tw testWriter) Write(p []byte) (n int, err error) {
	tw.t.Log(string(p))
	return len(p), nil
}

// ---- Tests -----------------------------------------------------------------

func TestProducerBasic(t *testing.T) {
	topic := "test-producer-basic"
	ensureTopicExists(t, topic)

	client := newClient(t, sharedBrokers, "test-group-producer")
	producer := kafka.NewProducer(client)

	testMsg := TestMessage{
		ID:      "msg-1",
		Content: "Hello, Kafka!",
		Created: time.Now(),
	}

	err := producer.SendMessage(context.Background(), topic, testMsg)
	assert.NoError(t, err, "failed to produce message")

	msg := &kafka.Message{
		Topic:   topic,
		Payload: testMsg,
		Key:     "test-key-1",
		Headers: map[string]string{
			"source":      "integration-test",
			"version":     "1.0",
			"environment": "test",
		},
	}

	err = producer.Produce(context.Background(), msg)
	assert.NoError(t, err, "failed to produce message with headers")
}

func TestProducerAsync(t *testing.T) {
	topic := "test-producer-async"
	ensureTopicExists(t, topic)

	client := newClient(t, sharedBrokers, "test-group-async")
	producer := kafka.NewProducer(client)

	var wg sync.WaitGroup
	var callbackErr error
	var mu sync.Mutex

	wg.Add(1)
	msg := &kafka.Message{
		Topic:   topic,
		Payload: TestMessage{ID: "async-1", Content: "Async message"},
		Callback: func(r *kgo.Record, err error) {
			mu.Lock()
			defer mu.Unlock()
			callbackErr = err
			wg.Done()
		},
	}

	err := producer.Produce(context.Background(), msg)
	assert.NoError(t, err)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		mu.Lock()
		assert.NoError(t, callbackErr, "callback received error")
		mu.Unlock()
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
	}
}

func TestConsumerBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := "test-consumer-basic"
	ensureTopicExists(t, topic)

	producerClient := newClient(t, sharedBrokers, "")
	producer := kafka.NewProducer(producerClient)

	consumerClient := newClient(t, sharedBrokers, "test-consumer-group", topic)

	testMessages := []TestMessage{
		{ID: "1", Content: "First message", Created: time.Now()},
		{ID: "2", Content: "Second message", Created: time.Now()},
		{ID: "3", Content: "Third message", Created: time.Now()},
	}

	var consumed []TestMessage
	var mu sync.Mutex

	handler := func(r *kgo.Record) error {
		var msg TestMessage
		if err := json.Unmarshal(r.Value, &msg); err != nil {
			return err
		}
		mu.Lock()
		consumed = append(consumed, msg)
		if len(consumed) >= len(testMessages) {
			cancel()
		}
		mu.Unlock()
		return nil
	}

	consumer := kafka.NewConsumer(consumerClient)
	go func() {
		if err := consumer.Start(ctx, handler); err != nil && err != context.Canceled {
			t.Logf("consumer error: %v", err)
		}
	}()

	time.Sleep(2 * time.Second)

	for _, msg := range testMessages {
		err := producer.SendMessage(ctx, topic, msg)
		require.NoError(t, err)
	}

	<-ctx.Done()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, len(testMessages), len(consumed), "wrong number of messages consumed")
}

func TestProducerConsumerFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := "test-flow"
	logger := testLogger(t)
	ensureTopicExists(t, topic)

	producerClient := newClient(t, sharedBrokers, "")
	producer := kafka.NewProducer(producerClient, kafka.WithProducerLogger(logger))

	consumerClient := newClient(t, sharedBrokers, "test-flow-group", topic)

	expectedCount := 10
	var receivedCount int
	var mu sync.Mutex

	handler := func(r *kgo.Record) error {
		var msg TestMessage
		if err := json.Unmarshal(r.Value, &msg); err != nil {
			return err
		}
		mu.Lock()
		receivedCount++
		t.Logf("Received message %d: %s", receivedCount, msg.ID)
		if receivedCount >= expectedCount {
			cancel()
		}
		mu.Unlock()
		return nil
	}

	consumer := kafka.NewConsumer(consumerClient, kafka.ConsumerOptions{
		Logger:     logger,
		MaxRetries: 3,
	})
	go func() {
		_ = consumer.Start(ctx, handler)
	}()

	time.Sleep(2 * time.Second)

	for i := range expectedCount {
		msg := TestMessage{
			ID:      fmt.Sprintf("msg-%d", i),
			Content: fmt.Sprintf("Message number %d", i),
			Created: time.Now(),
		}
		err := producer.SendMessage(ctx, topic, msg)
		require.NoError(t, err)
		t.Logf("Produced message %d", i)
	}

	<-ctx.Done()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, expectedCount, receivedCount, "not all messages consumed")
}

func TestConsumerWithDLQ(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := "test-dlq"
	logger := testLogger(t)
	ensureTopicExists(t, topic)
	ensureTopicExists(t, topic+".dlq")

	producerClient := newClient(t, sharedBrokers, "")
	producer := kafka.NewProducer(producerClient, kafka.WithProducerLogger(logger))

	dlqClient := newClient(t, sharedBrokers, "")
	dlqProducer := kafka.NewProducer(dlqClient, kafka.WithProducerLogger(logger))

	consumerClient := newClient(t, sharedBrokers, "test-dlq-group", topic)

	var dlqCount int
	var mu sync.Mutex

	handler := func(r *kgo.Record) error {
		var msg TestMessage
		if err := json.Unmarshal(r.Value, &msg); err != nil {
			return err
		}
		if msg.Content == "fail" {
			return fmt.Errorf("intentional failure for testing")
		}
		return nil
	}

	consumer := kafka.NewConsumer(consumerClient, kafka.ConsumerOptions{
		Logger:      logger,
		DLQProducer: dlqProducer,
		MaxRetries:  2,
		OnDLQPublish: func(topic string, err error) {
			mu.Lock()
			dlqCount++
			mu.Unlock()
			t.Logf("Message sent to DLQ: %s - %v", topic, err)
		},
	})
	go func() {
		_ = consumer.Start(ctx, handler)
	}()

	time.Sleep(2 * time.Second)

	messages := []TestMessage{
		{ID: "1", Content: "success", Created: time.Now()},
		{ID: "2", Content: "fail", Created: time.Now()},
		{ID: "3", Content: "success", Created: time.Now()},
		{ID: "4", Content: "fail", Created: time.Now()},
	}

	for _, msg := range messages {
		err := producer.SendMessage(ctx, topic, msg)
		require.NoError(t, err)
	}

	time.Sleep(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 2, dlqCount, "expected 2 messages in DLQ")
}

// failingDLQ is a Producer whose DLQ publishes always fail.
type failingDLQ struct{}

func (failingDLQ) Produce(context.Context, *kafka.Message) error  { return nil }
func (failingDLQ) SendMessage(context.Context, string, any) error { return nil }
func (failingDLQ) SendToDLQ(context.Context, string, []byte, []byte, error) error {
	return errors.New("dlq unavailable")
}

func TestConsumerStopsWhenDLQFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := "test-dlq-failure"
	group := "test-dlq-failure-group"
	ensureTopicExists(t, topic)

	producerClient := newClient(t, sharedBrokers, "")
	producer := kafka.NewProducer(producerClient)

	consumerClient := newClient(t, sharedBrokers, group, topic)

	var handled []string
	var mu sync.Mutex

	handler := func(r *kgo.Record) error {
		var msg TestMessage
		if err := json.Unmarshal(r.Value, &msg); err != nil {
			return err
		}
		mu.Lock()
		handled = append(handled, msg.ID)
		mu.Unlock()
		if msg.Content == "fail" {
			return errors.New("intentional failure for testing")
		}
		return nil
	}

	consumer := kafka.NewConsumer(consumerClient, kafka.ConsumerOptions{
		Logger:       testLogger(t),
		DLQProducer:  failingDLQ{},
		MaxRetries:   1,
		RetryBackoff: 10 * time.Millisecond,
	})
	startErr := make(chan error, 1)
	go func() {
		startErr <- consumer.Start(ctx, handler)
	}()

	time.Sleep(2 * time.Second)

	// Offsets 0, 1, 2 on the single partition; offset 1 cannot be dead-lettered.
	for _, msg := range []TestMessage{
		{ID: "1", Content: "success"},
		{ID: "2", Content: "fail"},
		{ID: "3", Content: "success"},
	} {
		require.NoError(t, producer.SendMessage(ctx, topic, msg))
	}

	select {
	case err := <-startErr:
		require.ErrorIs(t, err, kafka.ErrDLQPublish)
	case <-ctx.Done():
		t.Fatal("timeout waiting for Start to return")
	}

	mu.Lock()
	assert.NotContains(t, handled, "3", "records after the failed one must not be handled")
	mu.Unlock()

	offsets, err := kadm.NewClient(sharedClient).FetchOffsets(ctx, group)
	require.NoError(t, err)
	committed, ok := offsets.Lookup(topic, 0)
	require.True(t, ok, "offset for the record before the failure should be committed")
	assert.Equal(t, int64(1), committed.At, "the record whose DLQ publish failed must not be committed")
}

func TestHealthChecker(t *testing.T) {
	ctx := context.Background()

	client := newClient(t, sharedBrokers, "test-health-group")
	checker := kafka.NewHealthChecker(client)

	err := checker.Check(ctx)
	assert.NoError(t, err, "health check failed")

	health, err := checker.CheckWithMetadata(ctx)
	assert.NoError(t, err)
	assert.True(t, health.Healthy, "cluster should be healthy")
	assert.Greater(t, health.BrokerCount, 0, "should have at least one broker")
	assert.GreaterOrEqual(t, health.ControllerID, int32(0), "controller ID should be valid")

	err = checker.LivenessCheck(ctx)
	assert.NoError(t, err, "liveness check failed")
}

func TestConfig(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := kafka.DefaultConfig([]string{"localhost:9092"}, "test-group")
		err := cfg.Validate()
		assert.NoError(t, err)
	})

	t.Run("missing brokers", func(t *testing.T) {
		cfg := kafka.DefaultConfig([]string{}, "test-group")
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "broker")
	})

	t.Run("invalid SASL config", func(t *testing.T) {
		cfg := kafka.DefaultConfig([]string{"localhost:9092"}, "test-group")
		cfg.SASLMechanism = "PLAIN"
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "SASL")
	})

	t.Run("unsupported SASL mechanism", func(t *testing.T) {
		cfg := kafka.DefaultConfig([]string{"localhost:9092"}, "test-group")
		cfg.SASLMechanism = "INVALID"
		cfg.SASLUsername = "user"
		cfg.SASLPassword = "pass"
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported SASL mechanism")
	})

	t.Run("config with TLS", func(t *testing.T) {
		cfg := kafka.DefaultConfig([]string{"localhost:9092"}, "test-group")
		cfg.WithTLS(nil)
		assert.True(t, cfg.TLSEnabled)
		assert.NotNil(t, cfg.TLSConfig)
	})

	t.Run("config with SASL PLAIN", func(t *testing.T) {
		cfg := kafka.DefaultConfig([]string{"localhost:9092"}, "test-group")
		cfg.WithSASLPlain("user", "pass").WithTLS(nil)
		assert.Equal(t, "PLAIN", cfg.SASLMechanism)
		assert.Equal(t, "user", cfg.SASLUsername)
		err := cfg.Validate()
		assert.NoError(t, err)
	})

	t.Run("config with SASL SCRAM", func(t *testing.T) {
		cfg := kafka.DefaultConfig([]string{"localhost:9092"}, "test-group")
		cfg.WithSASLScram("SCRAM-SHA-256", "user", "pass")
		assert.Equal(t, "SCRAM-SHA-256", cfg.SASLMechanism)
		err := cfg.Validate()
		assert.NoError(t, err)
	})
}
