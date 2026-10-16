package example

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	testContainer testcontainers.Container
	testURL       string
	testLogger    *slog.Logger
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	testLogger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:4.2-management-alpine",
		ExposedPorts: []string{"5672/tcp", "15672/tcp"},
		WaitingFor:   wait.ForLog("Server startup complete").WithStartupTimeout(180 * time.Second),
	}

	var err error
	testContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		testLogger.Error("failed to start RabbitMQ container", "error", err)
		os.Exit(1)
	}

	host, err := testContainer.Host(ctx)
	if err != nil {
		testLogger.Error("failed to get container host", "error", err)
		os.Exit(1)
	}

	port, err := testContainer.MappedPort(ctx, "5672")
	if err != nil {
		testLogger.Error("failed to get container port", "error", err)
		os.Exit(1)
	}

	testURL = "amqp://guest:guest@" + host + ":" + port.Port() + "/"

	code := m.Run()

	if err := testContainer.Terminate(ctx); err != nil {
		testLogger.Error("failed to terminate container", "error", err)
	}

	os.Exit(code)
}

// TestMessage represents a test message structure.
type TestMessage struct {
	ID        string    `json:"id"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

// newTestClient creates a Client with sensible test defaults and registers t.Cleanup.
func newTestClient(t *testing.T, overrides ...func(*rabbitmq.Config)) *rabbitmq.Client {
	t.Helper()

	config := &rabbitmq.Config{
		URL:                      testURL,
		ConnectionName:           t.Name(),
		PublisherConfirms:        true,
		ChannelPoolSize:          rabbitmq.DefaultChannelPoolSize,
		PrefetchCount:            rabbitmq.DefaultPrefetchCount,
		QueueType:                rabbitmq.QueueTypeClassic,
		Durable:                  true,
		AutoAck:                  false,
		RetryEnabled:             true,
		RetryTTL:                 rabbitmq.DefaultRetryTTLSec * time.Second,
		MaxRetries:               rabbitmq.DefaultMaxRetries,
		DeadLetterEnabled:        true,
		ReconnectInitialInterval: rabbitmq.DefaultReconnectInitialSec * time.Second,
		ReconnectMaxInterval:     5 * time.Second,
	}

	for _, fn := range overrides {
		fn(config)
	}

	client, err := rabbitmq.NewClient(config, testLogger)
	require.NoError(t, err, "failed to create client")

	t.Cleanup(func() { client.Close() })
	return client
}

// newTestQueue declares an isolated queue and returns its name.
func newTestQueue(t *testing.T, client *rabbitmq.Client, name string) string {
	t.Helper()
	tm := rabbitmq.NewTopologyManager(client)
	_, err := tm.DeclareQueue(rabbitmq.QueueConfig{
		Name:    name,
		Durable: true,
	})
	require.NoError(t, err, "failed to declare queue %s", name)
	return name
}

func TestNewClient(t *testing.T) {
	t.Run("creates client successfully", func(t *testing.T) {
		client := newTestClient(t)

		assert.NotNil(t, client)
		assert.True(t, client.IsHealthy())
		assert.Equal(t, rabbitmq.StateConnected, client.GetState())
	})

	t.Run("fails with empty URL", func(t *testing.T) {
		config := &rabbitmq.Config{URL: ""}
		_, err := rabbitmq.NewClient(config, testLogger)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "URL is required")
	})
}

func TestProducer_PublishMessage(t *testing.T) {
	t.Run("publishes and consumes message", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		client := newTestClient(t)
		queueName := newTestQueue(t, client, "test-publish-consume")

		producer := rabbitmq.NewProducer(client)
		consumer := rabbitmq.NewConsumer(client, testLogger)

		received := make(chan TestMessage, 1)

		go func() {
			consumerCtx, consumerCancel := context.WithTimeout(ctx, 20*time.Second)
			defer consumerCancel()

			handler := func(_ context.Context, msg any, _ amqp.Delivery) error {
				body, _ := json.Marshal(msg)
				var testMsg TestMessage
				if err := json.Unmarshal(body, &testMsg); err != nil {
					return err
				}
				select {
				case received <- testMsg:
				default:
				}
				return nil
			}

			if err := consumer.ConsumeWithDefaults(consumerCtx, queueName, handler); err != nil && err != context.Canceled {
				t.Logf("consumer error: %v", err)
			}
		}()

		time.Sleep(1 * time.Second) // let consumer attach

		testMsg := TestMessage{
			ID:        "msg-001",
			Content:   "Hello, RabbitMQ!",
			Timestamp: time.Now(),
		}

		err := producer.SendMessage(ctx, queueName, testMsg)
		require.NoError(t, err, "failed to send message")

		select {
		case got := <-received:
			assert.Equal(t, testMsg.ID, got.ID)
			assert.Equal(t, testMsg.Content, got.Content)
		case <-time.After(15 * time.Second):
			t.Fatal("timeout waiting for message")
		}
	})
}

func TestConsumer_ManualAck(t *testing.T) {
	t.Run("manual acknowledgment works correctly", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		client := newTestClient(t)
		queueName := newTestQueue(t, client, "test-manual-ack")

		producer := rabbitmq.NewProducer(client)
		testMsg := TestMessage{
			ID:        "ack-test",
			Content:   "Testing manual ack",
			Timestamp: time.Now(),
		}

		err := producer.SendMessage(ctx, queueName, testMsg)
		require.NoError(t, err)

		consumer := rabbitmq.NewConsumer(client, testLogger)
		handlerCalled := make(chan bool, 1)

		go func() {
			consumerCtx, consumerCancel := context.WithTimeout(ctx, 5*time.Second)
			defer consumerCancel()

			handler := func(_ context.Context, _ any, _ amqp.Delivery) error {
				select {
				case handlerCalled <- true:
				default:
				}
				return fmt.Errorf("simulated processing error")
			}

			if err := consumer.ConsumeWithDefaults(consumerCtx, queueName, handler); err != nil && err != context.Canceled {
				t.Logf("consumer error: %v", err)
			}
		}()

		select {
		case <-handlerCalled:
			// Handler was called and returned an error — message should be requeued
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for handler call")
		}
	})
}

func TestTopologyManager_SetupDeadLetterExchange(t *testing.T) {
	t.Run("sets up DLX with retry and parking lot", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		client := newTestClient(t)
		tm := rabbitmq.NewTopologyManager(client)

		mainQueue := "test-dlx-main"
		dlxName := "test-dlx"

		err := tm.SetupDeadLetterExchange(mainQueue, dlxName, 5000)
		require.NoError(t, err)

		_, err = tm.SetupQueueWithDLX(mainQueue, dlxName)
		require.NoError(t, err)

		producer := rabbitmq.NewProducer(client)
		testMsg := TestMessage{
			ID:        "dlx-test",
			Content:   "Testing DLX",
			Timestamp: time.Now(),
		}

		err = producer.SendMessage(ctx, mainQueue, testMsg)
		require.NoError(t, err)
	})
}

func TestClient_Reconnection(t *testing.T) {
	t.Run("reconnects after connection loss", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		client := newTestClient(t)
		queueName := newTestQueue(t, client, "test-reconnect")

		assert.True(t, client.IsHealthy())

		// Simulate connection loss by stopping/starting the container
		timeout := 5 * time.Second
		require.NoError(t, testContainer.Stop(ctx, &timeout), "failed to stop container")
		require.NoError(t, testContainer.Start(ctx), "failed to start container")

		// Wait for reconnection
		var reconnected bool
		for range 60 {
			time.Sleep(500 * time.Millisecond)
			if client.IsHealthy() {
				reconnected = true
				break
			}
		}
		assert.True(t, reconnected, "client should reconnect after connection loss")

		// Re-declare queue after reconnect
		newTestQueue(t, client, queueName)

		// Verify publish works after reconnection
		producer := rabbitmq.NewProducer(client)
		testMsg := TestMessage{
			ID:        "reconnect-test",
			Content:   "After reconnection",
			Timestamp: time.Now(),
		}

		err := producer.SendMessage(ctx, queueName, testMsg)
		require.NoError(t, err)
	})
}

func TestBatchConsumer(t *testing.T) {
	t.Run("processes messages in batches", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		client := newTestClient(t)
		queueName := newTestQueue(t, client, "test-batch")

		producer := rabbitmq.NewProducer(client)

		// Send 5 messages
		for i := range 5 {
			err := producer.SendMessage(ctx, queueName, TestMessage{
				ID:        fmt.Sprintf("batch-%d", i),
				Content:   "Batch test",
				Timestamp: time.Now(),
			})
			require.NoError(t, err)
		}

		batchReceived := make(chan int, 1)
		bc := rabbitmq.NewBatchConsumer(client, testLogger, 5, 10*time.Second)

		go func() {
			consumerCtx, consumerCancel := context.WithTimeout(ctx, 15*time.Second)
			defer consumerCancel()

			handler := func(_ context.Context, messages []any, _ []amqp.Delivery) error {
				select {
				case batchReceived <- len(messages):
				default:
				}
				return nil
			}

			if err := bc.StartBatchConsumer(consumerCtx, rabbitmq.ConsumeOptions{QueueName: queueName}, handler); err != nil && err != context.Canceled {
				t.Logf("batch consumer error: %v", err)
			}
		}()

		select {
		case count := <-batchReceived:
			assert.Equal(t, 5, count)
		case <-time.After(15 * time.Second):
			t.Fatal("timeout waiting for batch")
		}
	})
}

func TestConsumer_AutoReconnect(t *testing.T) {
	t.Run("consumer restarts after delivery channel close", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		client := newTestClient(t)
		queueName := newTestQueue(t, client, "test-auto-reconnect")

		consumer := rabbitmq.NewConsumer(client, testLogger)
		received := make(chan string, 10)

		go func() {
			handler := func(_ context.Context, msg any, _ amqp.Delivery) error {
				body, _ := json.Marshal(msg)
				var testMsg TestMessage
				if err := json.Unmarshal(body, &testMsg); err != nil {
					return err
				}
				received <- testMsg.ID
				return nil
			}

			if err := consumer.StartConsumer(ctx, rabbitmq.ConsumeOptions{
				QueueName:     queueName,
				AutoReconnect: true,
			}, handler); err != nil && err != context.Canceled {
				t.Logf("consumer error: %v", err)
			}
		}()

		time.Sleep(1 * time.Second)

		// Send first message
		producer := rabbitmq.NewProducer(client)
		err := producer.SendMessage(ctx, queueName, TestMessage{ID: "before-drop", Content: "Pre-disconnect"})
		require.NoError(t, err)

		select {
		case id := <-received:
			assert.Equal(t, "before-drop", id)
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for first message")
		}

		// Simulate connection loss by stopping/starting the container
		timeout := 5 * time.Second
		require.NoError(t, testContainer.Stop(ctx, &timeout), "failed to stop container")
		require.NoError(t, testContainer.Start(ctx), "failed to start container")

		// Wait for reconnection
		for range 60 {
			time.Sleep(500 * time.Millisecond)
			if client.IsHealthy() {
				break
			}
		}
		require.True(t, client.IsHealthy(), "client should reconnect")

		// Re-declare queue after reconnect
		newTestQueue(t, client, queueName)

		time.Sleep(2 * time.Second) // let consumer re-attach

		// Send second message
		err = producer.SendMessage(ctx, queueName, TestMessage{ID: "after-reconnect", Content: "Post-disconnect"})
		require.NoError(t, err)

		select {
		case id := <-received:
			assert.Equal(t, "after-reconnect", id)
		case <-time.After(15 * time.Second):
			t.Fatal("timeout waiting for message after reconnection")
		}
	})
}

func TestConsumer_MaxRetriesMovesToParkingLot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client := newTestClient(t, func(c *rabbitmq.Config) {
		c.MaxRetries = 2
	})
	tm := rabbitmq.NewTopologyManager(client)

	mainQueue := "test-parking-main"
	dlxName := "test-parking-dlx"
	require.NoError(t, tm.SetupDeadLetterExchange(mainQueue, dlxName, 200))
	_, err := tm.SetupQueueWithDLX(mainQueue, dlxName)
	require.NoError(t, err)

	producer := rabbitmq.NewProducer(client)
	require.NoError(t, producer.SendMessage(ctx, mainQueue, TestMessage{ID: "poison"}))

	// The handler always fails: the message must go main -> retry (TTL) ->
	// main twice, then land in the parking lot instead of looping forever.
	var attempts atomic.Int32
	consumerCtx, stopConsumer := context.WithCancel(ctx)
	defer stopConsumer()
	go func() {
		_ = rabbitmq.NewConsumer(client, testLogger).ConsumeWithDefaults(consumerCtx, mainQueue,
			func(context.Context, any, amqp.Delivery) error {
				attempts.Add(1)
				return fmt.Errorf("always fails")
			})
	}()

	ch, err := client.CreateConsumerChannel()
	require.NoError(t, err)
	defer ch.Close()

	parkingLot := mainQueue + ".parking-lot"
	require.Eventually(t, func() bool {
		q, err := ch.QueueDeclarePassive(parkingLot, true, false, false, false, nil)
		return err == nil && q.Messages == 1
	}, 30*time.Second, 200*time.Millisecond, "message must reach the parking lot")

	assert.Equal(t, int32(3), attempts.Load(), "1 initial attempt + 2 retries")
}

func TestPublisherPool_SurvivesChannelErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := newTestClient(t, func(c *rabbitmq.Config) {
		c.ChannelPoolSize = 2
	})
	queueName := newTestQueue(t, client, "test-pool-survives")
	producer := rabbitmq.NewProducer(client)

	// Publishing to a missing exchange closes the channel with a 404. Do it
	// more times than the pool has channels; the pool must replace them.
	for range 5 {
		err := producer.SendToTopic(ctx, "exchange-that-does-not-exist", "rk", TestMessage{ID: "x"})
		require.Error(t, err)
	}

	publishCtx, publishCancel := context.WithTimeout(ctx, 5*time.Second)
	defer publishCancel()
	require.NoError(t, producer.SendMessage(publishCtx, queueName, TestMessage{ID: "after-errors"}))
}

func TestPublishBatch_ConfirmsAll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := newTestClient(t)
	queueName := newTestQueue(t, client, "test-publish-batch")

	payloads := make([]any, 50)
	for i := range payloads {
		payloads[i] = TestMessage{ID: fmt.Sprintf("batch-%d", i)}
	}
	require.NoError(t, rabbitmq.NewProducer(client).PublishBatch(ctx, rabbitmq.PublishOptions{RoutingKey: queueName}, payloads))

	ch, err := client.CreateConsumerChannel()
	require.NoError(t, err)
	defer ch.Close()
	q, err := ch.QueueDeclarePassive(queueName, true, false, false, false, nil)
	require.NoError(t, err)
	assert.Equal(t, 50, q.Messages)
}

func TestClient_CloseTwice(t *testing.T) {
	client, err := rabbitmq.NewClient(&rabbitmq.Config{
		URL:                      testURL,
		ConnectionName:           t.Name(),
		ChannelPoolSize:          1,
		PrefetchCount:            1,
		QueueType:                rabbitmq.QueueTypeClassic,
		RetryTTL:                 time.Second,
		MaxRetries:               1,
		ReconnectInitialInterval: time.Second,
		ReconnectMaxInterval:     time.Second,
	}, testLogger)
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		require.NoError(t, client.Close())
		require.NoError(t, client.Close())
	})
	assert.False(t, client.IsHealthy())
}
