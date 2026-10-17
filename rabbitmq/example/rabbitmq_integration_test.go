//go:build integration

package example

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
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
	testURL       string // direct URL of the broker
	testAddr      string // host:port behind testURL, the target of outage proxies
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

	testAddr = net.JoinHostPort(host, port.Port())
	testURL = "amqp://guest:guest@" + testAddr + "/"

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
		AutoAck:                  false,
		RetryEnabled:             true,
		RetryBackoff:             10 * time.Millisecond,
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

	t.Cleanup(func() { _ = client.Close() })
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

// tcpProxy forwards TCP connections to a target. Stop severs every open
// connection and refuses new ones until Start, while the target keeps running.
//
// Outage tests use it instead of stopping and restarting the container, which
// can change the container's mapped host port and break the reconnecting client.
type tcpProxy struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	conns map[net.Conn]struct{}
	down  bool
}

func newTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := &tcpProxy{ln: ln, target: target, conns: make(map[net.Conn]struct{})}
	go p.serve()
	t.Cleanup(func() {
		_ = ln.Close()
		p.Stop()
	})
	return p
}

// URL returns an AMQP URL that reaches the broker through the proxy.
func (p *tcpProxy) URL() string {
	return "amqp://guest:guest@" + p.ln.Addr().String() + "/"
}

// Stop simulates a broker outage: open connections are closed and new ones are
// accepted and closed immediately.
func (p *tcpProxy) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = true
	for c := range p.conns {
		_ = c.Close()
		delete(p.conns, c)
	}
}

// Start ends the outage.
func (p *tcpProxy) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = false
}

func (p *tcpProxy) serve() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.forward(client)
	}
}

func (p *tcpProxy) forward(client net.Conn) {
	upstream, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(context.Background(), "tcp", p.target)
	if err != nil {
		_ = client.Close()
		return
	}

	p.mu.Lock()
	if p.down {
		p.mu.Unlock()
		_ = client.Close()
		_ = upstream.Close()
		return
	}
	p.conns[client] = struct{}{}
	p.conns[upstream] = struct{}{}
	p.mu.Unlock()

	closeBoth := func() {
		_ = client.Close()
		_ = upstream.Close()
	}
	go func() {
		_, _ = io.Copy(upstream, client)
		closeBoth()
	}()
	_, _ = io.Copy(client, upstream)
	closeBoth()
}

// viaProxy points a client at the broker through p.
func viaProxy(p *tcpProxy) func(*rabbitmq.Config) {
	return func(c *rabbitmq.Config) { c.URL = p.URL() }
}

// publishRaw publishes body unchanged (no JSON encoding) to queue.
func publishRaw(t *testing.T, client *rabbitmq.Client, queue, contentType string, body []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := client.GetPublisherChannel(ctx)
	require.NoError(t, err)
	defer client.ReturnPublisherChannel(ch)

	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, false, false, amqp.Publishing{
		ContentType:  contentType,
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
	require.NoError(t, err)
	acked, err := dc.WaitContext(ctx)
	require.NoError(t, err)
	require.True(t, acked)
}

// queueMessages returns the number of ready messages in queue.
func queueMessages(t *testing.T, client *rabbitmq.Client, queue string) int {
	t.Helper()
	ch, err := client.CreateConsumerChannel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	require.NoError(t, err)
	return q.Messages
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

			handler := rabbitmq.JSONHandler(func(_ context.Context, testMsg TestMessage, _ amqp.Delivery) error {
				select {
				case received <- testMsg:
				default:
				}
				return nil
			})

			if err := consumer.ConsumeWithDefaults(consumerCtx, queueName, handler); err != nil && !errors.Is(err, context.Canceled) {
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

			handler := func(_ context.Context, _ amqp.Delivery) error {
				select {
				case handlerCalled <- true:
				default:
				}
				return errors.New("simulated processing error")
			}

			if err := consumer.ConsumeWithDefaults(consumerCtx, queueName, handler); err != nil && !errors.Is(err, context.Canceled) {
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

		proxy := newTCPProxy(t, testAddr)
		client := newTestClient(t, viaProxy(proxy))
		queueName := newTestQueue(t, client, "test-reconnect")

		assert.True(t, client.IsHealthy())

		// Simulate connection loss: sever every connection, refuse new ones
		proxy.Stop()
		require.Eventually(t, func() bool { return !client.IsHealthy() }, 10*time.Second, 50*time.Millisecond,
			"client should notice the connection loss")
		proxy.Start()

		require.Eventually(t, client.IsHealthy, 60*time.Second, 100*time.Millisecond,
			"client should reconnect after connection loss")

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

// The cached topology must be back on the broker by the time the client
// reports healthy: consumers that wait on IsHealthy re-subscribe immediately.
func TestClient_TopologyRestoredBeforeHealthy(t *testing.T) {
	const queueName = "test-restore-before-healthy"

	proxy := newTCPProxy(t, testAddr)
	client := newTestClient(t, viaProxy(proxy))
	newTestQueue(t, client, queueName)

	proxy.Stop()
	require.Eventually(t, func() bool { return !client.IsHealthy() }, 10*time.Second, 50*time.Millisecond)

	// While the client is away the queue disappears, as on a broker that lost its state.
	admin, err := amqp.Dial(testURL)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()
	adminCh, err := admin.Channel()
	require.NoError(t, err)
	_, err = adminCh.QueueDelete(queueName, false, false, false)
	require.NoError(t, err)

	proxy.Start()

	// Poll tightly so the check runs the moment the client turns healthy.
	deadline := time.Now().Add(60 * time.Second)
	for !client.IsHealthy() {
		require.True(t, time.Now().Before(deadline), "client should reconnect")
		time.Sleep(time.Millisecond)
	}

	ch, err := client.CreateConsumerChannel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()
	_, err = ch.QueueDeclarePassive(queueName, true, false, false, false, nil)
	require.NoError(t, err, "queue must be re-declared before the client reports healthy")
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

			handler := func(_ context.Context, deliveries []amqp.Delivery) error {
				select {
				case batchReceived <- len(deliveries):
				default:
				}
				return nil
			}

			if err := bc.StartBatchConsumer(consumerCtx, rabbitmq.ConsumeOptions{QueueName: queueName}, handler); err != nil && !errors.Is(err, context.Canceled) {
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

		proxy := newTCPProxy(t, testAddr)
		client := newTestClient(t, viaProxy(proxy))
		queueName := newTestQueue(t, client, "test-auto-reconnect")

		consumer := rabbitmq.NewConsumer(client, testLogger)
		received := make(chan string, 10)

		go func() {
			handler := rabbitmq.JSONHandler(func(_ context.Context, testMsg TestMessage, _ amqp.Delivery) error {
				received <- testMsg.ID
				return nil
			})

			if err := consumer.StartConsumer(ctx, rabbitmq.ConsumeOptions{
				QueueName:     queueName,
				AutoReconnect: true,
			}, handler); err != nil && !errors.Is(err, context.Canceled) {
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

		// Simulate connection loss: sever every connection, refuse new ones
		proxy.Stop()
		require.Eventually(t, func() bool { return !client.IsHealthy() }, 10*time.Second, 50*time.Millisecond)
		proxy.Start()

		// The cached topology is restored on reconnect, so the queue is back
		// before the consumer re-attaches.
		require.Eventually(t, client.IsHealthy, 60*time.Second, 100*time.Millisecond, "client should reconnect")

		time.Sleep(2 * time.Second) // let consumer re-attach

		// Send second message
		err = producer.SendMessage(ctx, queueName, TestMessage{ID: "after-reconnect", Content: "Post-disconnect"})
		require.NoError(t, err)

		// Delivery is at-least-once: the first message's ack may have been lost
		// with the connection we cut, in which case the broker redelivers it.
		timeout := time.After(15 * time.Second)
		for {
			select {
			case id := <-received:
				if id == "before-drop" {
					continue
				}
				assert.Equal(t, "after-reconnect", id)
				return
			case <-timeout:
				t.Fatal("timeout waiting for message after reconnection")
			}
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
			func(context.Context, amqp.Delivery) error {
				attempts.Add(1)
				return errors.New("always fails")
			})
	}()

	ch, err := client.CreateConsumerChannel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()

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
	defer func() { _ = ch.Close() }()
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

// With RetryEnabled=false a failing handler used to requeue the message with
// nothing counting the attempts on a classic queue. The client now counts them
// itself and parks the message after MaxRetries.
func TestConsumer_ClassicQueueWithoutRetryEnabledIsBounded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client := newTestClient(t, func(c *rabbitmq.Config) {
		c.QueueType = rabbitmq.QueueTypeClassic
		c.RetryEnabled = false
		c.MaxRetries = 2
		c.DeadLetterEnabled = true
	})
	queueName := newTestQueue(t, client, "test-classic-bounded")
	parkingLot := newTestQueue(t, client, queueName+".parking-lot")

	require.NoError(t, rabbitmq.NewProducer(client).SendMessage(ctx, queueName, TestMessage{ID: "loop"}))

	var attempts atomic.Int32
	consumerCtx, stopConsumer := context.WithCancel(ctx)
	defer stopConsumer()
	go func() {
		_ = rabbitmq.NewConsumer(client, testLogger).ConsumeWithDefaults(consumerCtx, queueName,
			func(context.Context, amqp.Delivery) error {
				attempts.Add(1)
				return errors.New("always fails")
			})
	}()

	require.Eventually(t, func() bool { return queueMessages(t, client, parkingLot) == 1 },
		30*time.Second, 200*time.Millisecond, "message must reach the parking lot")

	time.Sleep(time.Second) // a loop would keep delivering
	assert.Equal(t, int32(3), attempts.Load(), "1 initial attempt + MaxRetries retries, then no more")
	assert.Zero(t, queueMessages(t, client, queueName), "the main queue must be drained")
}

// A body the typed handler cannot decode can never succeed: it is parked at
// once instead of being retried.
func TestConsumer_PoisonMessageIsParkedWithoutRetries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client := newTestClient(t)
	queueName := newTestQueue(t, client, "test-poison")
	parkingLot := newTestQueue(t, client, queueName+".parking-lot")

	publishRaw(t, client, queueName, "text/plain", []byte("this is not json"))

	var handled atomic.Int32
	consumerCtx, stopConsumer := context.WithCancel(ctx)
	defer stopConsumer()
	go func() {
		_ = rabbitmq.NewConsumer(client, testLogger).ConsumeWithDefaults(consumerCtx, queueName,
			rabbitmq.JSONHandler(func(context.Context, TestMessage, amqp.Delivery) error {
				handled.Add(1)
				return nil
			}))
	}()

	require.Eventually(t, func() bool { return queueMessages(t, client, parkingLot) == 1 },
		30*time.Second, 200*time.Millisecond, "poison message must be parked")
	assert.Zero(t, handled.Load(), "the typed handler must not see an undecodable body")
	assert.Zero(t, queueMessages(t, client, queueName))
}

// The raw handler receives any payload; nothing assumes JSON.
func TestConsumer_RawHandlerReceivesNonJSONPayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := newTestClient(t)
	queueName := newTestQueue(t, client, "test-raw-payload")

	payload := []byte{0x00, 0x01, 0xfe, 0xff}
	publishRaw(t, client, queueName, "application/octet-stream", payload)

	type got struct {
		body        []byte
		contentType string
	}
	received := make(chan got, 1)
	go func() {
		_ = rabbitmq.NewConsumer(client, testLogger).ConsumeWithDefaults(ctx, queueName,
			func(_ context.Context, d amqp.Delivery) error {
				received <- got{body: d.Body, contentType: d.ContentType}
				return nil
			})
	}()

	select {
	case r := <-received:
		assert.Equal(t, payload, r.body)
		assert.Equal(t, "application/octet-stream", r.contentType)
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for raw message")
	}
}

// A validator keeps a malformed message out of the batch it would sit in: the
// good messages are handled together and the bad one is parked on its own.
func TestBatchConsumer_ValidatorParksInvalidMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client := newTestClient(t)
	queueName := newTestQueue(t, client, "test-batch-validator")
	parkingLot := newTestQueue(t, client, queueName+".parking-lot")

	producer := rabbitmq.NewProducer(client)
	require.NoError(t, producer.SendMessage(ctx, queueName, TestMessage{ID: "good-1"}))
	publishRaw(t, client, queueName, "text/plain", []byte("not json"))
	require.NoError(t, producer.SendMessage(ctx, queueName, TestMessage{ID: "good-2"}))

	ids := make(chan []string, 1)
	bc := rabbitmq.NewBatchConsumer(client, testLogger, 2, 10*time.Second, rabbitmq.WithBatchValidator(rabbitmq.ValidJSON))
	go func() {
		_ = bc.StartBatchConsumer(ctx, rabbitmq.ConsumeOptions{QueueName: queueName},
			rabbitmq.JSONBatchHandler(func(_ context.Context, msgs []TestMessage, _ []amqp.Delivery) error {
				batch := make([]string, len(msgs))
				for i, m := range msgs {
					batch[i] = m.ID
				}
				ids <- batch
				return nil
			}))
	}()

	select {
	case batch := <-ids:
		assert.Equal(t, []string{"good-1", "good-2"}, batch)
	case <-time.After(20 * time.Second):
		t.Fatal("timeout waiting for batch")
	}
	require.Eventually(t, func() bool { return queueMessages(t, client, parkingLot) == 1 },
		15*time.Second, 200*time.Millisecond, "invalid message must be parked")
}

// Without RetryEnabled the client waits between counted retries, doubling the
// delay each time, instead of burning every retry within milliseconds.
func TestConsumer_RetryBackoffSpacesAttempts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const backoff = 200 * time.Millisecond
	client := newTestClient(t, func(c *rabbitmq.Config) {
		c.RetryEnabled = false
		c.MaxRetries = 2
		c.RetryBackoff = backoff
		c.DeadLetterEnabled = true
	})
	queueName := newTestQueue(t, client, "test-retry-backoff")
	parkingLot := newTestQueue(t, client, queueName+".parking-lot")
	require.NoError(t, rabbitmq.NewProducer(client).SendMessage(ctx, queueName, TestMessage{ID: "slow"}))

	var mu sync.Mutex
	var times []time.Time
	consumerCtx, stopConsumer := context.WithCancel(ctx)
	defer stopConsumer()
	go func() {
		_ = rabbitmq.NewConsumer(client, testLogger).ConsumeWithDefaults(consumerCtx, queueName,
			func(context.Context, amqp.Delivery) error {
				mu.Lock()
				times = append(times, time.Now())
				mu.Unlock()
				return errors.New("always fails")
			})
	}()

	require.Eventually(t, func() bool { return queueMessages(t, client, parkingLot) == 1 },
		30*time.Second, 100*time.Millisecond, "message must reach the parking lot")

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, times, 3, "1 initial attempt + MaxRetries retries")
	assert.GreaterOrEqual(t, times[1].Sub(times[0]), backoff, "first retry waits the base backoff")
	assert.GreaterOrEqual(t, times[2].Sub(times[1]), 2*backoff, "second retry waits twice as long")
}

// A per-message TTL must survive a counted retry; the copy used to lose it and
// would then never expire.
func TestConsumer_RetryKeepsExpiration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client := newTestClient(t, func(c *rabbitmq.Config) {
		c.RetryEnabled = false
		c.MaxRetries = 2
	})
	queueName := newTestQueue(t, client, "test-retry-expiration")
	require.NoError(t, rabbitmq.NewProducer(client).PublishMessage(ctx,
		rabbitmq.PublishOptions{RoutingKey: queueName, Expiration: "600000"}, TestMessage{ID: "ttl"}))

	type seen struct {
		expiration string
		retries    any
	}
	deliveries := make(chan seen, 4)
	var attempts atomic.Int32
	consumerCtx, stopConsumer := context.WithCancel(ctx)
	defer stopConsumer()
	go func() {
		_ = rabbitmq.NewConsumer(client, testLogger).ConsumeWithDefaults(consumerCtx, queueName,
			func(_ context.Context, d amqp.Delivery) error {
				deliveries <- seen{expiration: d.Expiration, retries: d.Headers["x-retry-count"]}
				if attempts.Add(1) == 1 {
					return errors.New("fail once")
				}
				return nil
			})
	}()

	first, second := <-deliveries, <-deliveries
	assert.Equal(t, "600000", first.expiration)
	assert.Equal(t, "600000", second.expiration, "the retried copy must keep its TTL")
	assert.Equal(t, int64(1), second.retries, "the retried copy carries the attempt count")
}

// A cached declaration the broker rejects on reconnect must not keep the whole
// client unhealthy, and the entries after it must still be restored.
func TestClient_RejectedTopologyEntryDoesNotWedgeReconnect(t *testing.T) {
	const conflicting, restored = "test-restore-conflict", "test-restore-after-conflict"

	proxy := newTCPProxy(t, testAddr)
	client := newTestClient(t, viaProxy(proxy))
	newTestQueue(t, client, conflicting)
	newTestQueue(t, client, restored)

	proxy.Stop()
	require.Eventually(t, func() bool { return !client.IsHealthy() }, 10*time.Second, 50*time.Millisecond)

	// While the client is away both queues disappear, and the first comes back
	// with arguments that clash with the cached declaration (PRECONDITION_FAILED).
	admin, err := amqp.Dial(testURL)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()
	adminCh, err := admin.Channel()
	require.NoError(t, err)
	for _, q := range []string{conflicting, restored} {
		_, err = adminCh.QueueDelete(q, false, false, false)
		require.NoError(t, err)
	}
	_, err = adminCh.QueueDeclare(conflicting, true, false, false, false, amqp.Table{"x-max-length": int64(10)})
	require.NoError(t, err)

	proxy.Start()

	require.Eventually(t, client.IsHealthy, 60*time.Second, 100*time.Millisecond,
		"a rejected entry must not keep the client unhealthy")
	ch, err := client.CreateConsumerChannel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()
	_, err = ch.QueueDeclarePassive(restored, true, false, false, false, nil)
	assert.NoError(t, err, "entries after the rejected one must still be restored")
}
