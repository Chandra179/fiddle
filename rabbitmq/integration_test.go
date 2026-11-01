package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Optional broker test; no Docker is required. Point FIDDLE_RABBITMQ_URL at a
// disposable broker/vhost whose user may declare and delete test queues.
func TestIntegrationMissingParkingQueuePreservesOriginal(t *testing.T) {
	url := os.Getenv("FIDDLE_RABBITMQ_URL")
	if url == "" {
		t.Skip("set FIDDLE_RABBITMQ_URL for broker verification")
	}
	cfg := NewDefaultConfig()
	cfg.URL = url
	cfg.TransferTimeout = 3 * time.Second
	// Internal transfers must be reliable even with public confirms disabled.
	cfg.PublisherConfirms = false
	client, err := NewClient(cfg, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	admin, err := client.CreateConsumerChannel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close() }()
	source := fmt.Sprintf("fiddle-test-%d", time.Now().UnixNano())
	failed := source + ".failed"
	if _, err = admin.QueueDeclare(source, false, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = admin.QueueDelete(source, false, false, false)
		_, _ = admin.QueueDelete(failed, false, false, false)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err = admin.Confirm(false); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"card":"test"}`)
	dc, err := admin.PublishWithDeferredConfirmWithContext(ctx, "", source, true, false, amqp.Publishing{Body: body, DeliveryMode: amqp.Persistent})
	if err != nil {
		t.Fatal(err)
	}
	if err = waitConfirm(ctx, dc); err != nil {
		t.Fatal(err)
	}
	calls := 0
	err = NewConsumer(client, discardLogger()).StartConsumer(ctx, ConsumeOptions{
		QueueName:  source,
		DeadLetter: &DeadLetterConfig{Queue: QueueConfig{Name: failed, Durable: true}, QueueType: QueueTypeClassic, Provision: Declare},
	}, func(context.Context, amqp.Delivery) error {
		calls++
		if _, deleteErr := admin.QueueDelete(failed, false, false, false); deleteErr != nil {
			return deleteErr
		}
		return ErrPoisonMessage
	})
	if !errors.Is(err, ErrUnroutable) || calls != 1 {
		t.Fatalf("err=%v handler_calls=%d", err, calls)
	}
	delivery, ok, err := admin.Get(source, false)
	if err != nil || !ok {
		t.Fatalf("original was lost: present=%v err=%v", ok, err)
	}
	if string(delivery.Body) != string(body) {
		t.Fatalf("original payload changed: %q", delivery.Body)
	}
	if err = delivery.Ack(false); err != nil {
		t.Fatal(err)
	}
}
