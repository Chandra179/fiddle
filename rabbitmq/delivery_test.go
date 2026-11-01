package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestSettlementPreservesOriginalOnFailure(t *testing.T) {
	boom := errors.New("failed to read cards")
	for _, tc := range []struct {
		name            string
		failure         processingResult
		transferErr     error
		publishes, acks int
	}{
		{"success", processingResult{}, nil, 0, 1},
		{"unknown", processingResult{err: boom, kind: UnknownFailure, attempts: 3}, nil, 0, 0},
		{"outage", processingResult{err: boom, kind: DependencyUnavailable, attempts: 1}, nil, 0, 0},
		{"parked", processingResult{err: boom, kind: PermanentFailure, attempts: 1, park: true}, nil, 1, 1},
		{"unroutable", processingResult{err: boom, kind: PermanentFailure, attempts: 1, park: true}, ErrUnroutable, 1, 0},
		{"unconfirmed", processingResult{err: boom, kind: PermanentFailure, attempts: 1, park: true}, ErrTransferUnconfirmed, 1, 0},
		{"timeout", processingResult{err: boom, kind: PermanentFailure, attempts: 1, park: true}, context.DeadlineExceeded, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			acker := &fakeAcker{}
			publishes := 0
			err := settleProcessed(newDelivery(acker, nil), tc.failure, func() error {
				if len(acker.calls) != 0 {
					t.Fatal("acknowledged before transfer")
				}
				publishes++
				return tc.transferErr
			})
			if publishes != tc.publishes || len(acker.calls) != tc.acks {
				t.Fatalf("publishes=%d calls=%v err=%v", publishes, acker.calls, err)
			}
			for _, call := range acker.calls {
				if call.op != "ack" || call.multiple {
					t.Fatalf("unsafe settlement: %+v", call)
				}
			}
			if tc.acks == 0 && err == nil {
				t.Fatal("unsettled failure must stop consumption")
			}
		})
	}
}

func TestTransferReturnWinsOverAcknowledgement(t *testing.T) {
	// Both channels are ready: select may choose ack first. Repeat to exercise
	// both branches; an ack never masks RabbitMQ's preceding basic.return.
	for range 200 {
		returned := make(chan amqp.Return, 1)
		confirmed := make(chan amqp.Confirmation, 1)
		returned <- amqp.Return{ReplyCode: 312, ReplyText: "NO_ROUTE"}
		confirmed <- amqp.Confirmation{DeliveryTag: 1, Ack: true}
		if err := awaitTransfer(context.Background(), returned, confirmed); !errors.Is(err, ErrUnroutable) {
			t.Fatalf("unroutable publication accepted: %v", err)
		}
	}
}

func TestTransferConfirmationFailures(t *testing.T) {
	for _, tc := range []struct {
		name         string
		confirmation amqp.Confirmation
		wantErr      bool
	}{
		{"ack", amqp.Confirmation{DeliveryTag: 1, Ack: true}, false},
		{"nack", amqp.Confirmation{DeliveryTag: 1, Ack: false}, true},
		{"unexpected-tag", amqp.Confirmation{DeliveryTag: 2, Ack: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			confirmed := make(chan amqp.Confirmation, 1)
			confirmed <- tc.confirmation
			err := awaitTransfer(context.Background(), make(chan amqp.Return, 1), confirmed)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v", err)
			}
		})
	}
	closed := make(chan amqp.Confirmation)
	close(closed)
	if !errors.Is(awaitTransfer(context.Background(), make(chan amqp.Return), closed), ErrTransferUnconfirmed) {
		t.Fatal("closed confirmation stream accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(awaitTransfer(ctx, make(chan amqp.Return), make(chan amqp.Confirmation)), context.Canceled) {
		t.Fatal("cancelled transfer did not stop")
	}
}

func TestDeadLetterQueueConfiguration(t *testing.T) {
	d := (&DeadLetterConfig{}).normalized("cards")
	if d.Queue.Name != "cards.parking-lot" || !d.Queue.Durable || d.validate("cards") != nil {
		t.Fatalf("invalid defaults: %+v", d)
	}
	d.Queue.Name = "cards"
	if d.validate("cards") == nil {
		t.Fatal("source and dead-letter queue must differ")
	}
	d = (&DeadLetterConfig{Queue: QueueConfig{Name: "cards.failed", Durable: true, Args: map[string]any{"x-max-length-bytes": int64(1024)}}}).normalized("cards")
	if d.validate("cards") == nil {
		t.Fatal("implicit overflow policy accepted")
	}
	d.Queue.Args["x-overflow"] = "reject-publish"
	if d.validate("cards") != nil {
		t.Fatal("explicit safe overflow rejected")
	}
	d.Provision = ProvisionMode(99)
	if d.validate("cards") == nil {
		t.Fatal("invalid provisioning mode accepted")
	}
}

func TestBatchFailureNeverAcknowledgesLaterMessages(t *testing.T) {
	first, second := &fakeAcker{}, &fakeAcker{}
	bc := NewBatchConsumer(nil, nil, 2, time.Second)
	bc.policy = quickPolicy(DependencyUnavailable)
	b := &batcher{bc: bc, queue: "cards", pending: []amqp.Delivery{newDelivery(first, nil), newDelivery(second, nil)}, handler: func(context.Context, []amqp.Delivery) error { return errors.New("service down") }}
	if !errors.Is(b.flush(context.Background()), ErrConsumerPaused) {
		t.Fatal("batch failure did not stop consumption")
	}
	if len(first.calls)+len(second.calls) != 0 {
		t.Fatal("failed batch acknowledged input")
	}
	bc.policy = quickPolicy(UnknownFailure)
	b.handler = func(context.Context, []amqp.Delivery) error { return nil }
	if err := b.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, a := range []*fakeAcker{first, second} {
		if len(a.calls) != 1 || a.calls[0].multiple {
			t.Fatalf("unsafe batch ack: %+v", a.calls)
		}
	}
}

func TestValidatorFailureWithoutDLQStopsBatch(t *testing.T) {
	bc := NewBatchConsumer(nil, nil, 10, time.Second, WithBatchValidator(ValidJSON))
	b := &batcher{bc: bc, queue: "cards"}
	a := &fakeAcker{}
	d := newDelivery(a, nil)
	d.Body = []byte("broken json")
	b.add(context.Background(), d)
	if !errors.Is(b.err, ErrConsumerPaused) || len(a.calls) != 0 {
		t.Fatalf("invalid input lost: err=%v calls=%v", b.err, a.calls)
	}
}

func TestLegacyBrokerRetryLoopRejected(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.URL = "amqp://localhost"
	cfg.RetryEnabled = true
	if cfg.Validate() == nil {
		t.Fatal("broker retry loop accepted")
	}
}

func TestReconnectWaitStopsWhenClientCloses(t *testing.T) {
	closed := make(chan struct{})
	close(closed)
	c := &Consumer{client: &Client{closeChan: closed}}
	if err := c.waitForReconnect(context.Background()); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("closed client did not stop reconnect wait: %v", err)
	}
}
