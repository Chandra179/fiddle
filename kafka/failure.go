package kafka

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

// FailureKind describes an application processing failure, not a broker failure.
type FailureKind uint8

const (
	UnknownFailure FailureKind = iota
	RetryableFailure
	PermanentFailure
	DependencyUnavailable
	ConfigurationFailure
)

func (k FailureKind) String() string {
	switch k {
	case RetryableFailure:
		return "retryable"
	case PermanentFailure:
		return "permanent"
	case DependencyUnavailable:
		return "dependency_unavailable"
	case ConfigurationFailure:
		return "configuration"
	default:
		return "unknown"
	}
}

// ExhaustedAction applies to explicitly retryable failures only. Unknown
// failures always stop consumption; a DLQ is never a reason to discard them.
type ExhaustedAction uint8

const (
	StopConsumer ExhaustedAction = iota
	DeadLetter
)

// RetryPolicy is optional. A nil policy allows three retries, with jittered
// exponential backoff and a ten-second total budget, then stops consumption.
// A non-nil policy's MaxRetries is literal: zero disables retries.
// Handlers and classifier callbacks must respect context deadlines / not block.
type RetryPolicy struct {
	Classify       func(error) FailureKind
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	MaxElapsedTime time.Duration
	OnExhausted    ExhaustedAction
}

var ErrConsumerPaused = errors.New("consumer paused; message preserved")

// ProcessingError preserves the cause and describes why consumption stopped.
// Restart with backoff or RunWithRecovery; never restart in an immediate loop.
type ProcessingError struct {
	Kind     FailureKind
	Attempts int
	Err      error
}

func (e *ProcessingError) Error() string {
	return fmt.Sprintf("%s: %s after %d attempts: %v", ErrConsumerPaused, e.Kind, e.Attempts, e.Err)
}
func (e *ProcessingError) Unwrap() error        { return e.Err }
func (e *ProcessingError) Is(target error) bool { return target == ErrConsumerPaused }

func (p *RetryPolicy) normalized() RetryPolicy {
	var out RetryPolicy
	if p == nil {
		out.MaxRetries = 3
	} else {
		out = *p
	}
	if out.InitialBackoff == 0 {
		out.InitialBackoff = 100 * time.Millisecond
	}
	if out.MaxBackoff == 0 {
		out.MaxBackoff = 5 * time.Second
	}
	if out.MaxElapsedTime == 0 {
		out.MaxElapsedTime = 10 * time.Second
	}
	return out
}

func (p RetryPolicy) validate(hasDLQ bool) error {
	if p.MaxRetries < 0 || p.InitialBackoff < 0 || p.MaxBackoff < p.InitialBackoff || p.MaxElapsedTime <= 0 {
		return errors.New("invalid retry limits or time budget")
	}
	if p.OnExhausted > DeadLetter {
		return errors.New("invalid exhausted action")
	}
	if p.OnExhausted == DeadLetter && !hasDLQ {
		return errors.New("dead-letter exhaustion requires DeadLetterConfig")
	}
	return nil
}

type processingResult struct {
	err      error
	kind     FailureKind
	attempts int
	park     bool
}

func (r processingResult) paused() error {
	return &ProcessingError{Kind: r.kind, Attempts: r.attempts, Err: r.err}
}

// process retries in place. It never publishes a replacement or settles input.
func (p RetryPolicy) process(ctx context.Context, handler func(context.Context) error) processingResult {
	budget, cancel := context.WithTimeout(ctx, p.MaxElapsedTime)
	defer cancel()
	var result processingResult
	delay := min(p.InitialBackoff, p.MaxBackoff)
	for {
		if err := budget.Err(); err != nil {
			result.err = errors.Join(result.err, err)
			result.park = ctx.Err() == nil && result.kind == RetryableFailure && p.OnExhausted == DeadLetter
			return result
		}
		result.attempts++
		result.err = handler(budget)
		if err := ctx.Err(); err != nil {
			result.err = err
			result.park = false
			return result
		}
		if result.err == nil {
			return result
		}
		result.kind = UnknownFailure
		if p.Classify != nil {
			result.kind = p.Classify(result.err)
		}
		if result.kind > ConfigurationFailure {
			result.kind = UnknownFailure
		}
		switch result.kind {
		case PermanentFailure:
			result.park = true
			return result
		case DependencyUnavailable, ConfigurationFailure:
			return result
		}
		if result.attempts > p.MaxRetries || budget.Err() != nil {
			result.park = result.kind == RetryableFailure && p.OnExhausted == DeadLetter
			return result
		}
		if err := waitDelay(budget, time.Duration(float64(delay)*(0.5+rand.Float64()*0.5))); err != nil {
			result.err = errors.Join(result.err, err)
			result.park = ctx.Err() == nil && result.kind == RetryableFailure && p.OnExhausted == DeadLetter
			return result
		}
		if delay >= p.MaxBackoff/2 {
			delay = p.MaxBackoff
		} else {
			delay *= 2
		}
	}
}

func waitDelay(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

// RecoveryPolicy gates restarts after DependencyUnavailable. Probe must honor
// ctx. No messages are fetched while probing. Defaults: 30s interval, 5s timeout.
type RecoveryPolicy struct {
	Probe    func(context.Context) error
	Interval time.Duration
	Timeout  time.Duration
}

// RunWithRecovery runs consume, probing only after a classified dependency
// outage. consume must release its resources on return and create a fresh
// consumer/client on each invocation. Other failures return for operator action.
// ctx bounds the whole recovery loop; cancellation never starts another consumer.
func RunWithRecovery(ctx context.Context, recovery RecoveryPolicy, consume func(context.Context) error) error {
	if recovery.Interval == 0 {
		recovery.Interval = 30 * time.Second
	}
	if recovery.Timeout == 0 {
		recovery.Timeout = 5 * time.Second
	}
	if recovery.Interval < 0 || recovery.Timeout < 0 || consume == nil {
		return errors.New("invalid recovery policy")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := consume(ctx)
		paused, ok := errors.AsType[*ProcessingError](err)
		if !ok || paused.Kind != DependencyUnavailable || recovery.Probe == nil {
			return err
		}
		for {
			if err := waitDelay(ctx, recovery.Interval); err != nil {
				return err
			}
			probeCtx, cancel := context.WithTimeout(ctx, recovery.Timeout)
			probeErr := errors.Join(recovery.Probe(probeCtx), probeCtx.Err())
			cancel()
			if probeErr == nil {
				break
			}
		}
	}
}
