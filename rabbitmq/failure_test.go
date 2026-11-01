package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func quickPolicy(kind FailureKind) RetryPolicy {
	return RetryPolicy{Classify: func(error) FailureKind { return kind }, MaxRetries: 2,
		InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, MaxElapsedTime: time.Second}
}

func TestFailureClassificationControlsRetries(t *testing.T) {
	boom := errors.New("application failure")
	for _, tc := range []struct {
		kind     FailureKind
		attempts int
		park     bool
	}{
		{UnknownFailure, 3, false}, {RetryableFailure, 3, true},
		{PermanentFailure, 1, true}, {DependencyUnavailable, 1, false}, {ConfigurationFailure, 1, false},
	} {
		t.Run(tc.kind.String(), func(t *testing.T) {
			p := quickPolicy(tc.kind)
			p.OnExhausted = DeadLetter
			calls := 0
			got := p.process(context.Background(), func(context.Context) error { calls++; return boom })
			if calls != tc.attempts || got.attempts != calls || got.park != tc.park || !errors.Is(got.paused(), boom) || !errors.Is(got.paused(), ErrConsumerPaused) {
				t.Fatalf("calls=%d result=%+v", calls, got)
			}
		})
	}
}

func TestRetryBudgetAndCancellation(t *testing.T) {
	boom := errors.New("temporary failure")
	p := quickPolicy(RetryableFailure)
	p.MaxRetries = 1000
	p.MaxElapsedTime = 15 * time.Millisecond
	p.InitialBackoff, p.MaxBackoff = time.Second, time.Second
	start := time.Now()
	got := p.process(context.Background(), func(context.Context) error { return boom })
	if got.attempts != 1 || !errors.Is(got.err, boom) || !errors.Is(got.err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatalf("time budget did not stop retry wait: %+v", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	got = p.process(ctx, func(context.Context) error { cancel(); return boom })
	if !errors.Is(got.err, context.Canceled) || got.park {
		t.Fatalf("cancelled input must remain unsettled: %+v", got)
	}
	called := false
	got = p.process(ctx, func(context.Context) error { called = true; return nil })
	if called || !errors.Is(got.err, context.Canceled) {
		t.Fatal("cancelled context invoked handler")
	}
}

func TestRetrySuccessAndExplicitZero(t *testing.T) {
	p := quickPolicy(RetryableFailure)
	calls := 0
	got := p.process(context.Background(), func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("not yet")
		}
		return nil
	})
	if got.err != nil || calls != 3 {
		t.Fatalf("result=%+v calls=%d", got, calls)
	}
	p.MaxRetries = 0
	got = p.process(context.Background(), func(context.Context) error { return errors.New("failure") })
	if got.attempts != 1 {
		t.Fatalf("zero means no retries: %+v", got)
	}
	var defaults *RetryPolicy
	if defaults.normalized().MaxRetries != 3 {
		t.Fatal("nil policy lost default retry budget")
	}
}

func TestFailurePolicyValidation(t *testing.T) {
	p := quickPolicy(RetryableFailure)
	p.OnExhausted = DeadLetter
	if p.validate(false) == nil || p.validate(true) != nil {
		t.Fatal("parking requires configured destination")
	}
	p.MaxRetries = -1
	if p.validate(true) == nil {
		t.Fatal("negative retry budget accepted")
	}
}

func TestRecoveryProbesDoNotConsumeDuringOutage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	runs, probes := 0, 0
	err := RunWithRecovery(ctx, RecoveryPolicy{
		Interval: time.Millisecond, Timeout: time.Second,
		Probe: func(context.Context) error {
			probes++
			if probes < 4 {
				return errors.New("still down")
			}
			return nil
		},
	}, func(context.Context) error {
		runs++
		if runs == 1 {
			return &ProcessingError{Kind: DependencyUnavailable, Attempts: 1, Err: errors.New("service down")}
		}
		if probes != 4 {
			t.Fatal("consumed before successful recovery probe")
		}
		return nil
	})
	if err != nil || runs != 2 || probes != 4 {
		t.Fatalf("err=%v runs=%d probes=%d", err, runs, probes)
	}
}

func TestRecoveryDoesNotRestartUnknownFailures(t *testing.T) {
	probes, runs := 0, 0
	err := RunWithRecovery(context.Background(), RecoveryPolicy{Probe: func(context.Context) error { probes++; return nil }}, func(context.Context) error {
		runs++
		return &ProcessingError{Kind: UnknownFailure, Attempts: 1, Err: errors.New("unknown")}
	})
	if !errors.Is(err, ErrConsumerPaused) || probes != 0 || runs != 1 {
		t.Fatalf("err=%v probes=%d runs=%d", err, probes, runs)
	}
}

func TestRecoveryCancellationStopsProbesAndRestarts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runs := 0
	err := RunWithRecovery(ctx, RecoveryPolicy{Interval: time.Millisecond, Probe: func(ctx context.Context) error { cancel(); return ctx.Err() }}, func(context.Context) error {
		runs++
		return &ProcessingError{Kind: DependencyUnavailable, Err: errors.New("down")}
	})
	if !errors.Is(err, context.Canceled) || runs != 1 {
		t.Fatalf("err=%v runs=%d", err, runs)
	}
}

func TestElapsedBudgetUsesExplicitExhaustionPolicy(t *testing.T) {
	p := quickPolicy(RetryableFailure)
	p.MaxRetries = 100
	p.MaxElapsedTime = 10 * time.Millisecond
	p.InitialBackoff, p.MaxBackoff = time.Second, time.Second
	p.OnExhausted = DeadLetter
	result := p.process(context.Background(), func(context.Context) error { return errors.New("temporary") })
	if !result.park || result.kind != RetryableFailure {
		t.Fatalf("elapsed budget ignored explicit exhaustion policy: %+v", result)
	}
}
