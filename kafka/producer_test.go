package kafka

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBuildRecord(t *testing.T) {
	t.Run("empty key stays nil", func(t *testing.T) {
		r := buildRecord(&Message{Topic: "t"}, []byte("v"))
		assert.Nil(t, r.Key, "a non-nil key would pin every keyless message to one partition")
		assert.Equal(t, "t", r.Topic)
		assert.Equal(t, []byte("v"), r.Value)
		assert.Nil(t, r.Headers)
	})

	t.Run("key is set", func(t *testing.T) {
		r := buildRecord(&Message{Topic: "t", Key: "k"}, nil)
		assert.Equal(t, []byte("k"), r.Key)
	})

	t.Run("headers are copied", func(t *testing.T) {
		r := buildRecord(&Message{
			Topic:   "t",
			Headers: map[string]string{"a": "1", "b": "2"},
		}, nil)
		assert.ElementsMatch(t, []kgo.RecordHeader{
			{Key: "a", Value: []byte("1")},
			{Key: "b", Value: []byte("2")},
		}, r.Headers)
	})
}

func TestDLQErrorMessage(t *testing.T) {
	t.Run("short error is kept", func(t *testing.T) {
		assert.Equal(t, "boom", DLQErrorMessage(errors.New("boom")))
	})

	t.Run("long error is truncated to the limit", func(t *testing.T) {
		got := DLQErrorMessage(errors.New(strings.Repeat("a", 2*maxDLQErrorLen)))
		assert.Len(t, got, maxDLQErrorLen)
	})

	t.Run("truncation never splits a UTF-8 sequence", func(t *testing.T) {
		// "é" is 2 bytes; an odd limit would land in the middle of one.
		got := DLQErrorMessage(errors.New(strings.Repeat("é", maxDLQErrorLen)))
		assert.True(t, utf8.ValidString(got))
		assert.LessOrEqual(t, len(got), maxDLQErrorLen)
	})
}

type typedErr struct{ detail string }

func (e *typedErr) Error() string { return e.detail }

func TestDLQErrorType(t *testing.T) {
	secret := "token=hunter2 email=a@b.c"

	cases := []struct {
		name string
		err  error
		want string
	}{
		{"plain error", errors.New(secret), "*errors.errorString"},
		{"custom type", &typedErr{secret}, "*kafka.typedErr"},
		{"wrapped once", fmt.Errorf("handling order 42: %w", &typedErr{secret}), "*kafka.typedErr"},
		{"wrapped twice", fmt.Errorf("a: %w", fmt.Errorf("b: %w", &typedErr{secret})), "*kafka.typedErr"},
		{"multiple %w", fmt.Errorf("%w and %w", &typedErr{secret}, io.EOF), "*kafka.typedErr"},
		{"joined", errors.Join(&typedErr{secret}, io.EOF), "*kafka.typedErr"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DLQErrorType(tc.err)
			assert.Equal(t, tc.want, got)
			assert.NotContains(t, got, "hunter2", "the message must never reach the header")
		})
	}
}

func TestWithDLQClass(t *testing.T) {
	secret := "token=hunter2 email=a@b.c"
	base := errors.New(secret)

	t.Run("the class replaces the type and the message never appears", func(t *testing.T) {
		err := WithDLQClass(base, "db_timeout")
		got := DLQErrorType(err)
		assert.Equal(t, "db_timeout", got)
		assert.NotContains(t, got, "hunter2")
	})

	t.Run("a class survives wrapping", func(t *testing.T) {
		err := fmt.Errorf("handling order 42: %w", WithDLQClass(base, "invalid_payload"))
		assert.Equal(t, "invalid_payload", DLQErrorType(err))

		joined := errors.Join(io.EOF, WithDLQClass(base, "joined_class"))
		assert.Equal(t, "joined_class", DLQErrorType(joined))
	})

	t.Run("the outermost class wins", func(t *testing.T) {
		err := WithDLQClass(WithDLQClass(base, "inner"), "outer")
		assert.Equal(t, "outer", DLQErrorType(err))
	})

	t.Run("the tagged error still behaves like the original", func(t *testing.T) {
		err := WithDLQClass(base, "x")
		assert.Equal(t, secret, err.Error())
		assert.ErrorIs(t, err, base)
		assert.Nil(t, WithDLQClass(nil, "x"), "nil stays nil")
	})
}

func TestWithDLQErrorFormatter(t *testing.T) {
	secret := errors.New("password=hunter2")

	p := NewProducer(nil).(*producer)
	assert.Equal(t, "*errors.errorString", p.dlqErrorFormat(secret), "default writes the error type, not the message")

	p = NewProducer(nil, WithDLQErrorFormatter(DLQErrorMessage)).(*producer)
	assert.Equal(t, "password=hunter2", p.dlqErrorFormat(secret), "message is opt-in")

	p = NewProducer(nil, WithDLQErrorFormatter(func(error) string { return "redacted" })).(*producer)
	assert.Equal(t, "redacted", p.dlqErrorFormat(secret))

	p = NewProducer(nil, WithDLQErrorFormatter(nil)).(*producer)
	assert.Equal(t, "*errors.errorString", p.dlqErrorFormat(secret), "a nil formatter must not replace the default")
}
