package db

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestIsTimeoutError_Classification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"deadline", context.DeadlineExceeded, true},
		{"wrapped deadline", fmt.Errorf("query: %w", context.DeadlineExceeded), true},
		{"query_canceled", &pq.Error{Code: "57014"}, true},
		{"lock_not_available", &pq.Error{Code: "55P03"}, true},
		{"idle in tx timeout", &pq.Error{Code: "25P03"}, true},
		{"idle session timeout", &pq.Error{Code: "57P05"}, true},
		{"serialization failure", &pq.Error{Code: "40001"}, false},
		{"unique violation", &pq.Error{Code: "23505"}, false},
		{"wrapped pq", fmt.Errorf("repo: %w", &pq.Error{Code: "57014"}), true},
		{"plain error", errors.New("boom"), false},
		{"string fallback", errors.New("read tcp: i/o timeout"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsTimeoutError(tc.err))
		})
	}
}

func TestIsSerializationError_Classification(t *testing.T) {
	assert.True(t, IsSerializationError(&pq.Error{Code: "40001"}))
	assert.True(t, IsSerializationError(fmt.Errorf("tx: %w", &pq.Error{Code: "40P01"})))
	assert.True(t, IsSerializationError(errors.New("ERROR: deadlock detected")))
	assert.False(t, IsSerializationError(&pq.Error{Code: "57014"}))
	assert.False(t, IsSerializationError(nil))
}

func TestIsDuplicateKeyError_Classification(t *testing.T) {
	assert.True(t, IsDuplicateKeyError(&pq.Error{Code: "23505"}))
	assert.True(t, IsDuplicateKeyError(errors.New(`duplicate key value violates unique constraint "users_email_key"`)))
	assert.False(t, IsDuplicateKeyError(&pq.Error{Code: "42P07"}))
	assert.False(t, IsDuplicateKeyError(errors.New(`relation "users" already exists`)))
	assert.False(t, IsDuplicateKeyError(nil))
}
