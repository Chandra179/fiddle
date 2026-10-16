package kafka

import (
	"testing"

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
