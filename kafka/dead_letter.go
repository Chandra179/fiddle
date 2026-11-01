package kafka

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type ProvisionMode uint8

const (
	UseExisting ProvisionMode = iota
	Declare
)

// DeadLetterConfig describes a single destination for this consumer. Nil
// disables dead-lettering. Topic defaults to <source>.dlq only when exactly one
// source topic is configured. Configs accepts Kafka topic settings, including
// retention.ms, retention.bytes, and min.insync.replicas. Unspecified settings
// retain broker defaults; existing topics are never silently reconfigured.
type DeadLetterConfig struct {
	Topic             string
	Provision         ProvisionMode
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]string
	Timeout           time.Duration
	// ErrorFormatter defaults to DLQErrorType; opt into messages deliberately.
	ErrorFormatter func(error) string
}

func (d *DeadLetterConfig) normalized(topics []string) *DeadLetterConfig {
	if d == nil {
		return nil
	}
	out := *d
	out.Configs = maps.Clone(d.Configs)
	if out.Topic == "" && len(topics) == 1 {
		out.Topic = dlqTopic(topics[0])
	}
	if out.Timeout == 0 {
		out.Timeout = 10 * time.Second
	}
	if out.ErrorFormatter == nil {
		out.ErrorFormatter = DLQErrorType
	}
	return &out
}

func (d *DeadLetterConfig) validate(topics []string) error {
	if d == nil {
		return nil
	}
	if strings.TrimSpace(d.Topic) == "" || d.Topic == "." || d.Topic == ".." || len(d.Topic) > 249 || strings.ContainsAny(d.Topic, " /\\\t\n") {
		return errors.New("invalid dead-letter topic")
	}
	for _, source := range topics {
		if d.Topic == source {
			return errors.New("dead-letter topic must differ from source topics")
		}
	}
	if d.Provision > Declare || d.Timeout <= 0 || d.Partitions < 0 || d.ReplicationFactor < 0 {
		return errors.New("invalid dead-letter configuration")
	}
	if d.Provision == Declare && (d.Partitions == 0 || d.ReplicationFactor == 0) {
		return errors.New("declaring a dead-letter topic requires explicit partitions and replication factor")
	}
	return nil
}

func prepareDeadLetter(ctx context.Context, client *kgo.Client, d *DeadLetterConfig) error {
	if d == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, d.Timeout)
	defer cancel()
	if client.OptValue(kgo.RequiredAcks) != kgo.AllISRAcks() {
		return errors.New("dead-letter publishing requires AllISRAcks")
	}
	if id, _ := client.OptValue(kgo.TransactionalID).(string); id != "" {
		return errors.New("dead-letter consumer requires a non-transactional client")
	}
	admin := kadm.NewClient(client)
	if d.Provision == Declare {
		configs := make(map[string]*string, len(d.Configs))
		for key, value := range d.Configs {
			value := value
			configs[key] = &value
		}
		responses, err := admin.CreateTopics(ctx, d.Partitions, d.ReplicationFactor, configs, d.Topic)
		if err != nil {
			return err
		}
		response, ok := responses[d.Topic]
		if !ok {
			return errors.New("missing dead-letter create response")
		}
		if response.Err != nil && !errors.Is(response.Err, kerr.TopicAlreadyExists) {
			return response.Err
		}
	}
	topics, err := admin.ListTopics(ctx, d.Topic)
	if err != nil {
		return err
	}
	topic, ok := topics[d.Topic]
	if !ok {
		return fmt.Errorf("dead-letter topic %s does not exist", d.Topic)
	}
	if topic.Err != nil {
		return topic.Err
	}
	if len(topic.Partitions) == 0 {
		return errors.New("dead-letter topic has no partitions")
	}
	if d.Partitions > 0 && len(topic.Partitions) != int(d.Partitions) {
		return errors.New("dead-letter partition count differs from configuration")
	}
	for _, partition := range topic.Partitions {
		if partition.Err != nil {
			return partition.Err
		}
		if d.ReplicationFactor > 0 && len(partition.Replicas) != int(d.ReplicationFactor) {
			return errors.New("dead-letter replication factor differs from configuration")
		}
	}
	if len(d.Configs) == 0 {
		return nil
	}
	configs, err := admin.DescribeTopicConfigs(ctx, d.Topic)
	if err != nil {
		return err
	}
	resource, err := configs.On(d.Topic, nil)
	if err != nil {
		return err
	}
	if resource.Err != nil {
		return resource.Err
	}
	actual := make(map[string]string, len(resource.Configs))
	for _, cfg := range resource.Configs {
		if cfg.Value != nil {
			actual[cfg.Key] = *cfg.Value
		}
	}
	for key, want := range d.Configs {
		if got, ok := actual[key]; !ok || got != want {
			return fmt.Errorf("dead-letter topic setting %s differs from configuration", key)
		}
	}
	return nil
}

func deadLetterRecord(d *DeadLetterConfig, source *kgo.Record, failure processingResult) *kgo.Record {
	headers := make([]kgo.RecordHeader, 0, len(source.Headers)+7)
	for _, h := range source.Headers {
		if !strings.HasPrefix(h.Key, "fiddle.dlq.") {
			headers = append(headers, h)
		}
	}
	metadata := map[string]string{
		"source_topic":     source.Topic,
		"source_partition": strconv.FormatInt(int64(source.Partition), 10),
		"source_offset":    strconv.FormatInt(source.Offset, 10),
		"failure_kind":     failure.kind.String(),
		"attempts":         strconv.Itoa(failure.attempts),
		"failed_at":        time.Now().UTC().Format(time.RFC3339Nano),
	}
	if detail := d.ErrorFormatter(failure.err); detail != "" {
		metadata["error"] = detail
	}
	for key, value := range metadata {
		headers = append(headers, kgo.RecordHeader{Key: "fiddle.dlq." + key, Value: []byte(value)})
	}
	return &kgo.Record{Topic: d.Topic, Key: source.Key, Value: source.Value, Headers: headers}
}
