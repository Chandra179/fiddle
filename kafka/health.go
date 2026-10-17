package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ClusterHealth contains health information about the Kafka cluster.
type ClusterHealth struct {
	Healthy      bool
	BrokerCount  int
	ControllerID int32
	ClusterID    string
	ResponseTime time.Duration
	TopicCount   int
	ErrorMessage string
}

// healthChecker implements the HealthChecker interface.
type healthChecker struct {
	client *kgo.Client
}

// NewHealthChecker creates a new HealthChecker.
func NewHealthChecker(client *kgo.Client) HealthChecker {
	return &healthChecker{client: client}
}

// Check performs a basic health check by pinging the cluster.
func (h *healthChecker) Check(ctx context.Context) error {
	health, err := h.CheckWithMetadata(ctx)
	if err != nil {
		return err
	}
	if !health.Healthy {
		return fmt.Errorf("kafka cluster unhealthy: %s", health.ErrorMessage)
	}
	return nil
}

// CheckWithMetadata performs a health check and returns detailed cluster information.
func (h *healthChecker) CheckWithMetadata(ctx context.Context) (*ClusterHealth, error) {
	start := time.Now()
	health := &ClusterHealth{}

	adminClient := kadm.NewClient(h.client)

	metadata, err := adminClient.Metadata(ctx)
	if err != nil {
		health.Healthy = false
		health.ErrorMessage = fmt.Sprintf("failed to get metadata: %v", err)
		health.ResponseTime = time.Since(start)
		return health, nil
	}

	health.ResponseTime = time.Since(start)
	health.ClusterID = metadata.Cluster
	health.ControllerID = metadata.Controller
	health.BrokerCount = len(metadata.Brokers)
	health.TopicCount = len(metadata.Topics)

	if health.BrokerCount == 0 {
		health.Healthy = false
		health.ErrorMessage = "no brokers available"
		return health, nil
	}

	if health.ControllerID < 0 {
		health.Healthy = false
		health.ErrorMessage = "no controller elected"
		return health, nil
	}

	health.Healthy = true
	return health, nil
}

// LivenessCheck is Check with a 5 second timeout, for liveness probes.
func (h *healthChecker) LivenessCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return h.Check(ctx)
}
