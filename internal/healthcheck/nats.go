package healthcheck

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

type NatsChecker struct {
	name       string
	conn       *nats.Conn
	timeout    time.Duration
	isRequired bool
}

func NewNatsChecker(natsConn *nats.Conn) *NatsChecker {
	return &NatsChecker{
		name:       "nats",
		conn:       natsConn,
		timeout:    5 * time.Second,
		isRequired: true,
	}
}

func (nc *NatsChecker) Name() string                                { return nc.name }
func (nc *NatsChecker) IsRequired() bool                            { return nc.isRequired }
func (nc *NatsChecker) Check(ctx context.Context) HealthCheckResult { return HealthCheckResult{} }
