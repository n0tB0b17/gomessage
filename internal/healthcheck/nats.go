package healthcheck

import (
	"context"
	"fmt"
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

func (nc *NatsChecker) Name() string     { return nc.name }
func (nc *NatsChecker) IsRequired() bool { return nc.isRequired }
func (nc *NatsChecker) Check(ctx context.Context) HealthCheckResult {
	start := time.Now()
	resp := HealthCheckResult{
		Timestamp: start,
	}

	if nc.conn.Status() != nats.CONNECTED {
		resp.Status = StatusDown
		resp.Latency = time.Since(start).Milliseconds()
		resp.Message = fmt.Sprintf("NATS server not connected, current status: %s", nc.conn.Status())

		return resp
	}

	// unecessary but good to have
	inbox := nats.NewInbox()
	sus, err := nc.conn.SubscribeSync(inbox)
	if err != nil {
		resp.Status = StatusDown
		resp.Latency = time.Since(start).Milliseconds()
		resp.Message = fmt.Sprintf("Failed to create a new NATS subscriber: %s", err.Error())

		return resp
	}
	defer sus.Unsubscribe()

	err = nc.conn.Publish(inbox, []byte("ping"))
	if err != nil {
		resp.Status = StatusDown
		resp.Latency = time.Since(start).Milliseconds()
		resp.Message = fmt.Sprintf("Failed to publish NATS message: %s", err.Error())

		return resp
	}

	_, err = sus.NextMsg(nc.timeout)
	latency := time.Since(start).Milliseconds()
	resp.Latency = latency

	if err != nil {
		resp.Status = StatusDown
		resp.Message = fmt.Sprintf("Failed to receive NATS message: %s", err.Error())
	} else {
		resp.Status = StatusUP
		resp.Message = fmt.Sprintf("NATS connected successfully")
	}

	return resp
}
