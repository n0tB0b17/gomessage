package healthcheck

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

type MongoChecker struct {
	name       string
	client     *mongo.Client
	timeout    time.Duration
	isRequired bool
}

func NewMongoChecker(client *mongo.Client) *MongoChecker {
	return &MongoChecker{
		name:       "mongo",
		client:     client,
		timeout:    5 * time.Second,
		isRequired: true,
	}
}

func (mc *MongoChecker) Name() string     { return mc.name }
func (mc *MongoChecker) IsRequired() bool { return mc.isRequired }
func (mc *MongoChecker) Check(ctx context.Context) HealthCheckResult {
	start := time.Now()
	timeoutCTX, cancel := context.WithTimeout(ctx, mc.timeout)
	defer cancel()

	resp := HealthCheckResult{
		Timestamp: start,
	}

	err := mc.client.Ping(timeoutCTX, readpref.Primary())
	latency := time.Since(start).Milliseconds()
	resp.Latency = latency
	if err != nil {
		resp.Status = StatusDown
		resp.Message = fmt.Sprintf("mongodb database connection failed: %s", err.Error())
	} else {
		resp.Status = StatusUP
		resp.Message = fmt.Sprintf("mongodb database server is up and running")
	}
	return resp
}
