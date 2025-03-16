package healthcheck

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
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

func (mc *MongoChecker) Name() string                                { return mc.name }
func (mc *MongoChecker) IsRequired() bool                            { return mc.isRequired }
func (mc *MongoChecker) Check(ctx context.Context) HealthCheckResult { return HealthCheckResult{} }
