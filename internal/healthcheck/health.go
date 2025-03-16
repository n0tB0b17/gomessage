package healthcheck

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Status string

const (
	StatusUP   Status = "UP"
	StatusDown Status = "DOWN"
)

type HealthCheckResult struct {
	Status    Status    `json:"status"`
	Message   string    `json:"message,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Latency   int64     `json:"latency"`
}

// a contract for health check of external services
type DependencyChecker interface {
	Name() string
	Check(ctx context.Context) HealthCheckResult
	IsRequired() bool
}

type HealthRegistry struct {
	dependencies map[string]DependencyChecker
	mu           sync.RWMutex
}

func NewHealthRegistry() *HealthRegistry {
	return &HealthRegistry{
		dependencies: make(map[string]DependencyChecker),
	}
}

func (hr *HealthRegistry) Register(dependency DependencyChecker) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	hr.dependencies[dependency.Name()] = dependency
	fmt.Printf("Registered a external server named: %s to our health-check registry \n", dependency.Name())
}

func (hr *HealthRegistry) Unregister(name string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	delete(hr.dependencies, name)
	fmt.Printf("Unregistered a external server named: %s from our health-check registry \n", name)
}

func (hr *HealthRegistry) GetAllExternalServices() []DependencyChecker {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	externalServices := make([]DependencyChecker, 0, len(hr.dependencies))

	for _, service := range hr.dependencies {
		externalServices = append(externalServices, service)
	}
	return externalServices
}
