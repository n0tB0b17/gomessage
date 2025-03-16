package healthcheck

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// orchestrate health check across REGISTERED services
type HealthService struct {
	registry       *HealthRegistry // stores external services
	timeout        time.Duration
	checkFrequency time.Duration
	cache          map[string]HealthCheckResult
	cacheMutex     sync.RWMutex
	stopChannel    chan struct{}
	wg             sync.WaitGroup
}

func NewHealthService(r *HealthRegistry) *HealthService {
	return &HealthService{
		registry:       r,
		timeout:        5 * time.Second,
		checkFrequency: 10 * time.Second,
		cache:          make(map[string]HealthCheckResult),
		stopChannel:    make(chan struct{}),
	}
}

func (hs *HealthService) Start() {
	hs.wg.Add(1)
	go hs.backgroundCheck()
	fmt.Printf("Health check service started \n")
}

func (hs *HealthService) Stop() {
	close(hs.stopChannel)
	hs.wg.Wait()
	fmt.Printf("Health check service stopped \n")
}

func (hs *HealthService) backgroundCheck() {
	defer hs.wg.Done()
	checkFreq := time.NewTicker(hs.checkFrequency)
	defer checkFreq.Stop()

	// initial check
	hs.checkAll(context.Background())

	for {
		select {
		case <-checkFreq.C:
			hs.checkAll(context.Background())
		case <-hs.stopChannel:
			return
		}
	}
}

func (hs *HealthService) checkAll(ctx context.Context) {
	services := hs.registry.GetAllExternalServices()
	if len(services) == 0 {
		return
	}

	var wg sync.WaitGroup
	resp := make(map[string]HealthCheckResult)
	respMU := sync.Mutex{}

	for _, service := range services {
		wg.Add(1)
		go func(checker DependencyChecker) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), hs.timeout)
			defer cancel()

			healthCheckResp := checker.Check(ctx)

			respMU.Lock()
			resp[checker.Name()] = healthCheckResp
			respMU.Unlock()

		}(service)
	}

	wg.Wait()

	hs.cacheMutex.Lock()
	for name, res := range resp {
		hs.cache[name] = res
	}
	hs.cacheMutex.Unlock()
}

// get CURRENT health status
func (hs *HealthService) GetHealth() HealthStatus {
	hs.cacheMutex.RLock()
	defer hs.cacheMutex.RUnlock()

	status := HealthStatus{
		Status:       StatusUP,
		Dependencies: make(map[string]HealthCheckResult),
		Timestamp:    time.Now(),
	}

	// copying service's result
	for name, result := range hs.cache {
		status.Dependencies[name] = result
	}

	services := hs.registry.GetAllExternalServices()
	for _, service := range services {
		if service.IsRequired() {
			if result, exists := status.Dependencies[service.Name()]; exists && result.Status == StatusDown {
				status.Status = StatusDown
				break
			}
		}
	}

	return status
}

// force check of all services
func (hs *HealthService) CheckNow(ctx context.Context) HealthStatus {
	hs.checkAll(ctx)
	return hs.GetHealth()
}
