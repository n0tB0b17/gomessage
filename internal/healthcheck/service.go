package healthcheck

import (
	"context"
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
