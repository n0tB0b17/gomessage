package healthcheck

import "time"

type HealthStatus struct {
	Status       Status                       `json:"status"`
	Dependencies map[string]HealthCheckResult `json:"dependencies"`
	Timestamp    time.Time                    `json:"timestamp"`
}
