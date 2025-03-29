package llamastream

import (
	"fmt"
	"sync"
	"time"
)

// MetricsImpl implements the Metrics interface
type MetricsImpl struct {
	// Mutex to protect metrics data
	mu sync.RWMutex

	// Counters for events processed
	eventProcessed map[string]int64

	// Counters for events dropped
	eventDropped map[string]int64

	// Latency measurements
	latency map[string][]time.Duration

	// Start time for rate calculations
	startTime time.Time
}

// NewMetrics creates a new metrics collector
func NewMetrics() *MetricsImpl {
	return &MetricsImpl{
		eventProcessed: make(map[string]int64),
		eventDropped:   make(map[string]int64),
		latency:        make(map[string][]time.Duration),
		startTime:      time.Now(),
	}
}

// RecordEventProcessed increments the event processed counter
func (m *MetricsImpl) RecordEventProcessed(sourceID string, processorID string, sinkID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Increment source counter
	sourceKey := fmt.Sprintf("source:%s:processed", sourceID)
	m.eventProcessed[sourceKey]++

	// Increment processor counter if provided
	if processorID != "" {
		processorKey := fmt.Sprintf("processor:%s:processed", processorID)
		m.eventProcessed[processorKey]++
	}

	// Increment sink counter if provided
	if sinkID != "" {
		sinkKey := fmt.Sprintf("sink:%s:processed", sinkID)
		m.eventProcessed[sinkKey]++
	}

	// Increment total counter
	m.eventProcessed["total:processed"]++
}

// RecordEventDropped increments the event dropped counter
func (m *MetricsImpl) RecordEventDropped(sourceID string, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Increment source counter
	sourceKey := fmt.Sprintf("source:%s:dropped", sourceID)
	m.eventDropped[sourceKey]++

	// Increment reason counter
	reasonKey := fmt.Sprintf("reason:%s:dropped", reason)
	m.eventDropped[reasonKey]++

	// Increment total counter
	m.eventDropped["total:dropped"]++
}

// RecordLatency records the latency of event processing
func (m *MetricsImpl) RecordLatency(sourceID string, latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("latency:%s", sourceID)

	// Keep only the last 1000 latency measurements to avoid unbounded growth
	measurements := m.latency[key]
	if len(measurements) >= 1000 {
		// Remove the oldest measurement
		measurements = measurements[1:]
	}

	// Add the new measurement
	m.latency[key] = append(measurements, latency)
}

// GetMetrics returns all collected metrics
func (m *MetricsImpl) GetMetrics() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Calculate durations
	uptime := time.Since(m.startTime)

	// Copy all metrics
	metrics := make(map[string]interface{})

	// Add processed counts
	for k, v := range m.eventProcessed {
		metrics[k] = v
	}

	// Add dropped counts
	for k, v := range m.eventDropped {
		metrics[k] = v
	}

	// Add rates
	totalProcessed := m.eventProcessed["total:processed"]
	if totalProcessed > 0 {
		eventsPerSecond := float64(totalProcessed) / uptime.Seconds()
		metrics["rate:events_per_second"] = eventsPerSecond
	}

	// Calculate latency statistics
	for k, latencies := range m.latency {
		if len(latencies) == 0 {
			continue
		}

		// Calculate min, max, avg
		var sum time.Duration
		min := latencies[0]
		max := latencies[0]

		for _, l := range latencies {
			sum += l
			if l < min {
				min = l
			}
			if l > max {
				max = l
			}
		}

		avg := sum / time.Duration(len(latencies))

		metrics[k+":min"] = min.String()
		metrics[k+":max"] = max.String()
		metrics[k+":avg"] = avg.String()
		metrics[k+":count"] = len(latencies)
	}

	// Add general metrics
	metrics["uptime"] = uptime.String()
	metrics["start_time"] = m.startTime.Format(time.RFC3339)

	return metrics
}

// Reset resets all metrics
func (m *MetricsImpl) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.eventProcessed = make(map[string]int64)
	m.eventDropped = make(map[string]int64)
	m.latency = make(map[string][]time.Duration)
	m.startTime = time.Now()
}

// PrometheusMetrics is a metrics implementation that formats metrics for Prometheus
type PrometheusMetrics struct {
	MetricsImpl
}

// NewPrometheusMetrics creates a new Prometheus metrics collector
func NewPrometheusMetrics() *PrometheusMetrics {
	return &PrometheusMetrics{
		MetricsImpl: *NewMetrics(),
	}
}

// GetPrometheusMetrics returns metrics in Prometheus format
func (m *PrometheusMetrics) GetPrometheusMetrics() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result string

	// Add processed counts
	result += "# HELP llamastream_events_processed Number of events processed\n"
	result += "# TYPE llamastream_events_processed counter\n"
	for k, v := range m.eventProcessed {
		result += fmt.Sprintf("llamastream_events_processed{source=\"%s\"} %d\n", k, v)
	}

	// Add dropped counts
	result += "# HELP llamastream_events_dropped Number of events dropped\n"
	result += "# TYPE llamastream_events_dropped counter\n"
	for k, v := range m.eventDropped {
		result += fmt.Sprintf("llamastream_events_dropped{source=\"%s\"} %d\n", k, v)
	}

	// Add latency metrics
	result += "# HELP llamastream_latency_seconds Latency of event processing in seconds\n"
	result += "# TYPE llamastream_latency_seconds gauge\n"
	for k, latencies := range m.latency {
		if len(latencies) == 0 {
			continue
		}

		// Calculate min, max, avg
		var sum time.Duration
		min := latencies[0]
		max := latencies[0]

		for _, l := range latencies {
			sum += l
			if l < min {
				min = l
			}
			if l > max {
				max = l
			}
		}

		avg := sum / time.Duration(len(latencies))

		result += fmt.Sprintf("llamastream_latency_seconds{source=\"%s\",type=\"min\"} %f\n", k, min.Seconds())
		result += fmt.Sprintf("llamastream_latency_seconds{source=\"%s\",type=\"max\"} %f\n", k, max.Seconds())
		result += fmt.Sprintf("llamastream_latency_seconds{source=\"%s\",type=\"avg\"} %f\n", k, avg.Seconds())
	}

	// Add uptime
	uptime := time.Since(m.startTime)
	result += "# HELP llamastream_uptime_seconds Total uptime in seconds\n"
	result += "# TYPE llamastream_uptime_seconds gauge\n"
	result += fmt.Sprintf("llamastream_uptime_seconds %f\n", uptime.Seconds())

	return result
}
