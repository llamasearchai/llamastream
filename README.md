# LlamaStream: Real-time Event Processing for Go

LlamaStream is a lightweight, high-performance event streaming and processing library for Go. It provides a flexible framework for building real-time data pipelines with clean abstractions and a simple API.

[![Go Reference](https://pkg.go.dev/badge/github.com/llamastream/llamastream.svg)](https://pkg.go.dev/github.com/llamastream/llamastream)
[![Go Report Card](https://goreportcard.com/badge/github.com/llamastream/llamastream)](https://goreportcard.com/report/github.com/llamastream/llamastream)
[![License](https://img.shields.io/github/license/llamastream/llamastream)](https://github.com/llamastream/llamastream/blob/main/LICENSE)

## Features

- 🔄 **Stream Processing**: Process events in real-time through a pipeline of operations
- 🔌 **Pluggable Sources & Sinks**: Extensible architecture for different data sources and destinations
- 🧮 **Rich Processing Operations**: Filter, map, aggregate, window, and join operations
- 📊 **Built-in Metrics**: Monitor throughput, latency, and errors in your pipelines
- 🕒 **Scheduling**: Run pipelines on fixed schedules with cron-like expressions
- ⚡ **High Performance**: Designed for efficiency and low latency
- 🧠 **Smart Error Handling**: Built-in retry mechanisms and failure policies

## Installation

```bash
go get github.com/llamastream/llamastream
```

## Quick Start

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/llamastream/llamastream"
)

func main() {
	// Create a pipeline
	pipeline := llamastream.NewPipeline("simple-pipeline")

	// Create a source that generates events every second
	source := llamastream.NewGeneratorSource(func(ctx context.Context) llamastream.Event {
		return llamastream.NewEvent("event-"+time.Now().String(), map[string]interface{}{
			"timestamp": time.Now(),
			"value":     rand.Float64(),
		})
	}, 1*time.Second)

	// Add a filter to only keep events with value > 0.5
	filterProcessor := llamastream.NewFilterProcessor(func(event llamastream.Event) bool {
		value, _ := event.GetField("value").(float64)
		return value > 0.5
	})

	// Add a transform to add a new field
	mapProcessor := llamastream.NewMapProcessor(func(event llamastream.Event) llamastream.Event {
		value, _ := event.GetField("value").(float64)
		event.SetField("scaled_value", value*100)
		return event
	})

	// Create a sink to print events to console
	sink := llamastream.NewConsoleSink()

	// Build the pipeline
	pipeline.
		AddSource(source).
		AddProcessor(filterProcessor).
		AddProcessor(mapProcessor).
		AddSink(sink)

	// Run the pipeline
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start the pipeline
	if err := pipeline.Start(ctx); err != nil {
		log.Fatalf("Failed to start pipeline: %v", err)
	}

	// Wait for pipeline to complete
	<-ctx.Done()
	pipeline.Stop()
}
```

## Core Concepts

### Events

Events are the core data unit in LlamaStream. They have:
- A unique identifier
- A timestamp
- A set of key-value fields

```go
event := llamastream.NewEvent("my-event-id", map[string]interface{}{
    "user_id": 12345,
    "action": "login",
    "timestamp": time.Now(),
})
```

### Sources

Sources produce events from external systems like Kafka, Redis, or custom generators.

```go
// Create a Kafka source
kafkaSource := llamastream.NewKafkaSource(
    []string{"localhost:9092"},
    "my-topic",
    "my-group",
)

// Create a Redis source
redisSource := llamastream.NewRedisSource(
    "localhost:6379",
    "my-channel",
).WithPassword("password")
```

### Processors

Processors transform events in various ways:

```go
// Filter events based on a condition
filterProcessor := llamastream.NewFilterProcessor(func(event llamastream.Event) bool {
    return event.GetField("status") == "success"
})

// Transform events by adding/modifying fields
mapProcessor := llamastream.NewMapProcessor(func(event llamastream.Event) llamastream.Event {
    event.SetField("processed_at", time.Now())
    return event
})

// Aggregate events to calculate metrics
aggregateProcessor := llamastream.NewAggregateProcessor(
    func(state map[string]interface{}, event llamastream.Event) (map[string]interface{}, []llamastream.Event, error) {
        // Update the count
        count, _ := state["count"].(int)
        state["count"] = count + 1
        
        // Emit an event every 100 counts
        if count % 100 == 0 {
            countEvent := llamastream.NewEvent("count-event", map[string]interface{}{
                "count": count,
            })
            return state, []llamastream.Event{countEvent}, nil
        }
        
        return state, nil, nil
    },
)

// Window events by time
windowProcessor := llamastream.NewWindowProcessor(
    10*time.Second,
    func(windowEnd time.Time, events []llamastream.Event) []llamastream.Event {
        // Process window of events
        // ...
        return resultEvents
    },
)
```

### Sinks

Sinks write events to external systems:

```go
// Console sink for debugging
consoleSink := llamastream.NewConsoleSink()

// File sink for persisting events
fileSink := llamastream.NewFileSink("/path/to/events.json")

// Kafka sink for sending to Kafka
kafkaSink := llamastream.NewKafkaSink(
    []string{"localhost:9092"},
    "output-topic",
)

// Redis sink for publishing to Redis
redisSink := llamastream.NewRedisSink(
    "localhost:6379",
    "output-channel",
).WithPubSub()
```

### Pipelines

Pipelines connect sources, processors, and sinks:

```go
pipeline := llamastream.NewPipeline("my-pipeline")
pipeline.
    AddSource(source).
    AddProcessor(filterProcessor).
    AddProcessor(mapProcessor).
    AddSink(sink)

// Start the pipeline
if err := pipeline.Start(ctx); err != nil {
    log.Fatalf("Failed to start pipeline: %v", err)
}

// Stop the pipeline when done
pipeline.Stop()
```

## Advanced Usage

### Error Handling

LlamaStream provides several error handling strategies:

```go
// Retry on failure
source.WithRetry(
    llamastream.RetryOptions{
        MaxRetries: 3,
        Backoff:    llamastream.ExponentialBackoff(100*time.Millisecond),
    },
)

// Dead-letter queue
pipeline.WithDeadLetterSink(
    llamastream.NewFileSink("/path/to/errors.json"),
)
```

### Metrics

Monitor your pipelines with built-in metrics:

```go
// Create metrics collector
metrics := llamastream.NewMetrics()

// Attach to pipeline
pipeline.WithMetrics(metrics)

// Print metrics every minute
go func() {
    ticker := time.NewTicker(1 * time.Minute)
    for range ticker.C {
        log.Printf("Metrics: %v", metrics.GetMetrics())
    }
}()

// For Prometheus integration
prometheusMetrics := llamastream.NewPrometheusMetrics()
pipeline.WithMetrics(prometheusMetrics)

// Expose metrics endpoint
http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
    fmt.Fprint(w, prometheusMetrics.GetPrometheusMetrics())
})
http.ListenAndServe(":8080", nil)
```

### Scheduling

Run pipelines on a schedule:

```go
scheduler := llamastream.NewScheduler()

// Schedule a pipeline to run every hour
scheduler.Schedule(pipeline, "@hourly")

// Schedule a pipeline to run every 5 minutes
scheduler.Schedule(pipeline, "5m")

// Start the scheduler
scheduler.Start()

// Stop the scheduler when done
scheduler.Stop()
```

## Examples

### Log Processing Pipeline

```go
// Create a pipeline for processing logs
pipeline := llamastream.NewPipeline("log-processor")

// Source: Read from a log file
source := llamastream.NewFileTailSource("/var/log/app.log")

// Process: Parse JSON logs
jsonProcessor := llamastream.NewMapProcessor(func(event llamastream.Event) llamastream.Event {
    line, _ := event.GetField("line").(string)
    var parsed map[string]interface{}
    if err := json.Unmarshal([]byte(line), &parsed); err == nil {
        for k, v := range parsed {
            event.SetField(k, v)
        }
    }
    return event
})

// Process: Filter errors
errorFilter := llamastream.NewFilterProcessor(func(event llamastream.Event) bool {
    level, _ := event.GetField("level").(string)
    return level == "error"
})

// Sink: Send to Elasticsearch
esSink := llamastream.NewElasticsearchSink(
    "http://localhost:9200",
    "logs",
)

// Build and start the pipeline
pipeline.
    AddSource(source).
    AddProcessor(jsonProcessor).
    AddProcessor(errorFilter).
    AddSink(esSink)

pipeline.Start(context.Background())
```

### Real-time Analytics

```go
// Create a pipeline for user analytics
pipeline := llamastream.NewPipeline("user-analytics")

// Source: Listen to user events from Kafka
source := llamastream.NewKafkaSource(
    []string{"localhost:9092"},
    "user-events",
    "analytics-consumer",
)

// Process: Window events by 5-minute intervals
windowProcessor := llamastream.NewWindowProcessor(
    5*time.Minute,
    func(windowEnd time.Time, events []llamastream.Event) []llamastream.Event {
        // Count unique users
        users := make(map[string]bool)
        for _, event := range events {
            userID, _ := event.GetField("user_id").(string)
            if userID != "" {
                users[userID] = true
            }
        }
        
        // Create summary event
        summary := llamastream.NewEvent("window-summary", map[string]interface{}{
            "window_end": windowEnd,
            "user_count": len(users),
            "event_count": len(events),
        })
        
        return []llamastream.Event{summary}
    },
)

// Sink: Write to Redis for real-time dashboards
redisSink := llamastream.NewRedisSink(
    "localhost:6379",
    "analytics",
).WithPubSub()

// Build and start the pipeline
pipeline.
    AddSource(source).
    AddProcessor(windowProcessor).
    AddSink(redisSink)

pipeline.Start(context.Background())
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Inspired by Apache Flink, Kafka Streams, and other stream processing frameworks
- Built with love by the LlamaStream team 
# Updated in commit 1 - 2025-04-04 17:40:28

# Updated in commit 9 - 2025-04-04 17:40:28

# Updated in commit 17 - 2025-04-04 17:40:28
