package llamastream

import (
	"context"
	"time"
)

// Event represents a single data event in the stream
type Event interface {
	// GetID returns the event's unique identifier
	GetID() string

	// GetTimestamp returns when the event occurred
	GetTimestamp() time.Time

	// GetField returns a specific field value from the event
	GetField(key string) interface{}

	// SetField sets a field value on the event
	SetField(key string, value interface{})

	// GetAllFields returns all fields in the event
	GetAllFields() map[string]interface{}
}

// Processor transforms events in a stream
type Processor interface {
	// Process transforms an event into zero or more new events
	Process(event Event) ([]Event, error)
}

// Sink is a destination where events are written
type Sink interface {
	// Write sends events to the sink
	Write(ctx context.Context, events []Event) error

	// Close closes the sink and releases resources
	Close() error
}

// Source is the origin of events in a stream
type Source interface {
	// Connect attaches a sink to this source
	Connect(sink Sink) Source

	// Start begins producing events to connected sinks
	Start(ctx context.Context) error

	// Stop halts the flow of events
	Stop() error
}

// FilteredSource is a source with events filtered by a predicate
type FilteredSource interface {
	Source

	// Start begins producing filtered events to connected sinks
	Start(ctx context.Context) error
}

// MappedSource is a source with events transformed by a mapping function
type MappedSource interface {
	Source

	// Start begins producing mapped events to connected sinks
	Start(ctx context.Context) error
}

// Pipeline combines sources, processors, and sinks
type Pipeline interface {
	// AddSource adds a source to the pipeline
	AddSource(source Source) Pipeline

	// AddProcessor adds a processor to the pipeline
	AddProcessor(processor Processor) Pipeline

	// AddSink adds a sink to the pipeline
	AddSink(sink Sink) Pipeline

	// Start begins the flow of events through the pipeline
	Start(ctx context.Context) error

	// Stop halts the flow of events
	Stop() error
}

// Scheduler manages the execution of pipelines on a schedule
type Scheduler interface {
	// Schedule adds a pipeline to run on the given schedule
	Schedule(pipeline Pipeline, cronExpr string) error

	// Start begins executing scheduled pipelines
	Start() error

	// Stop halts all scheduled executions
	Stop() error
}

// Metrics collects and reports performance metrics
type Metrics interface {
	// RecordEventProcessed increments the event processed counter
	RecordEventProcessed(sourceID string, processorID string, sinkID string)

	// RecordEventDropped increments the event dropped counter
	RecordEventDropped(sourceID string, reason string)

	// RecordLatency records the latency of event processing
	RecordLatency(sourceID string, latency time.Duration)

	// GetMetrics returns all collected metrics
	GetMetrics() map[string]interface{}
}
