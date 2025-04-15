package llamastream

import (
	"fmt"
	"log"
	"time"
)

// ProcessorImpl is a basic implementation of the Processor interface
type ProcessorImpl struct {
	processFn func(Event) ([]Event, error)
	name      string
}

// NewProcessor creates a new processor with the given process function
func NewProcessorFunc(name string, processFn func(Event) ([]Event, error)) Processor {
	return &ProcessorImpl{
		processFn: processFn,
		name:      name,
	}
}

// Process processes an event and returns resulting events
func (p *ProcessorImpl) Process(event Event) ([]Event, error) {
	return p.processFn(event)
}

// FilterProcessor filters events based on a predicate
type FilterProcessor struct {
	ProcessorImpl
	filterFn func(Event) bool
}

// NewFilterProcessor creates a processor that filters events
func NewFilterProcessor(filterFn func(Event) bool) *FilterProcessor {
	processor := &FilterProcessor{
		filterFn: filterFn,
	}

	processor.ProcessorImpl = ProcessorImpl{
		name: "filter",
		processFn: func(event Event) ([]Event, error) {
			if processor.filterFn(event) {
				return []Event{event}, nil
			}
			return []Event{}, nil
		},
	}

	return processor
}

// MapProcessor transforms events using a mapping function
type MapProcessor struct {
	ProcessorImpl
	mapFn func(Event) Event
}

// NewMapProcessor creates a processor that transforms events
func NewMapProcessor(mapFn func(Event) Event) *MapProcessor {
	processor := &MapProcessor{
		mapFn: mapFn,
	}

	processor.ProcessorImpl = ProcessorImpl{
		name: "map",
		processFn: func(event Event) ([]Event, error) {
			result := processor.mapFn(event)
			if result == nil {
				return []Event{}, nil
			}
			return []Event{result}, nil
		},
	}

	return processor
}

// FlatMapProcessor transforms events using a function that returns multiple events
type FlatMapProcessor struct {
	ProcessorImpl
	flatMapFn func(Event) []Event
}

// NewFlatMapProcessor creates a processor that expands one event into multiple
func NewFlatMapProcessor(flatMapFn func(Event) []Event) *FlatMapProcessor {
	processor := &FlatMapProcessor{
		flatMapFn: flatMapFn,
	}

	processor.ProcessorImpl = ProcessorImpl{
		name: "flatmap",
		processFn: func(event Event) ([]Event, error) {
			return processor.flatMapFn(event), nil
		},
	}

	return processor
}

// WindowProcessor groups events into time windows
type WindowProcessor struct {
	ProcessorImpl
	windowSize   time.Duration
	windows      map[int64][]Event
	emitFn       func(time.Time, []Event) []Event
	currentTime  time.Time
	windowOffset time.Duration
}

// NewWindowProcessor creates a new windowing processor
func NewWindowProcessor(windowSize time.Duration, emitFn func(time.Time, []Event) []Event) *WindowProcessor {
	processor := &WindowProcessor{
		windowSize:   windowSize,
		windows:      make(map[int64][]Event),
		emitFn:       emitFn,
		currentTime:  time.Now(),
		windowOffset: 0,
	}

	processor.ProcessorImpl = ProcessorImpl{
		name: "window",
		processFn: func(event Event) ([]Event, error) {
			// Get window key based on event timestamp
			timestamp := event.GetTimestamp()
			windowKey := timestamp.UnixNano() / processor.windowSize.Nanoseconds()

			// Add event to the window
			processor.windows[windowKey] = append(processor.windows[windowKey], event)

			// Check if we need to emit any completed windows
			var result []Event

			// Current time has advanced
			if timestamp.After(processor.currentTime) {
				processor.currentTime = timestamp

				// Find all windows that are completed
				completedWindowKey := processor.currentTime.Add(-processor.windowSize).UnixNano() / processor.windowSize.Nanoseconds()

				// Emit all windows that are earlier than the completed window
				for windowKey, windowEvents := range processor.windows {
					if windowKey <= completedWindowKey {
						// Compute the window end time
						windowEndTime := time.Unix(0, windowKey*processor.windowSize.Nanoseconds()).Add(processor.windowSize)

						// Apply the emit function to get the result events
						windowResult := processor.emitFn(windowEndTime, windowEvents)
						result = append(result, windowResult...)

						// Remove the window
						delete(processor.windows, windowKey)
					}
				}
			}

			return result, nil
		},
	}

	return processor
}

// WithOffset sets an offset for the window
func (p *WindowProcessor) WithOffset(offset time.Duration) *WindowProcessor {
	p.windowOffset = offset
	return p
}

// AggregateProcessor maintains state and aggregates events
type AggregateProcessor struct {
	ProcessorImpl
	state       map[string]interface{}
	aggregateFn func(map[string]interface{}, Event) (map[string]interface{}, []Event, error)
}

// NewAggregateProcessor creates a processor that aggregates events with state
func NewAggregateProcessor(
	aggregateFn func(map[string]interface{}, Event) (map[string]interface{}, []Event, error),
) *AggregateProcessor {
	processor := &AggregateProcessor{
		state:       make(map[string]interface{}),
		aggregateFn: aggregateFn,
	}

	processor.ProcessorImpl = ProcessorImpl{
		name: "aggregate",
		processFn: func(event Event) ([]Event, error) {
			var err error
			processor.state, _, err = processor.aggregateFn(processor.state, event)
			if err != nil {
				return nil, fmt.Errorf("error in aggregate processor: %w", err)
			}

			// Extract key from event for keyed aggregation
			key := fmt.Sprintf("%v", event.GetField("key"))
			if key == "<nil>" || key == "" {
				key = "default"
			}

			var result []Event
			processor.state, result, err = processor.aggregateFn(processor.state, event)
			if err != nil {
				return nil, fmt.Errorf("error in aggregate processor: %w", err)
			}

			return result, nil
		},
	}

	return processor
}

// GetState returns the current state of the aggregator
func (p *AggregateProcessor) GetState() map[string]interface{} {
	return p.state
}

// JoinProcessor joins events from two sources
type JoinProcessor struct {
	ProcessorImpl
	leftEvents  map[string]Event
	rightEvents map[string]Event
	keyFn       func(Event) string
	joinFn      func(Event, Event) (Event, error)
}

// NewJoinProcessor creates a processor that joins events from different sources
func NewJoinProcessor(
	keyFn func(Event) string,
	joinFn func(Event, Event) (Event, error),
) *JoinProcessor {
	processor := &JoinProcessor{
		leftEvents:  make(map[string]Event),
		rightEvents: make(map[string]Event),
		keyFn:       keyFn,
		joinFn:      joinFn,
	}

	processor.ProcessorImpl = ProcessorImpl{
		name: "join",
		processFn: func(event Event) ([]Event, error) {
			// Determine if this is from left or right based on a marker field
			isLeft := event.GetField("__source") == "left"
			key := processor.keyFn(event)

			var result []Event

			if isLeft {
				// Store in left events
				processor.leftEvents[key] = event

				// Check if there's a matching right event
				if rightEvent, exists := processor.rightEvents[key]; exists {
					// Join the events
					joined, err := processor.joinFn(event, rightEvent)
					if err != nil {
						return nil, fmt.Errorf("error joining events: %w", err)
					}

					if joined != nil {
						result = append(result, joined)
					}
				}
			} else {
				// Store in right events
				processor.rightEvents[key] = event

				// Check if there's a matching left event
				if leftEvent, exists := processor.leftEvents[key]; exists {
					// Join the events
					joined, err := processor.joinFn(leftEvent, event)
					if err != nil {
						return nil, fmt.Errorf("error joining events: %w", err)
					}

					if joined != nil {
						result = append(result, joined)
					}
				}
			}

			return result, nil
		},
	}

	return processor
}

// LoggingProcessor logs events as they pass through
type LoggingProcessor struct {
	ProcessorImpl
	logPrefix string
	logFn     func(string, Event)
}

// NewLoggingProcessor creates a processor that logs events
func NewLoggingProcessor(logPrefix string) *LoggingProcessor {
	processor := &LoggingProcessor{
		logPrefix: logPrefix,
		logFn: func(prefix string, event Event) {
			log.Printf("%s: Event %s at %s with %d fields",
				prefix,
				event.GetID(),
				event.GetTimestamp().Format(time.RFC3339),
				len(event.GetAllFields()),
			)
		},
	}

	processor.ProcessorImpl = ProcessorImpl{
		name: "logging",
		processFn: func(event Event) ([]Event, error) {
			processor.logFn(processor.logPrefix, event)
			return []Event{event}, nil
		},
	}

	return processor
}

// WithLogFunction sets a custom logging function
func (p *LoggingProcessor) WithLogFunction(logFn func(string, Event)) *LoggingProcessor {
	p.logFn = logFn
	return p
}
