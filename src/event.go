package llamastream

import (
	"context"
	"sync"
	"time"
)

// BasicEvent implements the Event interface
type BasicEvent struct {
	id        string
	timestamp time.Time
	fields    map[string]interface{}
	mu        sync.RWMutex
}

// NewEvent creates a new event with the given ID and fields
func NewEvent(id string, fields map[string]interface{}) *BasicEvent {
	if fields == nil {
		fields = make(map[string]interface{})
	}

	return &BasicEvent{
		id:        id,
		timestamp: time.Now(),
		fields:    fields,
	}
}

// GetID returns the event's unique identifier
func (e *BasicEvent) GetID() string {
	return e.id
}

// GetTimestamp returns when the event occurred
func (e *BasicEvent) GetTimestamp() time.Time {
	return e.timestamp
}

// GetField returns a specific field value from the event
func (e *BasicEvent) GetField(key string) interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.fields[key]
}

// SetField sets a field value on the event
func (e *BasicEvent) SetField(key string, value interface{}) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.fields[key] = value
}

// GetAllFields returns all fields in the event
func (e *BasicEvent) GetAllFields() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Create a copy to avoid concurrent modification issues
	result := make(map[string]interface{}, len(e.fields))
	for k, v := range e.fields {
		result[k] = v
	}

	return result
}

// FilteredSourceImpl implements the FilteredSource interface
type FilteredSourceImpl struct {
	source Source
	filter func(Event) bool
	sinks  []Sink
}

// NewFilteredSource creates a filtered source from another source
func NewFilteredSource(source Source, filter func(Event) bool) *FilteredSourceImpl {
	return &FilteredSourceImpl{
		source: source,
		filter: filter,
		sinks:  make([]Sink, 0),
	}
}

// Connect connects a sink to this source
func (fs *FilteredSourceImpl) Connect(sink Sink) Source {
	fs.sinks = append(fs.sinks, sink)
	return fs
}

// Start begins producing filtered events to connected sinks
func (fs *FilteredSourceImpl) Start(ctx context.Context) error {
	// Create a filtering sink to connect to the source
	filterSink := &filterSink{
		filter: fs.filter,
		sinks:  fs.sinks,
	}

	// Connect the source to our filtering sink
	fs.source.Connect(filterSink)

	// Start the source
	return fs.source.Start(ctx)
}

// Stop halts the flow of events
func (fs *FilteredSourceImpl) Stop() error {
	return fs.source.Stop()
}

// filterSink is an internal sink that filters events before passing them to real sinks
type filterSink struct {
	filter func(Event) bool
	sinks  []Sink
}

// Write filters events and writes them to the real sinks
func (s *filterSink) Write(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	// Filter events
	filtered := make([]Event, 0, len(events))
	for _, event := range events {
		if s.filter(event) {
			filtered = append(filtered, event)
		}
	}

	// If no events remain after filtering, we're done
	if len(filtered) == 0 {
		return nil
	}

	// Write to all sinks
	for _, sink := range s.sinks {
		if err := sink.Write(ctx, filtered); err != nil {
			return err
		}
	}

	return nil
}

// Close is a no-op for the filter sink
func (s *filterSink) Close() error {
	return nil
}

// MappedSourceImpl implements the MappedSource interface
type MappedSourceImpl struct {
	source Source
	mapper func(Event) Event
	sinks  []Sink
}

// NewMappedSource creates a mapped source from another source
func NewMappedSource(source Source, mapper func(Event) Event) *MappedSourceImpl {
	return &MappedSourceImpl{
		source: source,
		mapper: mapper,
		sinks:  make([]Sink, 0),
	}
}

// Connect connects a sink to this source
func (ms *MappedSourceImpl) Connect(sink Sink) Source {
	ms.sinks = append(ms.sinks, sink)
	return ms
}

// Start begins producing mapped events to connected sinks
func (ms *MappedSourceImpl) Start(ctx context.Context) error {
	// Create a mapping sink to connect to the source
	mapSink := &mapSink{
		mapper: ms.mapper,
		sinks:  ms.sinks,
	}

	// Connect the source to our mapping sink
	ms.source.Connect(mapSink)

	// Start the source
	return ms.source.Start(ctx)
}

// Stop halts the flow of events
func (ms *MappedSourceImpl) Stop() error {
	return ms.source.Stop()
}

// mapSink is an internal sink that maps events before passing them to real sinks
type mapSink struct {
	mapper func(Event) Event
	sinks  []Sink
}

// Write maps events and writes them to the real sinks
func (s *mapSink) Write(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	// Map events
	mapped := make([]Event, 0, len(events))
	for _, event := range events {
		result := s.mapper(event)
		if result != nil {
			mapped = append(mapped, result)
		}
	}

	// If no events remain after mapping, we're done
	if len(mapped) == 0 {
		return nil
	}

	// Write to all sinks
	for _, sink := range s.sinks {
		if err := sink.Write(ctx, mapped); err != nil {
			return err
		}
	}

	return nil
}

// Close is a no-op for the map sink
func (s *mapSink) Close() error {
	return nil
}
