package llamastream

import (
	"fmt"
	"sync"
	"time"
)

// Version of the llamastream package
const Version = "0.1.0"

// Event represents a data event in the stream
type Event interface {
	GetID() string
	GetTimestamp() time.Time
	GetField(name string) interface{}
	SetField(name string, value interface{})
	GetAllFields() map[string]interface{}
}

// Source is a stream data source
type Source interface {
	Start() error
	Stop() error
	Events() <-chan Event
}

// Processor transforms, filters or aggregates events
type Processor interface {
	Process(event Event) ([]Event, error)
}

// Sink is a destination for processed events
type Sink interface {
	Write(events []Event) error
	Flush() error
	Close() error
}

// StreamProcessor is the main entry point for the llamastream package
type StreamProcessor struct {
	sources    []Source
	processors []Processor
	sinks      []Sink
	wg         sync.WaitGroup
	stopChan   chan struct{}
}

// NewProcessor creates a new StreamProcessor
func NewProcessor() *StreamProcessor {
	return &StreamProcessor{
		sources:    make([]Source, 0),
		processors: make([]Processor, 0),
		sinks:      make([]Sink, 0),
		stopChan:   make(chan struct{}),
	}
}

// AddSource adds a source to the processor
func (p *StreamProcessor) AddSource(source Source) {
	p.sources = append(p.sources, source)
}

// AddProcessor adds a processor to the pipeline
func (p *StreamProcessor) AddProcessor(processor Processor) {
	p.processors = append(p.processors, processor)
}

// AddSink adds a sink to the processor
func (p *StreamProcessor) AddSink(sink Sink) {
	p.sinks = append(p.sinks, sink)
}

// Start begins processing data from all sources
func (p *StreamProcessor) Start() error {
	for _, source := range p.sources {
		if err := source.Start(); err != nil {
			return fmt.Errorf("failed to start source: %w", err)
		}

		p.wg.Add(1)
		go p.processSource(source)
	}

	return nil
}

// Stop gracefully shuts down the processor
func (p *StreamProcessor) Stop() error {
	close(p.stopChan)

	for _, source := range p.sources {
		if err := source.Stop(); err != nil {
			return fmt.Errorf("failed to stop source: %w", err)
		}
	}

	p.wg.Wait()

	for _, sink := range p.sinks {
		if err := sink.Flush(); err != nil {
			return fmt.Errorf("failed to flush sink: %w", err)
		}

		if err := sink.Close(); err != nil {
			return fmt.Errorf("failed to close sink: %w", err)
		}
	}

	return nil
}

// processSource handles events from a single source
func (p *StreamProcessor) processSource(source Source) {
	defer p.wg.Done()

	events := source.Events()

	for {
		select {
		case <-p.stopChan:
			return
		case event, ok := <-events:
			if !ok {
				return
			}

			processedEvents := []Event{event}

			// Apply all processors in sequence
			for _, processor := range p.processors {
				var newEvents []Event

				for _, e := range processedEvents {
					output, err := processor.Process(e)
					if err != nil {
						// Handle error - could log or send to error sink
						continue
					}

					newEvents = append(newEvents, output...)
				}

				processedEvents = newEvents
			}

			// Send to all sinks
			for _, sink := range p.sinks {
				if err := sink.Write(processedEvents); err != nil {
					// Handle error - could log or retry
				}
			}
		}
	}
}
