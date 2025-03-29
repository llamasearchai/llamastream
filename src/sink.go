package llamastream

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

// BaseSink provides common functionality for sink implementations
type BaseSink struct {
	name string
	// Channel for receiving events to process
	eventCh chan Event
	// Done signal
	doneCh chan struct{}
	// Number of worker goroutines
	workers int
}

// NewBaseSink creates a new base sink with default configuration
func NewBaseSink(name string) BaseSink {
	return BaseSink{
		name:    name,
		eventCh: make(chan Event, 1000),
		doneCh:  make(chan struct{}),
		workers: 1,
	}
}

// WithWorkers sets the number of worker goroutines for the sink
func (b BaseSink) WithWorkers(workers int) BaseSink {
	b.workers = workers
	return b
}

// ConsoleSink prints events to the console
type ConsoleSink struct {
	BaseSink
}

// NewConsoleSink creates a new sink that prints events to stdout
func NewConsoleSink() *ConsoleSink {
	return &ConsoleSink{
		BaseSink: NewBaseSink("console"),
	}
}

// Write sends events to the console sink
func (s *ConsoleSink) Write(ctx context.Context, events []Event) error {
	for _, event := range events {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.eventCh <- event:
			// Event sent to channel
		}
	}
	return nil
}

// Start begins processing events
func (s *ConsoleSink) Start() error {
	for i := 0; i < s.workers; i++ {
		go func() {
			for {
				select {
				case <-s.doneCh:
					return
				case event := <-s.eventCh:
					fields, err := json.MarshalIndent(event.GetAllFields(), "", "  ")
					if err != nil {
						log.Printf("Error marshaling event: %v", err)
						continue
					}
					fmt.Printf("Event ID: %s\nTimestamp: %s\nFields: %s\n\n",
						event.GetID(),
						event.GetTimestamp().Format(time.RFC3339),
						string(fields),
					)
				}
			}
		}()
	}
	return nil
}

// Close stops processing events
func (s *ConsoleSink) Close() error {
	close(s.doneCh)
	return nil
}

// FileSink writes events to a file
type FileSink struct {
	BaseSink
	filePath   string
	file       *os.File
	jsonFormat bool
}

// NewFileSink creates a new sink that writes events to a file
func NewFileSink(filePath string) *FileSink {
	return &FileSink{
		BaseSink:   NewBaseSink("file"),
		filePath:   filePath,
		jsonFormat: true,
	}
}

// WithTextFormat configures the sink to write events in plain text
func (s *FileSink) WithTextFormat() *FileSink {
	s.jsonFormat = false
	return s
}

// WithJSONFormat configures the sink to write events in JSON format
func (s *FileSink) WithJSONFormat() *FileSink {
	s.jsonFormat = true
	return s
}

// Write sends events to the file sink
func (s *FileSink) Write(ctx context.Context, events []Event) error {
	for _, event := range events {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.eventCh <- event:
			// Event sent to channel
		}
	}
	return nil
}

// Start begins processing events
func (s *FileSink) Start() error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(s.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Open file
	file, err := os.OpenFile(s.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", s.filePath, err)
	}
	s.file = file

	for i := 0; i < s.workers; i++ {
		go func() {
			for {
				select {
				case <-s.doneCh:
					return
				case event := <-s.eventCh:
					if s.jsonFormat {
						data := map[string]interface{}{
							"id":        event.GetID(),
							"timestamp": event.GetTimestamp().Format(time.RFC3339),
							"fields":    event.GetAllFields(),
						}
						bytes, err := json.Marshal(data)
						if err != nil {
							log.Printf("Error marshaling event: %v", err)
							continue
						}
						if _, err := s.file.Write(append(bytes, '\n')); err != nil {
							log.Printf("Error writing to file: %v", err)
						}
					} else {
						fields, err := json.Marshal(event.GetAllFields())
						if err != nil {
							log.Printf("Error marshaling event fields: %v", err)
							continue
						}
						if _, err := fmt.Fprintf(s.file, "ID: %s, Time: %s, Fields: %s\n",
							event.GetID(),
							event.GetTimestamp().Format(time.RFC3339),
							string(fields),
						); err != nil {
							log.Printf("Error writing to file: %v", err)
						}
					}
				}
			}
		}()
	}
	return nil
}

// Close stops processing events and closes the file
func (s *FileSink) Close() error {
	close(s.doneCh)
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

// MemorySink stores events in memory for testing
type MemorySink struct {
	BaseSink
	events []Event
}

// NewMemorySink creates a new sink that stores events in memory
func NewMemorySink() *MemorySink {
	return &MemorySink{
		BaseSink: NewBaseSink("memory"),
		events:   make([]Event, 0),
	}
}

// Write sends events to the memory sink
func (s *MemorySink) Write(ctx context.Context, events []Event) error {
	for _, event := range events {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.eventCh <- event:
			// Event sent to channel
		}
	}
	return nil
}

// Start begins processing events
func (s *MemorySink) Start() error {
	for i := 0; i < s.workers; i++ {
		go func() {
			for {
				select {
				case <-s.doneCh:
					return
				case event := <-s.eventCh:
					s.events = append(s.events, event)
				}
			}
		}()
	}
	return nil
}

// Close stops processing events
func (s *MemorySink) Close() error {
	close(s.doneCh)
	return nil
}

// GetEvents returns all events stored in the sink
func (s *MemorySink) GetEvents() []Event {
	return s.events
}

// Clear removes all events from the sink
func (s *MemorySink) Clear() {
	s.events = make([]Event, 0)
}

// KafkaSink writes events to a Kafka topic
type KafkaSink struct {
	BaseSink
	brokers []string
	topic   string
	// The actual Kafka producer would be initialized here
}

// NewKafkaSink creates a new sink that writes events to Kafka
func NewKafkaSink(brokers []string, topic string) *KafkaSink {
	return &KafkaSink{
		BaseSink: NewBaseSink("kafka"),
		brokers:  brokers,
		topic:    topic,
	}
}

// Write sends events to the Kafka sink
func (s *KafkaSink) Write(ctx context.Context, events []Event) error {
	for _, event := range events {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.eventCh <- event:
			// Event sent to channel
		}
	}
	return nil
}

// Start begins processing events
func (s *KafkaSink) Start() error {
	// Initialize Kafka producer here
	// This is a placeholder implementation
	log.Printf("Starting Kafka sink with brokers %v and topic %s", s.brokers, s.topic)

	for i := 0; i < s.workers; i++ {
		go func() {
			for {
				select {
				case <-s.doneCh:
					return
				case event := <-s.eventCh:
					// Serialize the event
					data, err := json.Marshal(map[string]interface{}{
						"id":        event.GetID(),
						"timestamp": event.GetTimestamp().Format(time.RFC3339),
						"fields":    event.GetAllFields(),
					})
					if err != nil {
						log.Printf("Error marshaling event: %v", err)
						continue
					}

					// In a real implementation, send to Kafka here
					log.Printf("Would send to Kafka topic %s: %s", s.topic, string(data))
				}
			}
		}()
	}
	return nil
}

// Close stops processing events
func (s *KafkaSink) Close() error {
	close(s.doneCh)
	// Close Kafka producer here
	return nil
}

// RedisSink writes events to a Redis list or pub/sub channel
type RedisSink struct {
	BaseSink
	address  string
	key      string
	password string
	db       int
	useList  bool
}

// NewRedisSink creates a new sink that writes events to Redis
func NewRedisSink(address string, key string) *RedisSink {
	return &RedisSink{
		BaseSink: NewBaseSink("redis"),
		address:  address,
		key:      key,
		db:       0,
		useList:  true,
	}
}

// WithPassword sets the Redis password
func (s *RedisSink) WithPassword(password string) *RedisSink {
	s.password = password
	return s
}

// WithDB sets the Redis database
func (s *RedisSink) WithDB(db int) *RedisSink {
	s.db = db
	return s
}

// WithPubSub configures the sink to use pub/sub
func (s *RedisSink) WithPubSub() *RedisSink {
	s.useList = false
	return s
}

// WithList configures the sink to use a list
func (s *RedisSink) WithList() *RedisSink {
	s.useList = true
	return s
}

// Write sends events to the Redis sink
func (s *RedisSink) Write(ctx context.Context, events []Event) error {
	for _, event := range events {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.eventCh <- event:
			// Event sent to channel
		}
	}
	return nil
}

// Start begins processing events
func (s *RedisSink) Start() error {
	// Initialize Redis client here
	// This is a placeholder implementation
	log.Printf("Starting Redis sink with address %s and key %s", s.address, s.key)

	for i := 0; i < s.workers; i++ {
		go func() {
			for {
				select {
				case <-s.doneCh:
					return
				case event := <-s.eventCh:
					// Serialize the event
					data, err := json.Marshal(map[string]interface{}{
						"id":        event.GetID(),
						"timestamp": event.GetTimestamp().Format(time.RFC3339),
						"fields":    event.GetAllFields(),
					})
					if err != nil {
						log.Printf("Error marshaling event: %v", err)
						continue
					}

					// In a real implementation, send to Redis here
					if s.useList {
						log.Printf("Would push to Redis list %s: %s", s.key, string(data))
					} else {
						log.Printf("Would publish to Redis channel %s: %s", s.key, string(data))
					}
				}
			}
		}()
	}
	return nil
}

// Close stops processing events
func (s *RedisSink) Close() error {
	close(s.doneCh)
	// Close Redis client here
	return nil
}
