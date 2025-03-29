package llamastream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

// EventSource is an interface that implements a specific source type
type EventSource interface {
	Source
	Filter(filterFunc func(Event) bool) FilteredSource
	Map(mapFunc func(Event) Event) MappedSource
	To(sink Sink) error
}

// BaseSource provides common functionality for source implementations
type BaseSource struct {
	name      string
	sinks     []Sink
	mu        sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	isRunning bool
}

// NewBaseSource creates a new base source with the given name
func NewBaseSource(name string) BaseSource {
	return BaseSource{
		name:  name,
		sinks: make([]Sink, 0),
	}
}

// Connect attaches a sink to this source
func (bs *BaseSource) Connect(sink Sink) Source {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	bs.sinks = append(bs.sinks, sink)
	return bs
}

// Stop halts the flow of events
func (bs *BaseSource) Stop() error {
	bs.mu.Lock()
	if !bs.isRunning {
		bs.mu.Unlock()
		return errors.New("source not running")
	}

	if bs.cancel != nil {
		bs.cancel()
	}
	bs.isRunning = false
	bs.mu.Unlock()

	// Wait for all goroutines to finish
	bs.wg.Wait()

	log.Printf("Source %s stopped", bs.name)
	return nil
}

// sendEvents sends events to all connected sinks
func (bs *BaseSource) sendEvents(ctx context.Context, events []Event) error {
	for _, sink := range bs.sinks {
		if err := sink.Write(ctx, events); err != nil {
			return err
		}
	}
	return nil
}

// GeneratorSource generates events using a generator function
type GeneratorSource struct {
	BaseSource
	generator func(context.Context) Event
	interval  time.Duration
}

// NewGeneratorSource creates a source that generates events using a function
func NewGeneratorSource(generator func(context.Context) Event, interval time.Duration) *GeneratorSource {
	return &GeneratorSource{
		BaseSource: NewBaseSource("generator"),
		generator:  generator,
		interval:   interval,
	}
}

// Start begins generating events to connected sinks
func (s *GeneratorSource) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return errors.New("source already running")
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.isRunning = true
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				event := s.generator(s.ctx)
				if event != nil {
					if err := s.sendEvents(s.ctx, []Event{event}); err != nil {
						log.Printf("Error sending event: %v", err)
					}
				}
			}
		}
	}()

	log.Printf("Generator source started with interval %v", s.interval)
	return nil
}

// FileSource reads events from a file
type FileSource struct {
	BaseSource
	filePath   string
	batchSize  int
	jsonFormat bool
}

// NewFileSource creates a source that reads events from a file
func NewFileSource(filePath string) *FileSource {
	return &FileSource{
		BaseSource: NewBaseSource("file"),
		filePath:   filePath,
		batchSize:  100,
		jsonFormat: true,
	}
}

// WithBatchSize sets the number of events to read at once
func (s *FileSource) WithBatchSize(batchSize int) *FileSource {
	s.batchSize = batchSize
	return s
}

// WithTextFormat configures the source to read plain text (one event per line)
func (s *FileSource) WithTextFormat() *FileSource {
	s.jsonFormat = false
	return s
}

// WithJSONFormat configures the source to read JSON (one JSON object per line)
func (s *FileSource) WithJSONFormat() *FileSource {
	s.jsonFormat = true
	return s
}

// Start begins reading events from the file
func (s *FileSource) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return errors.New("source already running")
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.isRunning = true
	s.mu.Unlock()

	// Open file
	file, err := os.Open(s.filePath)
	if err != nil {
		s.isRunning = false
		return fmt.Errorf("failed to open file %s: %w", s.filePath, err)
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer file.Close()

		lineNumber := 0
		scanner := NewChunkScanner(file, 8192) // 8KB buffer

		for scanner.Scan() {
			select {
			case <-s.ctx.Done():
				return
			default:
				line := scanner.Text()
				lineNumber++

				if line == "" {
					continue
				}

				var event Event
				if s.jsonFormat {
					// Parse JSON
					var data map[string]interface{}
					if err := json.Unmarshal([]byte(line), &data); err != nil {
						log.Printf("Error parsing JSON at line %d: %v", lineNumber, err)
						continue
					}

					// Extract ID and timestamp if available
					id, _ := data["id"].(string)
					if id == "" {
						id = fmt.Sprintf("line-%d", lineNumber)
					}

					// Create event
					event = NewEvent(id, data)
				} else {
					// Create event from plain text
					event = NewEvent(fmt.Sprintf("line-%d", lineNumber), map[string]interface{}{
						"line": line,
					})
				}

				// Send the event
				if err := s.sendEvents(s.ctx, []Event{event}); err != nil {
					log.Printf("Error sending event: %v", err)
				}
			}
		}

		if err := scanner.Err(); err != nil {
			log.Printf("Error reading file: %v", err)
		}

		log.Printf("Finished reading file %s", s.filePath)
	}()

	log.Printf("File source started with file %s", s.filePath)
	return nil
}

// ChunkScanner is a simple line scanner for reading files
type ChunkScanner struct {
	reader    io.Reader
	buffer    []byte
	remaining []byte
	err       error
}

// NewChunkScanner creates a new scanner
func NewChunkScanner(reader io.Reader, bufferSize int) *ChunkScanner {
	return &ChunkScanner{
		reader: reader,
		buffer: make([]byte, bufferSize),
	}
}

// Scan advances to the next line
func (s *ChunkScanner) Scan() bool {
	// If we have an error, we're done
	if s.err != nil {
		return false
	}

	// Look for a newline in the remaining data
	for i, b := range s.remaining {
		if b == '\n' {
			s.buffer = s.remaining[:i]
			s.remaining = s.remaining[i+1:]
			return true
		}
	}

	// No newline found, read more data
	var buf []byte
	if len(s.remaining) > 0 {
		buf = s.remaining
	}

	for {
		// Read more data
		newbuf := make([]byte, 4096)
		n, err := s.reader.Read(newbuf)

		if err != nil {
			if err == io.EOF {
				// End of file, return the remaining data
				if len(buf) > 0 {
					s.buffer = buf
					s.remaining = nil
					s.err = io.EOF
					return true
				}
				s.err = io.EOF
				return false
			}
			s.err = err
			return false
		}

		// Append to buffer
		buf = append(buf, newbuf[:n]...)

		// Look for a newline
		for i, b := range buf {
			if b == '\n' {
				s.buffer = buf[:i]
				s.remaining = buf[i+1:]
				return true
			}
		}

		// If buffer is too large, return what we have
		if len(buf) > 1024*1024 {
			s.buffer = buf
			s.remaining = nil
			return true
		}
	}
}

// Text returns the current line
func (s *ChunkScanner) Text() string {
	return string(s.buffer)
}

// Err returns any error that occurred during scanning
func (s *ChunkScanner) Err() error {
	if s.err == io.EOF {
		return nil
	}
	return s.err
}

// KafkaSource reads events from Kafka
type KafkaSource struct {
	BaseSource
	brokers []string
	topic   string
	group   string
	// The actual Kafka consumer would be initialized here
}

// NewKafkaSource creates a source that reads events from Kafka
func NewKafkaSource(brokers []string, topic string, group string) *KafkaSource {
	return &KafkaSource{
		BaseSource: NewBaseSource("kafka"),
		brokers:    brokers,
		topic:      topic,
		group:      group,
	}
}

// Start begins consuming events from Kafka
func (s *KafkaSource) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return errors.New("source already running")
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.isRunning = true
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		// This is a mock implementation
		log.Printf("Kafka source started with brokers %v, topic %s, group %s", s.brokers, s.topic, s.group)

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				// In a real implementation, we would consume messages from Kafka here
				// For now, just create a mock event
				event := NewEvent(fmt.Sprintf("kafka-%d", time.Now().UnixNano()), map[string]interface{}{
					"topic":     s.topic,
					"timestamp": time.Now(),
					"value":     "This is a mock Kafka message",
				})

				if err := s.sendEvents(s.ctx, []Event{event}); err != nil {
					log.Printf("Error sending event: %v", err)
				}
			}
		}
	}()

	return nil
}

// RedisSource reads events from Redis
type RedisSource struct {
	BaseSource
	address   string
	channel   string
	password  string
	db        int
	useStream bool
}

// NewRedisSource creates a source that reads events from Redis
func NewRedisSource(address string, channel string) *RedisSource {
	return &RedisSource{
		BaseSource: NewBaseSource("redis"),
		address:    address,
		channel:    channel,
		db:         0,
		useStream:  false,
	}
}

// WithPassword sets the Redis password
func (s *RedisSource) WithPassword(password string) *RedisSource {
	s.password = password
	return s
}

// WithDB sets the Redis database
func (s *RedisSource) WithDB(db int) *RedisSource {
	s.db = db
	return s
}

// WithPubSub configures the source to use pub/sub
func (s *RedisSource) WithPubSub() *RedisSource {
	s.useStream = false
	return s
}

// WithStream configures the source to use a stream
func (s *RedisSource) WithStream() *RedisSource {
	s.useStream = true
	return s
}

// Start begins consuming events from Redis
func (s *RedisSource) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return errors.New("source already running")
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.isRunning = true
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		// This is a mock implementation
		mode := "pub/sub"
		if s.useStream {
			mode = "stream"
		}
		log.Printf("Redis source started with address %s, %s %s", s.address, mode, s.channel)

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				// In a real implementation, we would consume messages from Redis here
				// For now, just create a mock event
				event := NewEvent(fmt.Sprintf("redis-%d", time.Now().UnixNano()), map[string]interface{}{
					"channel":   s.channel,
					"timestamp": time.Now(),
					"value":     "This is a mock Redis message",
				})

				if err := s.sendEvents(s.ctx, []Event{event}); err != nil {
					log.Printf("Error sending event: %v", err)
				}
			}
		}
	}()

	return nil
}

// HTTPSource receives events via HTTP POST requests
type HTTPSource struct {
	BaseSource
	port       int
	path       string
	eventCh    chan Event
	httpServer *HTTPServer
}

// NewHTTPSource creates a source that receives events via HTTP
func NewHTTPSource(port int, path string) *HTTPSource {
	if path == "" {
		path = "/events"
	}

	return &HTTPSource{
		BaseSource: NewBaseSource("http"),
		port:       port,
		path:       path,
		eventCh:    make(chan Event, 1000),
	}
}

// Start begins receiving events via HTTP
func (s *HTTPSource) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return errors.New("source already running")
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.isRunning = true

	// Create HTTP server
	s.httpServer = NewHTTPServer(s.port)

	// Register event handler
	s.httpServer.HandleFunc(s.path, func(body []byte) {
		// Parse JSON
		var data map[string]interface{}
		if err := json.Unmarshal(body, &data); err != nil {
			log.Printf("Error parsing JSON: %v", err)
			return
		}

		// Extract ID if available
		id, _ := data["id"].(string)
		if id == "" {
			id = fmt.Sprintf("http-%d", time.Now().UnixNano())
		}

		// Create event
		event := NewEvent(id, data)

		// Send event to channel
		select {
		case s.eventCh <- event:
			// Event sent
		default:
			// Channel full, drop event
			log.Printf("HTTP source channel full, dropping event")
		}
	})

	// Start server
	if err := s.httpServer.Start(); err != nil {
		s.isRunning = false
		s.mu.Unlock()
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}

	s.mu.Unlock()

	// Process events from channel
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		for {
			select {
			case <-s.ctx.Done():
				return
			case event := <-s.eventCh:
				if err := s.sendEvents(s.ctx, []Event{event}); err != nil {
					log.Printf("Error sending event: %v", err)
				}
			}
		}
	}()

	log.Printf("HTTP source started on port %d with path %s", s.port, s.path)
	return nil
}

// Stop halts the HTTP server and event processing
func (s *HTTPSource) Stop() error {
	s.mu.Lock()
	if !s.isRunning {
		s.mu.Unlock()
		return errors.New("source not running")
	}

	s.isRunning = false
	if s.cancel != nil {
		s.cancel()
	}

	// Stop HTTP server
	if s.httpServer != nil {
		s.httpServer.Stop()
	}

	s.mu.Unlock()

	// Wait for goroutines to finish
	s.wg.Wait()

	log.Printf("HTTP source stopped")
	return nil
}

// HTTPServer is a simple HTTP server for receiving events
type HTTPServer struct {
	port     int
	handlers map[string]func([]byte)
	server   interface{} // net/http.Server
}

// NewHTTPServer creates a new HTTP server
func NewHTTPServer(port int) *HTTPServer {
	return &HTTPServer{
		port:     port,
		handlers: make(map[string]func([]byte)),
	}
}

// HandleFunc registers a function to handle requests at the given path
func (s *HTTPServer) HandleFunc(path string, handler func([]byte)) {
	s.handlers[path] = handler
}

// Start begins listening for HTTP requests
func (s *HTTPServer) Start() error {
	// This is a mock implementation
	log.Printf("HTTP server started on port %d", s.port)
	return nil
}

// Stop halts the HTTP server
func (s *HTTPServer) Stop() {
	// This is a mock implementation
	log.Printf("HTTP server stopped")
}
