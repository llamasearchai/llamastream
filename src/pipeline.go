package llamastream

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// PipelineImpl implements the Pipeline interface
type PipelineImpl struct {
	name       string
	sources    []Source
	processors []Processor
	sinks      []Sink
	metrics    Metrics
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	running    bool
}

// NewPipeline creates a new pipeline with the given name
func NewPipeline(name string) *PipelineImpl {
	return &PipelineImpl{
		name:       name,
		sources:    make([]Source, 0),
		processors: make([]Processor, 0),
		sinks:      make([]Sink, 0),
	}
}

// WithMetrics sets the metrics collector for the pipeline
func (p *PipelineImpl) WithMetrics(metrics Metrics) *PipelineImpl {
	p.metrics = metrics
	return p
}

// AddSource adds a source to the pipeline
func (p *PipelineImpl) AddSource(source Source) Pipeline {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		log.Printf("Warning: Adding source to running pipeline %s", p.name)
	}

	p.sources = append(p.sources, source)
	return p
}

// AddProcessor adds a processor to the pipeline
func (p *PipelineImpl) AddProcessor(processor Processor) Pipeline {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		log.Printf("Warning: Adding processor to running pipeline %s", p.name)
	}

	p.processors = append(p.processors, processor)
	return p
}

// AddSink adds a sink to the pipeline
func (p *PipelineImpl) AddSink(sink Sink) Pipeline {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		log.Printf("Warning: Adding sink to running pipeline %s", p.name)
	}

	p.sinks = append(p.sinks, sink)
	return p
}

// Start begins the flow of events through the pipeline
func (p *PipelineImpl) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return errors.New("pipeline already running")
	}

	p.ctx, p.cancel = context.WithCancel(ctx)
	p.running = true

	// Start sinks first
	for _, sink := range p.sinks {
		if starter, ok := sink.(interface{ Start() error }); ok {
			if err := starter.Start(); err != nil {
				p.mu.Unlock()
				return fmt.Errorf("failed to start sink: %w", err)
			}
		}
	}

	// Create a processor sink that applies processors to events
	processorSink := &processorSink{
		processors: p.processors,
		sinks:      p.sinks,
		metrics:    p.metrics,
	}

	// Connect sources to the processor sink
	for _, source := range p.sources {
		source.Connect(processorSink)
	}

	// Start sources
	for i, source := range p.sources {
		p.wg.Add(1)
		go func(idx int, src Source) {
			defer p.wg.Done()
			sourceCtx, cancel := context.WithCancel(p.ctx)
			defer cancel()

			if err := src.Start(sourceCtx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("Error starting source %d in pipeline %s: %v", idx, p.name, err)
			}
		}(i, source)
	}

	p.mu.Unlock()
	log.Printf("Pipeline %s started with %d sources, %d processors, and %d sinks",
		p.name, len(p.sources), len(p.processors), len(p.sinks))

	return nil
}

// Stop halts the flow of events through the pipeline
func (p *PipelineImpl) Stop() error {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return errors.New("pipeline not running")
	}

	p.running = false
	p.cancel()
	p.mu.Unlock()

	// Wait for sources to stop
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Sources stopped cleanly
	case <-time.After(5 * time.Second):
		log.Printf("Timeout waiting for pipeline %s sources to stop", p.name)
	}

	// Stop all sources
	for _, source := range p.sources {
		if err := source.Stop(); err != nil {
			log.Printf("Error stopping source in pipeline %s: %v", p.name, err)
		}
	}

	// Close all sinks
	for _, sink := range p.sinks {
		if err := sink.Close(); err != nil {
			log.Printf("Error closing sink in pipeline %s: %v", p.name, err)
		}
	}

	log.Printf("Pipeline %s stopped", p.name)
	return nil
}

// processorSink is an internal sink that applies processors to events before sending them to real sinks
type processorSink struct {
	processors []Processor
	sinks      []Sink
	metrics    Metrics
}

// Write processes events and writes them to the real sinks
func (p *processorSink) Write(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	processedEvents := events

	// Apply each processor in sequence
	for _, processor := range p.processors {
		nextEvents := make([]Event, 0, len(processedEvents))

		for _, event := range processedEvents {
			// Record metrics if available
			startTime := time.Now()

			// Process the event
			results, err := processor.Process(event)
			if err != nil {
				// Log the error and continue with the next event
				log.Printf("Error processing event: %v", err)
				continue
			}

			// Record processing latency
			if p.metrics != nil {
				p.metrics.RecordLatency("processor", time.Since(startTime))
			}

			// Add results to the next batch
			nextEvents = append(nextEvents, results...)
		}

		// Update processed events for the next processor
		processedEvents = nextEvents
	}

	// If no events remain after processing, we're done
	if len(processedEvents) == 0 {
		return nil
	}

	// Write to all sinks
	for _, sink := range p.sinks {
		if err := sink.Write(ctx, processedEvents); err != nil {
			log.Printf("Error writing to sink: %v", err)
			// Continue with other sinks even if one fails
		}
	}

	return nil
}

// Close is a no-op for the processor sink
func (p *processorSink) Close() error {
	return nil
}

// SchedulerImpl implements the Scheduler interface
type SchedulerImpl struct {
	schedules map[string]*scheduleEntry
	wg        sync.WaitGroup
	mu        sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	running   bool
}

// scheduleEntry represents a pipeline scheduled to run on a cron expression
type scheduleEntry struct {
	pipeline  Pipeline
	cronExpr  string
	nextRun   time.Time
	interval  time.Duration // For simple interval-based scheduling
	running   bool
	cancel    context.CancelFunc
	runCount  int64
	lastError error
}

// NewScheduler creates a new scheduler
func NewScheduler() *SchedulerImpl {
	ctx, cancel := context.WithCancel(context.Background())
	return &SchedulerImpl{
		schedules: make(map[string]*scheduleEntry),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Schedule adds a pipeline to run on the given cron expression
func (s *SchedulerImpl) Schedule(pipeline Pipeline, cronExpr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// For simplicity, we'll just use a duration-based schedule in this example
	// A real implementation would parse the cron expression
	var interval time.Duration

	// Very simple "cron" format parsing - just for intervals
	switch cronExpr {
	case "@hourly":
		interval = time.Hour
	case "@daily":
		interval = 24 * time.Hour
	case "@weekly":
		interval = 7 * 24 * time.Hour
	case "@monthly":
		interval = 30 * 24 * time.Hour
	default:
		// Try to parse as duration
		var err error
		interval, err = time.ParseDuration(cronExpr)
		if err != nil {
			return fmt.Errorf("unsupported cron expression: %s", cronExpr)
		}
	}

	pipelineImpl, ok := pipeline.(*PipelineImpl)
	if !ok {
		return errors.New("unsupported pipeline implementation")
	}

	s.schedules[pipelineImpl.name] = &scheduleEntry{
		pipeline: pipeline,
		cronExpr: cronExpr,
		nextRun:  time.Now().Add(interval),
		interval: interval,
	}

	log.Printf("Scheduled pipeline %s to run with interval %v", pipelineImpl.name, interval)
	return nil
}

// Start begins executing scheduled pipelines
func (s *SchedulerImpl) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return errors.New("scheduler already running")
	}

	s.running = true
	s.mu.Unlock()

	// Start the scheduler loop
	s.wg.Add(1)
	go s.loop()

	log.Printf("Scheduler started with %d scheduled pipelines", len(s.schedules))
	return nil
}

// loop is the main scheduler loop
func (s *SchedulerImpl) loop() {
	defer s.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case t := <-ticker.C:
			s.checkSchedules(t)
		}
	}
}

// checkSchedules checks if any schedules need to run
func (s *SchedulerImpl) checkSchedules(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for name, entry := range s.schedules {
		if !entry.running && t.After(entry.nextRun) {
			// Time to run this pipeline
			ctx, cancel := context.WithCancel(s.ctx)
			entry.cancel = cancel
			entry.running = true

			s.wg.Add(1)
			go func(name string, entry *scheduleEntry) {
				defer s.wg.Done()
				defer func() {
					s.mu.Lock()
					entry.running = false
					entry.nextRun = time.Now().Add(entry.interval)
					entry.runCount++
					s.mu.Unlock()
				}()

				log.Printf("Starting scheduled run of pipeline %s", name)
				err := entry.pipeline.Start(ctx)
				if err != nil {
					log.Printf("Error starting scheduled pipeline %s: %v", name, err)
					entry.lastError = err
					return
				}

				// Let it run for a while
				time.Sleep(10 * time.Second)

				// Stop the pipeline
				err = entry.pipeline.Stop()
				if err != nil {
					log.Printf("Error stopping scheduled pipeline %s: %v", name, err)
					entry.lastError = err
				} else {
					log.Printf("Completed scheduled run of pipeline %s", name)
					entry.lastError = nil
				}
			}(name, entry)
		}
	}
}

// Stop halts all scheduled executions
func (s *SchedulerImpl) Stop() error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return errors.New("scheduler not running")
	}

	s.running = false
	s.cancel()

	// Cancel all running pipelines
	for _, entry := range s.schedules {
		if entry.running && entry.cancel != nil {
			entry.cancel()
		}
	}
	s.mu.Unlock()

	// Wait for all pipelines to stop
	s.wg.Wait()
	log.Printf("Scheduler stopped")
	return nil
}

// GetStatus returns the status of all scheduled pipelines
func (s *SchedulerImpl) GetStatus() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	status := make(map[string]interface{})

	for name, entry := range s.schedules {
		pipelineStatus := map[string]interface{}{
			"running":   entry.running,
			"next_run":  entry.nextRun.Format(time.RFC3339),
			"run_count": entry.runCount,
		}

		if entry.lastError != nil {
			pipelineStatus["last_error"] = entry.lastError.Error()
		}

		status[name] = pipelineStatus
	}

	return status
}
