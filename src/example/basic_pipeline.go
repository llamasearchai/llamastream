package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/llamastream/llamastream"
)

func main() {
	// Create a pipeline
	pipeline := llamastream.NewPipeline("basic-pipeline")

	// Create a source that generates random events
	source := llamastream.NewGeneratorSource(generateEvent, 500*time.Millisecond)

	// Add a filter processor to only keep events with score > 0.7
	filterProcessor := llamastream.NewFilterProcessor(func(event llamastream.Event) bool {
		score, _ := event.GetField("score").(float64)
		return score > 0.7
	})

	// Add a map processor to add a "processed" field
	mapProcessor := llamastream.NewMapProcessor(func(event llamastream.Event) llamastream.Event {
		event.SetField("processed", true)
		event.SetField("processed_at", time.Now())
		return event
	})

	// Add a logging processor to log events
	loggingProcessor := llamastream.NewLoggingProcessor("High Score Event")

	// Add a window processor to group events in 5-second windows
	windowProcessor := llamastream.NewWindowProcessor(
		5*time.Second,
		func(windowEnd time.Time, events []llamastream.Event) []llamastream.Event {
			// Calculate average score for the window
			var totalScore float64
			for _, event := range events {
				score, _ := event.GetField("score").(float64)
				totalScore += score
			}

			avgScore := 0.0
			if len(events) > 0 {
				avgScore = totalScore / float64(len(events))
			}

			// Create a summary event
			summary := llamastream.NewEvent(
				fmt.Sprintf("window-summary-%s", windowEnd.Format(time.RFC3339)),
				map[string]interface{}{
					"window_end":    windowEnd,
					"event_count":   len(events),
					"average_score": avgScore,
				},
			)

			return []llamastream.Event{summary}
		},
	)

	// Create a console sink for visualization
	consoleSink := llamastream.NewConsoleSink()

	// Create a file sink for persistence
	fileSink := llamastream.NewFileSink("events.json")

	// Build the pipeline
	pipeline.
		AddSource(source).
		AddProcessor(filterProcessor).
		AddProcessor(mapProcessor).
		AddProcessor(loggingProcessor).
		AddProcessor(windowProcessor).
		AddSink(consoleSink).
		AddSink(fileSink)

	// Create metrics for the pipeline
	metrics := llamastream.NewMetrics()
	pipeline.WithMetrics(metrics)

	// Run the pipeline for 30 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Periodically print metrics
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				printMetrics(metrics)
			}
		}
	}()

	log.Println("Starting pipeline...")
	if err := pipeline.Start(ctx); err != nil {
		log.Fatalf("Failed to start pipeline: %v", err)
	}

	<-ctx.Done()
	log.Println("Stopping pipeline...")

	if err := pipeline.Stop(); err != nil {
		log.Printf("Error stopping pipeline: %v", err)
	}

	log.Println("Pipeline complete")
}

// generateEvent creates a random event for the generator source
func generateEvent(ctx context.Context) llamastream.Event {
	// Generate a random score between 0 and 1
	score := rand.Float64()

	// Create event with random data
	return llamastream.NewEvent(
		fmt.Sprintf("event-%d", time.Now().UnixNano()),
		map[string]interface{}{
			"score":      score,
			"timestamp":  time.Now(),
			"category":   randomCategory(),
			"is_valid":   score > 0.3,
			"importance": randomImportance(score),
		},
	)
}

// randomCategory returns a random category
func randomCategory() string {
	categories := []string{"sports", "news", "entertainment", "technology", "science"}
	return categories[rand.Intn(len(categories))]
}

// randomImportance returns an importance level based on score
func randomImportance(score float64) string {
	if score > 0.8 {
		return "high"
	} else if score > 0.5 {
		return "medium"
	}
	return "low"
}

// printMetrics prints the current pipeline metrics
func printMetrics(metrics llamastream.Metrics) {
	fmt.Println("\n--- Pipeline Metrics ---")
	data := metrics.GetMetrics()

	// Print processed events
	fmt.Printf("Total events processed: %v\n", data["total:processed"])

	// Print event rate
	if rate, ok := data["rate:events_per_second"]; ok {
		fmt.Printf("Events per second: %.2f\n", rate)
	}

	// Print latency if available
	if avg, ok := data["latency:processor:avg"]; ok {
		fmt.Printf("Average processing latency: %v\n", avg)
	}

	fmt.Println("------------------------\n")
}
