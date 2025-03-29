package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/llamastream/llamastream"
)

// UserEvent represents a user interaction event
type UserEvent struct {
	UserID    string    `json:"user_id"`
	EventType string    `json:"event_type"`
	Timestamp time.Time `json:"timestamp"`
	Page      string    `json:"page"`
	Duration  int       `json:"duration,omitempty"`
	Referrer  string    `json:"referrer,omitempty"`
	Browser   string    `json:"browser,omitempty"`
	Device    string    `json:"device,omitempty"`
	Country   string    `json:"country,omitempty"`
}

func main() {
	log.Println("Starting real-time analytics pipeline")

	// Create a pipeline for processing user events
	pipeline := llamastream.NewPipeline("user-analytics")

	// Create metrics collector
	metrics := llamastream.NewPrometheusMetrics()
	pipeline.WithMetrics(metrics)

	// Create an HTTP source to receive user events
	httpSource := llamastream.NewHTTPSource(8080, "/events")

	// Create a processor to filter out invalid events
	filterProcessor := llamastream.NewFilterProcessor(func(event llamastream.Event) bool {
		userID, _ := event.GetField("user_id").(string)
		eventType, _ := event.GetField("event_type").(string)
		return userID != "" && eventType != ""
	})

	// Create a processor to enrich events with additional information
	enrichProcessor := llamastream.NewMapProcessor(func(event llamastream.Event) llamastream.Event {
		// Add processing timestamp
		event.SetField("processed_at", time.Now())

		// Categorize event type
		eventType, _ := event.GetField("event_type").(string)
		switch eventType {
		case "pageview", "click", "scroll":
			event.SetField("category", "interaction")
		case "signup", "login", "logout":
			event.SetField("category", "auth")
		case "purchase", "add_to_cart", "checkout":
			event.SetField("category", "commerce")
		default:
			event.SetField("category", "other")
		}

		return event
	})

	// Create a window processor for 1-minute pageview summaries
	pageviewWindowProcessor := llamastream.NewWindowProcessor(
		1*time.Minute,
		func(windowEnd time.Time, events []llamastream.Event) []llamastream.Event {
			// Group pageviews by page
			pageviews := make(map[string]int)
			uniqueUsers := make(map[string]bool)

			for _, event := range events {
				eventType, _ := event.GetField("event_type").(string)
				if eventType == "pageview" {
					page, _ := event.GetField("page").(string)
					if page != "" {
						pageviews[page]++
					}

					userID, _ := event.GetField("user_id").(string)
					if userID != "" {
						uniqueUsers[userID] = true
					}
				}
			}

			// Create summary events
			var results []llamastream.Event

			// Overall summary
			summary := llamastream.NewEvent(
				fmt.Sprintf("pageview-summary-%s", windowEnd.Format(time.RFC3339)),
				map[string]interface{}{
					"window_end":      windowEnd,
					"window_start":    windowEnd.Add(-1 * time.Minute),
					"total_pageviews": len(events),
					"unique_users":    len(uniqueUsers),
				},
			)
			results = append(results, summary)

			// Page-specific summaries
			for page, count := range pageviews {
				pageSummary := llamastream.NewEvent(
					fmt.Sprintf("page-summary-%s-%s", page, windowEnd.Format(time.RFC3339)),
					map[string]interface{}{
						"window_end":   windowEnd,
						"window_start": windowEnd.Add(-1 * time.Minute),
						"page":         page,
						"pageviews":    count,
					},
				)
				results = append(results, pageSummary)
			}

			return results
		},
	)

	// Create a window processor for 5-minute user session analysis
	sessionWindowProcessor := llamastream.NewWindowProcessor(
		5*time.Minute,
		func(windowEnd time.Time, events []llamastream.Event) []llamastream.Event {
			// Group events by user
			userEvents := make(map[string][]llamastream.Event)
			for _, event := range events {
				userID, _ := event.GetField("user_id").(string)
				if userID != "" {
					userEvents[userID] = append(userEvents[userID], event)
				}
			}

			// Create user session summaries
			var results []llamastream.Event

			for userID, userEvts := range userEvents {
				if len(userEvts) < 2 {
					continue // Skip users with only one event
				}

				// Calculate session metrics
				eventCount := len(userEvts)
				pageviewCount := 0
				totalDuration := 0

				pages := make(map[string]bool)

				for _, e := range userEvts {
					eventType, _ := e.GetField("event_type").(string)
					if eventType == "pageview" {
						pageviewCount++
						page, _ := e.GetField("page").(string)
						if page != "" {
							pages[page] = true
						}
					}

					duration, _ := e.GetField("duration").(int)
					totalDuration += duration
				}

				// Create session summary
				sessionSummary := llamastream.NewEvent(
					fmt.Sprintf("session-summary-%s-%s", userID, windowEnd.Format(time.RFC3339)),
					map[string]interface{}{
						"window_end":     windowEnd,
						"window_start":   windowEnd.Add(-5 * time.Minute),
						"user_id":        userID,
						"event_count":    eventCount,
						"pageview_count": pageviewCount,
						"unique_pages":   len(pages),
						"total_duration": totalDuration,
						"pages_visited":  getMapKeys(pages),
					},
				)
				results = append(results, sessionSummary)
			}

			return results
		},
	)

	// Create a console sink for viewing events
	consoleSink := llamastream.NewConsoleSink()

	// Create a file sink for storing processed events
	rawEventsSink := llamastream.NewFileSink("data/raw_events.json")

	// Create a file sink for storing summaries
	summariesSink := llamastream.NewFileSink("data/summaries.json")

	// Build the analytics pipeline:
	// 1. HTTP source receives events
	// 2. Filter validates events
	// 3. Enrich adds metadata
	// 4. Branch 1: Store raw events
	// 5. Branch 2: Process pageview summaries
	// 6. Branch 3: Process session summaries
	pipeline.
		AddSource(httpSource).
		AddProcessor(filterProcessor).
		AddProcessor(enrichProcessor).
		AddSink(rawEventsSink).
		AddProcessor(pageviewWindowProcessor).
		AddProcessor(sessionWindowProcessor).
		AddSink(summariesSink).
		AddSink(consoleSink)

	// Start an HTTP server to expose metrics
	go func() {
		http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, metrics.GetPrometheusMetrics())
		})

		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprint(w, `
				<html>
					<head><title>LlamaStream Analytics</title></head>
					<body>
						<h1>LlamaStream Analytics</h1>
						<p>Send events to POST /events</p>
						<p>View metrics at <a href="/metrics">/metrics</a></p>
					</body>
				</html>
			`)
		})

		log.Println("Starting metrics server on :8081")
		if err := http.ListenAndServe(":8081", nil); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	// Create a context that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())

	// Set up signal handling
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-signals
		log.Printf("Received signal: %v", sig)
		cancel()
	}()

	// Start the pipeline
	log.Println("Starting pipeline...")
	if err := pipeline.Start(ctx); err != nil {
		log.Fatalf("Failed to start pipeline: %v", err)
	}

	// Wait for context to be cancelled
	<-ctx.Done()
	log.Println("Context cancelled, stopping pipeline...")

	// Stop the pipeline
	if err := pipeline.Stop(); err != nil {
		log.Printf("Error stopping pipeline: %v", err)
	}

	log.Println("Analytics pipeline shutdown complete")
}

// getMapKeys returns the keys of a map as a slice
func getMapKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// simulateEvents sends simulated events to the HTTP source
// This function can be used for testing
func simulateEvents(address string) {
	users := []string{"user1", "user2", "user3", "user4", "user5"}
	pages := []string{"/home", "/products", "/about", "/contact", "/blog", "/checkout"}
	eventTypes := []string{"pageview", "click", "scroll", "signup", "login", "purchase"}
	devices := []string{"desktop", "mobile", "tablet"}
	browsers := []string{"chrome", "firefox", "safari", "edge"}
	countries := []string{"US", "UK", "DE", "FR", "JP", "CA", "BR", "IN"}

	for {
		// Create a random event
		userID := users[rand.Intn(len(users))]
		eventType := eventTypes[rand.Intn(len(eventTypes))]
		page := pages[rand.Intn(len(pages))]
		device := devices[rand.Intn(len(devices))]
		browser := browsers[rand.Intn(len(browsers))]
		country := countries[rand.Intn(len(countries))]

		event := UserEvent{
			UserID:    userID,
			EventType: eventType,
			Timestamp: time.Now(),
			Page:      page,
			Duration:  rand.Intn(300),
			Device:    device,
			Browser:   browser,
			Country:   country,
		}

		// Convert to JSON
		data, err := json.Marshal(event)
		if err != nil {
			log.Printf("Error marshaling event: %v", err)
			continue
		}

		// Send to HTTP endpoint
		resp, err := http.Post(address, "application/json", bytes.NewBuffer(data))
		if err != nil {
			log.Printf("Error sending event: %v", err)
			continue
		}
		resp.Body.Close()

		// Wait a bit
		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
	}
}
