package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/robfig/cron/v3"
)

// Updated to map the provider's native UTC update string
type ExchangeRateApiResponse struct {
	Rates struct {
		Inr float64 `json:"INR"`
	} `json:"rates"`
	TimeLastUpdateUtc string `json:"time_last_update_utc"`
}

type Cache struct {
	mu          sync.RWMutex
	rate        float64
	lastUpdated string
}

func (c *Cache) Set(rate float64, timestamp string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rate = rate
	c.lastUpdated = timestamp
}

func (c *Cache) Get() (float64, string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rate, c.lastUpdated
}

func enableCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "https://arewe100yet.in")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Updated to return both the extracted rate and the API's update timestamp string
func getExchangeRate() (float64, string, error) {
	e := &ExchangeRateApiResponse{}
	resp, err := http.Get("https://open.er-api.com/v6/latest/USD")
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		return 0, "", err
	}
	
	return e.Rates.Inr, e.TimeLastUpdateUtc, nil
}

func main() {
	cache := &Cache{}
	mux := http.NewServeMux()

	// 1. Initial load pulling directly from the API response data fields
	initialRate, apiTime, err := getExchangeRate()
	if err != nil {
		log.Printf("Warning: Initial API fetch failed (%v). Starting cache empty.", err)
	} else {
		cache.Set(initialRate, apiTime)
		log.Printf("Initial rate loaded successfully: %.2f (API Time: %s)\n", initialRate, apiTime)
	}

	// 2. Background cron synchronization engine
	c := cron.New()
	_, err = c.AddFunc("@hourly", func() {
		log.Println("[Cron] Fetching fresh hourly rate...")
		newRate, apiUpdateTime, err := getExchangeRate()
		if err != nil {
			log.Printf("[Cron Error] Failed to update rate: %v. Retaining previous cache.", err)
			return
		}
		
		// Saving the official API time string instead of machine time
		cache.Set(newRate, apiUpdateTime)
		log.Printf("[Cron] Cache updated successfully to: %.2f (API Time: %s)\n", newRate, apiUpdateTime)
	})
	if err != nil {
		log.Fatalf("Error scheduling cron: %v", err)
	}
	c.Start()
	defer c.Stop()

	// 3. Endpoint delivery pipeline
	mux.HandleFunc("/rate", func(w http.ResponseWriter, r *http.Request) {
		currentRate, lastUpdated := cache.Get()
		w.Header().Set("Content-Type", "application/json")
		
		responsePayload := map[string]interface{}{
			"USD_TO_INR":  currentRate,
			"time_last_update_utc": lastUpdated, // Sends the API's string straight to your frontend
		}
		
		_ = json.NewEncoder(w).Encode(responsePayload)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Server starting on port %s...\n", port)
	if err := http.ListenAndServe(":"+port, enableCORS(mux)); err != nil {
		log.Fatalf("Server closed unexpectedly: %v", err)
	}
}
