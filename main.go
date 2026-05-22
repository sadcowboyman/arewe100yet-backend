package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/robfig/cron/v3"
)

type ExchangeRateApiResponse struct {
	Rates struct {
		Inr float64 `json:"INR"`
	} `json:"rates"`
}

type Cache struct {
	mu   sync.RWMutex
	rate float64
}

func (c *Cache) Set(rate float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rate = rate
}

func (c *Cache) Get() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rate
}

// FIX: Return an error instead of using os.Exit(1) to keep the background worker from crashing the server
func getExchangeRate() (float64, error) {
	e := &ExchangeRateApiResponse{}
	resp, err := http.Get("https://open.er-api.com/v6/latest/USD")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		return 0, err
	}
	return e.Rates.Inr, nil
}

func main() {
	cache := &Cache{}

	// 1. Initial load with error logging instead of an application crash
	initialRate, err := getExchangeRate()
	if err != nil {
		log.Printf("Warning: Initial API fetch failed (%v). Starting cache at 0.0.", err)
	} else {
		cache.Set(initialRate)
		log.Printf("Initial rate loaded successfully: %.2f\n", initialRate)
	}

	// 2. Set up the background cron engine
	c := cron.New()
	_, err = c.AddFunc("@hourly", func() {
		log.Println("[Cron] Fetching fresh hourly rate...")
		newRate, err := getExchangeRate()
		if err != nil {
			// FIX: Log the error and skip updating. The web server keeps running and serves the old cached rate.
			log.Printf("[Cron Error] Failed to update rate: %v. Retaining previous cache.", err)
			return
		}
		cache.Set(newRate)
		log.Printf("[Cron] Cache updated successfully to: %.2f\n", newRate)
	})
	if err != nil {
		log.Fatalf("Error scheduling cron: %v", err)
	}
	c.Start()
	defer c.Stop()

	// 3. Set up your HTTP Server Routing
	http.HandleFunc("/rate", func(w http.ResponseWriter, r *http.Request) {
		currentRate := cache.Get()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]float64{"USD_TO_INR": currentRate})
	})

	// FIX: Grab the dynamic port Render gives your application 
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // Fallback local development port if PORT env is empty
	}

	log.Printf("Server starting on port %s...\n", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server closed unexpectedly: %v", err)
	}
}
