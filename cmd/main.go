package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq"

	"wayback-discover-diff/config"
	"wayback-discover-diff/internal/handler"
	wk "wayback-discover-diff/pkg/worker"
)

func main() {
	configFile := flag.String("config", "config.yml", "path to config file")
	flag.Parse()

	// Load configuration
	if err := config.LoadConfig(*configFile); err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: config.AppConfig.Redis.URL,
	})

	// Initialize Asynq client and server
	taskClient := asynq.NewClient(asynq.RedisClientOpt{Addr: config.AppConfig.Redis.URL})
	defer taskClient.Close()

	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: config.AppConfig.Redis.URL},
		asynq.Config{
			Concurrency: config.AppConfig.Threads,
		},
	)

	// Initialize worker
	worker := wk.NewWorker(redisClient)

	// Register task handler
	mux := asynq.NewServeMux()
	mux.HandleFunc(wk.TypeCalculateSimHash, worker.HandleCalculateSimHash)

	// Start task processor in background
	go func() {
		if err := srv.Run(mux); err != nil {
			log.Fatalf("Failed to run task processor: %v", err)
		}
	}()

	// Initialize HTTP handlers
	handler := handler.NewHandler(redisClient, taskClient)

	// Setup Gin router
	r := gin.Default()
	// Register routes
	r.GET("/calculate-simhash", handler.CalculateSimHash)
	r.GET("/simhash", handler.GetSimHash)
	r.GET("/job", handler.GetJobStatus)

	httpSrv := &http.Server{
		Addr:    ":4000",
		Handler: r,
	}

	// Start HTTP server in a goroutine
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, os.Kill)
	sig := <-sigChan
	log.Println("Received signal:", sig)

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Println("Shutting down server...")
	srv.Shutdown()
	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}
	log.Println("Server stopped")
}
