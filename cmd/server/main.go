package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kvstore/internal/config"
	"kvstore/internal/server"
	"kvstore/internal/store"
)

func main() {
	cfg := loadConfig()

	wal, err := store.OpenWAL(cfg.WALPath)
	if err != nil {
		log.Fatalf("failed to open WAL: %v", err)
	}

	s := store.New(wal)

	if err := wal.Replay(s); err != nil {
		log.Fatalf("failed to replay WAL: %v", err)
	}

	// auto-compaction in background
	go runAutoCompaction(s, time.Duration(cfg.CompactionIntervalSeconds)*time.Second)

	srv, err := server.New(cfg.Address, s)
	if err != nil {
		log.Fatalf("failed to start server: %v", err)
	}

	// listen for Ctrl+C or kill signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// start server in background goroutine
	go srv.Start()

	// block until signal received
	sig := <-quit
	fmt.Printf("\nreceived signal: %s\n", sig)
	fmt.Println("compacting WAL before shutdown...")

	if err := s.Compact(); err != nil {
		log.Printf("compaction error: %v", err)
	}

	fmt.Println("closing WAL...")
	if err := wal.Close(); err != nil {
		log.Printf("WAL close error: %v", err)
	}

	fmt.Println("kvstore shutdown complete.")
}

func runAutoCompaction(s *store.Store, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		log.Println("auto-compaction running...")
		if err := s.Compact(); err != nil {
			log.Printf("auto-compaction error: %v", err)
		}
	}
}

func loadConfig() *config.Config {
	if len(os.Args) > 1 {
		f, err := os.Open(os.Args[1])
		if err != nil {
			log.Fatalf("could not open config: %v", err)
		}
		defer f.Close()
		var cfg config.Config
		if err := json.NewDecoder(f).Decode(&cfg); err != nil {
			log.Fatalf("could not parse config: %v", err)
		}
		return &cfg
	}
	return config.Default()
}