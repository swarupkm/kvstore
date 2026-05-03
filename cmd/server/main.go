package main

import (
	_ "net/http/pprof"
	"net/http"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kvstore/internal/config"
	"kvstore/internal/replication"
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

	// primary mode: stream WAL to replicas
	if cfg.ReplicationAddr != "" && cfg.ReplicaOf == "" {
		primary := replication.NewPrimary()
		if err := primary.Listen(cfg.ReplicationAddr); err != nil {
			log.Fatalf("replication listener failed: %v", err)
		}
		wal.SetStreamer(primary)
	}

	// replica mode: connect to primary and apply entries
	if cfg.ReplicaOf != "" {
		replica := replication.NewReplica(cfg.ReplicaOf, s)
		replica.Start()
		fmt.Println("running as replica of", cfg.ReplicaOf)
	}

	go runAutoCompaction(s, time.Duration(cfg.CompactionIntervalSeconds)*time.Second)

	srv, err := server.New(cfg.Address, s)
	if err != nil {
		log.Fatalf("failed to start server: %v", err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		log.Println("pprof listening on :6060")
		log.Println(http.ListenAndServe(":6060", nil))
	}()
	go srv.Start()

	sig := <-quit
	fmt.Printf("\nreceived signal: %s\n", sig)
	fmt.Println("compacting WAL before shutdown...")
	if err := s.Compact(); err != nil {
		log.Printf("compaction error: %v", err)
	}
	fmt.Println("closing WAL...")
	wal.Close()
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