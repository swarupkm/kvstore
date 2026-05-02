package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	Address                   string `json:"address"`
	WALPath                   string `json:"wal_path"`
	CompactionIntervalSeconds int    `json:"compaction_interval_seconds"`
	ReplicationAddr           string `json:"replication_addr"`
	ReplicaOf                 string `json:"replica_of"`
}

func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var cfg Config
	if err := json.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func Default() *Config {
	return &Config{
		Address:                   ":5379",
		WALPath:                   "kvstore.wal",
		CompactionIntervalSeconds: 300,
	}
}