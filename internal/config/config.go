package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	EngineID            string   `json:"engine_id"`
	Symbols             []string `json:"symbols"`
	ShardCount          int      `json:"shard_count"`
	IngressQueueSize    int      `json:"ingress_queue_size"`
	ShardQueueSize      int      `json:"shard_queue_size"`
	WALDir              string   `json:"wal_dir"`
	SnapshotDir         string   `json:"snapshot_dir"`
	MaxBatchCommands    int      `json:"max_batch_commands"`
	MaxBatchBytes       int      `json:"max_batch_bytes"`
	MaxBatchDelayMicros int      `json:"max_batch_delay_micros"`
	EnableMetrics       bool     `json:"enable_metrics"`
	MetricsReportMillis int      `json:"metrics_report_millis"`
}

func Load(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()

	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (c Config) Validate() error {
	if c.EngineID == "" {
		return fmt.Errorf("engine_id is required")
	}
	if len(c.Symbols) == 0 {
		return fmt.Errorf("at least one symbol is required")
	}
	if c.ShardCount <= 0 {
		return fmt.Errorf("shard_count must be positive")
	}
	if c.IngressQueueSize <= 0 || c.ShardQueueSize <= 0 {
		return fmt.Errorf("queue sizes must be positive")
	}
	if c.MaxBatchCommands <= 0 || c.MaxBatchBytes <= 0 || c.MaxBatchDelayMicros <= 0 {
		return fmt.Errorf("batch thresholds must be positive")
	}
	if c.WALDir == "" || c.SnapshotDir == "" {
		return fmt.Errorf("wal_dir and snapshot_dir are required")
	}
	if c.MetricsReportMillis < 0 {
		return fmt.Errorf("metrics_report_millis cannot be negative")
	}

	seen := make(map[string]struct{}, len(c.Symbols))
	for _, s := range c.Symbols {
		if s == "" {
			return fmt.Errorf("symbols cannot contain empty values")
		}
		if _, ok := seen[s]; ok {
			return fmt.Errorf("duplicate symbol: %s", s)
		}
		seen[s] = struct{}{}
	}

	return nil
}
