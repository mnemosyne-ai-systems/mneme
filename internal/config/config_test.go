package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(filepath.Join("testdata", "not-exist.yaml"))
	if err == nil {
		t.Logf("cfg: %+v", cfg)
	}
	// 指定不存在的文件应报错
	if err == nil {
		t.Fatal("expected error for missing explicit config file")
	}
}

func TestLoadFromFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	content := `
server:
  port: 9090
clickhouse:
  hosts: ["ch:9000"]
  batch_size: 100
  flush_interval: 1s
log:
  level: debug
`
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Server.Port != 9090 {
		t.Errorf("port = %d, want 9090", cfg.Server.Port)
	}
	if cfg.ClickHouse.BatchSize != 100 {
		t.Errorf("batch_size = %d, want 100", cfg.ClickHouse.BatchSize)
	}
	if cfg.ClickHouse.FlushInterval != time.Second {
		t.Errorf("flush_interval = %v", cfg.ClickHouse.FlushInterval)
	}
	// 默认值仍生效
	if cfg.Pulsar.Topics.History != "mneme-history" {
		t.Errorf("history topic = %s", cfg.Pulsar.Topics.History)
	}
	if cfg.Server.Addr() != ":9090" {
		t.Errorf("addr = %s", cfg.Server.Addr())
	}
}

func TestEnvOverride(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p, []byte("server:\n  port: 8080\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MNEME_SERVER_PORT", "7777")
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Server.Port != 7777 {
		t.Errorf("port = %d, want 7777 (env override)", cfg.Server.Port)
	}
}

func TestCacheConfig(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	content := `
cache:
  session_ttl: 1h
  profile_ttl: 2h
`
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Cache.SessionTTL != time.Hour {
		t.Errorf("session_ttl = %v, want 1h", cfg.Cache.SessionTTL)
	}
	if cfg.Cache.ProfileTTL != 2*time.Hour {
		t.Errorf("profile_ttl = %v, want 2h", cfg.Cache.ProfileTTL)
	}
}

func TestValidate(t *testing.T) {
	c := &Config{}
	if err := c.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}
