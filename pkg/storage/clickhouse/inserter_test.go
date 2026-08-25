package clickhouse

import (
	"testing"
	"time"
)

func TestBackoffDelay(t *testing.T) {
	cases := map[int]time.Duration{
		1: time.Second,
		2: 2 * time.Second,
		3: 4 * time.Second,
		4: 8 * time.Second,
		5: 16 * time.Second,
		6: 30 * time.Second, // capped
		9: 30 * time.Second,
	}
	for attempt, want := range cases {
		if got := backoffDelay(attempt); got != want {
			t.Errorf("attempt %d: got %v want %v", attempt, got, want)
		}
	}
}

func TestConfigDefaults(t *testing.T) {
	c := Config{}.withDefaults()
	if c.MaxOpenConns != 10 || c.MaxIdleConns != 5 || c.BatchSize != 10000 {
		t.Errorf("defaults wrong: %+v", c)
	}
	if c.FlushInterval != 5*time.Second || c.MaxRetries != 5 || c.Database != "mnemosyne" {
		t.Errorf("defaults wrong: %+v", c)
	}
	c2 := Config{MaxOpenConns: 1, BatchSize: 2, FlushInterval: time.Second, MaxRetries: 1, Database: "x", MaxIdleConns: 3, DialTimeout: time.Second}.withDefaults()
	if c2.BatchSize != 2 || c2.Database != "x" {
		t.Errorf("should preserve explicit values: %+v", c2)
	}
}
