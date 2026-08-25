package cache

import (
	"context"
	"testing"
)

func TestKeyHelpers(t *testing.T) {
	if got := MemoryKey("tenantA", "user", "123"); got != "mnemosyne:mneme:memory:tenantA:user:123" {
		t.Errorf("got %s", got)
	}
	if got := BloomKey("history", "tenantA"); got != "mnemosyne:mneme:bloom:history:tenantA" {
		t.Errorf("got %s", got)
	}
	if got := MemoryVersionKey("t", "user", "1"); got != "mnemosyne:mneme:memver:t:user:1" {
		t.Errorf("got %s", got)
	}
}

func TestMemoryBloomFilter(t *testing.T) {
	ctx := context.Background()
	b := NewMemoryBloomFilter()

	ok, err := b.Exists(ctx, "a")
	if err != nil || ok {
		t.Fatalf("unexpected: %v %v", ok, err)
	}
	if err := b.Add(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	ok, _ = b.Exists(ctx, "a")
	if !ok {
		t.Error("expected exists")
	}
	dup, err := b.CheckAndAdd(ctx, "a")
	if err != nil || !dup {
		t.Errorf("expected duplicate, got %v %v", dup, err)
	}
	dup, _ = b.CheckAndAdd(ctx, "b")
	if dup {
		t.Error("b should be new")
	}
	if b.Len() != 2 {
		t.Errorf("len = %d", b.Len())
	}
}

func TestBloomOffsets(t *testing.T) {
	b := NewRedisBloomFilter(nil, BloomOptions{Key: "k"})
	offs := b.offsets("abc")
	if len(offs) != 7 {
		t.Fatalf("hash count = %d", len(offs))
	}
	for _, o := range offs {
		if o >= b.bits {
			t.Errorf("offset out of range: %d", o)
		}
	}
	// 相同 key 结果稳定
	offs2 := b.offsets("abc")
	for i := range offs {
		if offs[i] != offs2[i] {
			t.Error("offsets not deterministic")
		}
	}
	// 不同 key 结果不同
	if b.offsets("xyz")[0] == offs[0] {
		t.Log("hash collision on first offset (acceptable)")
	}
}

func TestBloomOptionsDefaults(t *testing.T) {
	b := NewRedisBloomFilter(nil, BloomOptions{Key: "k", Bits: 128, HashCount: 3})
	if b.bits != 128 || b.hashCount != 3 {
		t.Errorf("options not respected: %d %d", b.bits, b.hashCount)
	}
}
