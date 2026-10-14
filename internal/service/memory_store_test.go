package service

import (
	"context"
	"testing"

	"github.com/mnemosyne-labs/mneme/pkg/storage"
)

func TestMemoryProfileStoreSaveAndGet(t *testing.T) {
	ctx := context.Background()
	ins := storage.NewMockInserter()
	store := NewMemoryProfileStore(ins)

	v1, err := store.SaveProfile(ctx, "t1", "user", "u1", map[string]string{"lang": "go"})
	if err != nil {
		t.Fatal(err)
	}
	if v1 != 1 {
		t.Errorf("version 1 = %d", v1)
	}
	v2, err := store.SaveProfile(ctx, "t1", "user", "u1", map[string]string{"lang": "go", "level": "5"})
	if err != nil {
		t.Fatal(err)
	}
	if v2 != 2 {
		t.Errorf("version 2 = %d", v2)
	}

	p, err := store.GetProfile(ctx, "t1", "user", "u1")
	if err != nil {
		t.Fatal(err)
	}
	if p["lang"] != "go" || p["level"] != "5" {
		t.Errorf("profile = %v", p)
	}
	if p["_version"] != "2" {
		t.Errorf("_version = %q", p["_version"])
	}
	if ins.MemoryCount() != 2 {
		t.Errorf("clickhouse memories = %d", ins.MemoryCount())
	}
	last := ins.Memories[len(ins.Memories)-1]
	if last.Version != 2 {
		t.Errorf("last version = %d", last.Version)
	}
}

func TestMemoryProfileStoreEmpty(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryProfileStore(nil)
	v, err := store.SaveProfile(ctx, "t", "user", "u", nil)
	if err != nil {
		t.Fatal(err)
	}
	if v != 0 {
		t.Errorf("empty version = %d", v)
	}
}

func TestMemoryProfileStoreGetMissing(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryProfileStore(nil)
	p, err := store.GetProfile(ctx, "t", "user", "missing")
	if err != nil {
		t.Fatal(err)
	}
	if len(p) != 0 {
		t.Errorf("expected empty profile, got %v", p)
	}
}

func TestMemoryProfileStoreInserterError(t *testing.T) {
	ctx := context.Background()
	ins := storage.NewMockInserter()
	ins.Err = errBoom
	store := NewMemoryProfileStore(ins)

	// 内存写入仍成功，但返回错误
	_, err := store.SaveProfile(ctx, "t", "user", "u", map[string]string{"a": "b"})
	if err == nil {
		t.Fatal("expected inserter error")
	}
	// 画像仍已写入内存
	p, _ := store.GetProfile(ctx, "t", "user", "u")
	if p["a"] != "b" {
		t.Errorf("profile should still be persisted in memory: %v", p)
	}
}

func TestMemoryProfileStoreIsolation(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryProfileStore(nil)
	_, _ = store.SaveProfile(ctx, "t1", "user", "u1", map[string]string{"a": "b"})
	p, _ := store.GetProfile(ctx, "t2", "user", "u1")
	if len(p) != 0 {
		t.Errorf("tenant isolation broken: %v", p)
	}
}
