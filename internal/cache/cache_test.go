package cache

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestCacheConcurrentAccess tests concurrent access to cache with race detection
func TestCacheConcurrentAccess(t *testing.T) {
	cache := NewShardedCache(16, 3600)
	ctx := context.Background()

	numGoroutines := 100
	numOperations := 100
	var wg sync.WaitGroup

	// Concurrent writes
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				value := fmt.Sprintf("value-%d-%d", id, j)
				err := cache.Set(ctx, key, value)
				assert.NoError(t, err)
			}
		}(i)
	}

	// Concurrent reads
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				_, _ = cache.Get(ctx, key)
			}
		}(i)
	}

	// Concurrent deletes
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				_ = cache.Delete(ctx, key)
			}
		}(i)
	}

	wg.Wait()
}

// TestCacheDataRace tests for data races in cache operations
func TestCacheDataRace(t *testing.T) {
	cache := NewShardedCache(16, 1) // Short TTL for expiration testing
	ctx := context.Background()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
				key := fmt.Sprintf("race-key-%d", i)
				cache.Set(ctx, key, i)
				i++
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	// Reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				key := "race-key-0"
				_, _ = cache.Get(ctx, key)
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	// Cleanup goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				cache.CleanExpired(ctx)
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Run for a short time
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestCacheCleanupWorker tests cleanup worker with concurrent access
func TestCacheCleanupWorker(t *testing.T) {
	cache := NewShardedCache(16, 1) // 1 second TTL
	ctx := context.Background()

	cache.StartCleanupWorker()
	defer cache.StopCleanupWorker()

	// Set items that will expire
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("expire-key-%d", i)
		cache.Set(ctx, key, i)
	}

	// Wait for expiration
	time.Sleep(2 * time.Second)

	// Verify items are cleaned up
	cleaned := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("expire-key-%d", i)
		_, exists := cache.Get(ctx, key)
		if !exists {
			cleaned++
		}
	}

	// Most items should be cleaned up
	assert.Greater(t, cleaned, 90, "Most expired items should be cleaned up")
}

// TestCacheSharding tests that sharding distributes keys evenly
func TestCacheSharding(t *testing.T) {
	cache := NewShardedCache(16, 3600)
	ctx := context.Background()

	// Set many keys
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("shard-key-%d", i)
		cache.Set(ctx, key, i)
	}

	// Get statistics
	stats := cache.GetStats()
	assert.Equal(t, 16, stats.ShardCount)
	assert.Equal(t, 1000, stats.TotalItems)

	// Check that items are distributed across shards
	nonEmptyShards := 0
	for _, shardStat := range stats.ShardStats {
		if shardStat.ItemCount > 0 {
			nonEmptyShards++
		}
	}

	// Most shards should have items
	assert.Greater(t, nonEmptyShards, 10, "Items should be distributed across multiple shards")
}

