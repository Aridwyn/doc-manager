package cache

import (
	"context"
	"hash/fnv"
	"sync"
	"time"

	"github.com/your-org/involta/internal/domain"
)

const (
	// Default settings
	defaultShardCount = 16
	defaultTTL        = 15 * time.Minute
	defaultCleanupInterval = 1 * time.Minute
)

// CacheItem represents a cached item with expiration
type CacheItem struct {
	Value     interface{}
	CreatedAt time.Time
	ExpiresAt time.Time
}

// IsExpired checks if the cache item has expired
func (item *CacheItem) IsExpired() bool {
	return time.Now().After(item.ExpiresAt)
}

// CacheShard represents a single shard of the cache with its own lock
type CacheShard struct {
	mu    sync.RWMutex
	items map[string]*CacheItem
}

// ShardedCache is a thread-safe sharded cache implementation
type ShardedCache struct {
	shards          []*CacheShard
	shardCount      int
	ttl             time.Duration
	cleanupInterval time.Duration
	
	// Cleanup worker management
	cleanupWorkerRunning bool
	cleanupWorkerMu      sync.Mutex
	cleanupWorkerStop    chan struct{}
	cleanupWorkerWg      sync.WaitGroup
}

// NewShardedCache creates a new sharded cache with default settings
func NewShardedCache(shardCount int, ttl int) *ShardedCache {
	if shardCount < 1 {
		shardCount = defaultShardCount
	}
	
	ttlDuration := time.Duration(ttl) * time.Second
	if ttlDuration <= 0 {
		ttlDuration = defaultTTL
	}
	
	shards := make([]*CacheShard, shardCount)
	for i := range shards {
		shards[i] = &CacheShard{
			items: make(map[string]*CacheItem),
		}
	}
	
	return &ShardedCache{
		shards:          shards,
		shardCount:      shardCount,
		ttl:             ttlDuration,
		cleanupInterval: defaultCleanupInterval,
		cleanupWorkerStop: make(chan struct{}),
	}
}

// getShard returns the shard for a given key using FNV hash
func (c *ShardedCache) getShard(key string) *CacheShard {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	shardIndex := hash.Sum32() % uint32(c.shardCount)
	return c.shards[shardIndex]
}

// Get retrieves a value from the cache by key (implements domain.Cache)
func (c *ShardedCache) Get(ctx context.Context, key string) (interface{}, bool) {
	select {
	case <-ctx.Done():
		return nil, false
	default:
	}
	
	shard := c.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	
	item, exists := shard.items[key]
	if !exists {
		return nil, false
	}
	
	// Check expiration
	if item.IsExpired() {
		// Don't delete here, let cleanup worker handle it
		// Or delete with write lock if needed
		return nil, false
	}
	
	return item.Value, true
}

// Set stores a value in the cache with the given key (implements domain.Cache)
func (c *ShardedCache) Set(ctx context.Context, key string, value interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	
	shard := c.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	
	now := time.Now()
	shard.items[key] = &CacheItem{
		Value:     value,
		CreatedAt: now,
		ExpiresAt: now.Add(c.ttl),
	}
	
	return nil
}

// Delete removes a value from the cache by key (implements domain.Cache)
func (c *ShardedCache) Delete(ctx context.Context, key string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	
	shard := c.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	
	delete(shard.items, key)
	return nil
}

// CleanExpired removes all expired items from the cache (implements domain.Cache)
func (c *ShardedCache) CleanExpired(ctx context.Context) error {
	for _, shard := range c.shards {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		
		shard.mu.Lock()
		for key, item := range shard.items {
			if item.IsExpired() {
				delete(shard.items, key)
			}
		}
		shard.mu.Unlock()
	}
	return nil
}

// StartCleanupWorker starts a background goroutine that periodically removes expired items
func (c *ShardedCache) StartCleanupWorker() {
	c.cleanupWorkerMu.Lock()
	defer c.cleanupWorkerMu.Unlock()
	
	if c.cleanupWorkerRunning {
		return // Already running
	}
	
	c.cleanupWorkerRunning = true
	c.cleanupWorkerStop = make(chan struct{})
	
	c.cleanupWorkerWg.Add(1)
	go c.cleanupWorker()
}

// StopCleanupWorker stops the background cleanup worker gracefully
func (c *ShardedCache) StopCleanupWorker() {
	c.cleanupWorkerMu.Lock()
	defer c.cleanupWorkerMu.Unlock()
	
	if !c.cleanupWorkerRunning {
		return // Not running
	}
	
	close(c.cleanupWorkerStop)
	c.cleanupWorkerWg.Wait()
	c.cleanupWorkerRunning = false
}

// cleanupWorker is the background goroutine that periodically cleans up expired items
func (c *ShardedCache) cleanupWorker() {
	defer c.cleanupWorkerWg.Done()
	
	ticker := time.NewTicker(c.cleanupInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-c.cleanupWorkerStop:
			// Perform final cleanup before stopping
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = c.CleanExpired(ctx)
			cancel()
			return
		case <-ticker.C:
			// Perform periodic cleanup
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = c.CleanExpired(ctx)
			cancel()
		}
	}
}

// Clear removes all items from the cache (internal method)
func (c *ShardedCache) Clear() {
	for _, shard := range c.shards {
		shard.mu.Lock()
		shard.items = make(map[string]*CacheItem)
		shard.mu.Unlock()
	}
}

// GetStats returns cache statistics
func (c *ShardedCache) GetStats() CacheStats {
	stats := CacheStats{
		ShardCount: c.shardCount,
		TotalItems: 0,
		ShardStats: make([]ShardStat, c.shardCount),
	}
	
	for i, shard := range c.shards {
		shard.mu.RLock()
		itemCount := len(shard.items)
		expiredCount := 0
		for _, item := range shard.items {
			if item.IsExpired() {
				expiredCount++
			}
		}
		shard.mu.RUnlock()
		
		stats.ShardStats[i] = ShardStat{
			Index:       i,
			ItemCount:   itemCount,
			ExpiredCount: expiredCount,
		}
		stats.TotalItems += itemCount
	}
	
	return stats
}

// CacheStats represents cache statistics
type CacheStats struct {
	ShardCount int
	TotalItems int
	ShardStats []ShardStat
}

// ShardStat represents statistics for a single shard
type ShardStat struct {
	Index       int
	ItemCount   int
	ExpiredCount int
}

// Verify that ShardedCache implements domain.Cache interface
var _ domain.Cache = (*ShardedCache)(nil)
