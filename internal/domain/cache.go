package domain

import "context"

// Cache defines the interface for caching operations
type Cache interface {
	// Get retrieves a value from the cache by key
	Get(ctx context.Context, key string) (interface{}, bool)

	// Set stores a value in the cache with the given key
	Set(ctx context.Context, key string, value interface{}) error

	// Delete removes a value from the cache by key
	Delete(ctx context.Context, key string) error

	// CleanExpired removes all expired items from the cache
	CleanExpired(ctx context.Context) error
}

