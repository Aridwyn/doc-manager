package repositories

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/your-org/involta/internal/domain"
)

// waitForReindexer waits for Reindexer to be available
func waitForReindexer(dsn string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		repo, err := NewReindexerRepository(dsn, 1, zaptest.NewLogger(&testing.T{}))
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := repo.CheckConnection(ctx); err == nil {
				cancel()
				repo.Close()
				return nil
			}
			cancel()
			repo.Close()
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("timeout waiting for Reindexer")
}

// TestConcurrentCRUDOperations tests concurrent CRUD operations
func TestConcurrentCRUDOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	logger := zaptest.NewLogger(t)
	// Use real Reindexer from Docker only
	dsn := os.Getenv("REINDEXER_DSN")
	if dsn == "" {
		dsn = "cproto://localhost:9088/test_db"
	}
	
	// Wait for Reindexer to be ready (must be running in Docker)
	if err := waitForReindexer(dsn, 30*time.Second); err != nil {
		t.Fatalf("Reindexer from Docker is not available. Please run: docker-compose up -d reindexer. Error: %v", err)
	}
	
	repo, err := NewReindexerRepository(dsn, 10, logger)
	require.NoError(t, err)
	defer repo.Close()

	ctx := context.Background()

	// Ensure collections
	err = repo.EnsureCollections(ctx)
	require.NoError(t, err)

	// Number of concurrent operations
	numGoroutines := 50
	numOperations := 10

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*numOperations)

	// Concurrent Create operations
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				doc := &domain.Document{
					ID:        fmt.Sprintf("doc-%d-%d", id, j),
					Name:      fmt.Sprintf("Document %d-%d", id, j),
					CreatedAt: time.Now(),
					Items:     []domain.Level1Item{},
				}
				if err := repo.Create(ctx, doc); err != nil {
					errors <- err
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Error during concurrent create: %v", err)
	}

	// Concurrent Read operations
	wg.Add(numGoroutines)
	errors = make(chan error, numGoroutines*numOperations)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				docID := fmt.Sprintf("doc-%d-%d", id, j)
				_, err := repo.GetByID(ctx, docID)
				if err != nil {
					errors <- err
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Error during concurrent read: %v", err)
	}

	// Concurrent Update operations
	wg.Add(numGoroutines)
	errors = make(chan error, numGoroutines*numOperations)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				doc := &domain.Document{
					ID:        fmt.Sprintf("doc-%d-%d", id, j),
					Name:      fmt.Sprintf("Updated Document %d-%d", id, j),
					CreatedAt: time.Now(),
					Items:     []domain.Level1Item{},
				}
				if err := repo.Update(ctx, doc); err != nil {
					errors <- err
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Error during concurrent update: %v", err)
	}

	// Concurrent Delete operations
	wg.Add(numGoroutines)
	errors = make(chan error, numGoroutines*numOperations)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				docID := fmt.Sprintf("doc-%d-%d", id, j)
				if err := repo.Delete(ctx, docID); err != nil {
					errors <- err
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Error during concurrent delete: %v", err)
	}
}

// TestRepositoryConnectionPool tests connection pool under load
func TestRepositoryConnectionPool(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	logger := zaptest.NewLogger(t)
	// Use real Reindexer from Docker only
	dsn := os.Getenv("REINDEXER_DSN")
	if dsn == "" {
		dsn = "cproto://localhost:9088/test_db_pool"
	}
	
	// Wait for Reindexer to be ready (must be running in Docker)
	if err := waitForReindexer(dsn, 30*time.Second); err != nil {
		t.Fatalf("Reindexer from Docker is not available. Please run: docker-compose up -d reindexer. Error: %v", err)
	}
	
	repo, err := NewReindexerRepository(dsn, 5, logger)
	require.NoError(t, err)
	defer repo.Close()

	ctx := context.Background()
	err = repo.EnsureCollections(ctx)
	require.NoError(t, err)

	// Test concurrent connections
	numGoroutines := 100
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			doc := &domain.Document{
				ID:        fmt.Sprintf("pool-doc-%d", id),
				Name:      fmt.Sprintf("Pool Document %d", id),
				CreatedAt: time.Now(),
			}
			if err := repo.Create(ctx, doc); err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	errorCount := 0
	for err := range errors {
		t.Logf("Error: %v", err)
		errorCount++
	}

	// Allow some errors due to connection limits, but not too many
	assert.Less(t, errorCount, numGoroutines/10, "Too many errors in connection pool test")
}
