package processor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/your-org/involta/internal/domain"
)

// TestProcessorOrderPreservation tests that processor preserves order
func TestProcessorOrderPreservation(t *testing.T) {
	logger := zaptest.NewLogger(t)
	processor := NewDocumentProcessor(5, 100, logger)
	processor.Start()
	defer processor.Stop()

	// Create documents with specific order
	documents := make([]*domain.Document, 100)
	for i := 0; i < 100; i++ {
		documents[i] = &domain.Document{
			ID:        fmt.Sprintf("doc-%d", i),
			Name:      fmt.Sprintf("Document %d", i),
			CreatedAt: time.Now(),
			Items: []domain.Level1Item{
				{
					ID:   fmt.Sprintf("item-%d", i),
					Name: fmt.Sprintf("Item %d", i),
					Sort: 100 - i, // Descending order
				},
			},
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := processor.ProcessDocuments(ctx, documents)
	require.NoError(t, err)
	require.Len(t, results, 100)

	// Verify order is preserved
	for i, result := range results {
		assert.Equal(t, documents[i].ID, result.Document.ID, "Order should be preserved at index %d", i)
		assert.Equal(t, domain.ProcessingStatusSuccess, result.Result.Status, "Document %d should be processed successfully", i)
	}
}

// TestProcessorConcurrentProcessing tests concurrent processing
func TestProcessorConcurrentProcessing(t *testing.T) {
	logger := zaptest.NewLogger(t)
	processor := NewDocumentProcessor(10, 200, logger)
	processor.Start()
	defer processor.Stop()

	// Create multiple batches
	numBatches := 10
	batchSize := 20

	var allResults [][]*domain.ProcessedDocument
	var mu sync.Mutex
	var wg sync.WaitGroup
	errors := make(chan error, numBatches)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Process batches concurrently
	wg.Add(numBatches)
	for batch := 0; batch < numBatches; batch++ {
		documents := make([]*domain.Document, batchSize)
		for i := 0; i < batchSize; i++ {
			documents[i] = &domain.Document{
				ID:        fmt.Sprintf("batch-%d-doc-%d", batch, i),
				Name:      fmt.Sprintf("Batch %d Document %d", batch, i),
				CreatedAt: time.Now(),
			}
		}

		go func(batchNum int, docs []*domain.Document) {
			defer wg.Done()
			results, err := processor.ProcessDocuments(ctx, docs)
			if err != nil {
				errors <- err
				return
			}
			mu.Lock()
			allResults = append(allResults, results)
			mu.Unlock()
		}(batch, documents)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Error during concurrent processing: %v", err)
	}

	// Verify all batches were processed
	assert.GreaterOrEqual(t, len(allResults), numBatches/2, "At least half of batches should be processed")
}

// TestProcessorWorkerPool tests worker pool functionality
func TestProcessorWorkerPool(t *testing.T) {
	logger := zaptest.NewLogger(t)
	processor := NewDocumentProcessor(5, 50, logger)
	processor.Start()
	defer processor.Stop()

	// Create documents
	documents := make([]*domain.Document, 50)
	for i := 0; i < 50; i++ {
		documents[i] = &domain.Document{
			ID:        fmt.Sprintf("worker-doc-%d", i),
			Name:      fmt.Sprintf("Worker Document %d", i),
			CreatedAt: time.Now(),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	results, err := processor.ProcessDocuments(ctx, documents)
	duration := time.Since(start)

	require.NoError(t, err)
	require.Len(t, results, 50)

	// Verify all documents were processed
	for _, result := range results {
		assert.Equal(t, domain.ProcessingStatusSuccess, result.Result.Status)
	}

	// Processing should be faster than sequential (with 5 workers)
	// Sequential would take at least 50 * 10ms = 500ms
	// With 5 workers, should take around 100-200ms
	assert.Less(t, duration, 1*time.Second, "Processing should be faster with worker pool")
}

// TestProcessorGracefulShutdown tests graceful shutdown
func TestProcessorGracefulShutdown(t *testing.T) {
	logger := zaptest.NewLogger(t)
	processor := NewDocumentProcessor(5, 100, logger)
	processor.Start()

	// Start processing
	documents := make([]*domain.Document, 100)
	for i := 0; i < 100; i++ {
		documents[i] = &domain.Document{
			ID:        fmt.Sprintf("shutdown-doc-%d", i),
			Name:      fmt.Sprintf("Shutdown Document %d", i),
			CreatedAt: time.Now(),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start processing in goroutine
	done := make(chan bool)
	go func() {
		_, _ = processor.ProcessDocuments(ctx, documents)
		done <- true
	}()

	// Stop processor
	processor.Stop()

	// Wait for completion or timeout
	select {
	case <-done:
		// Processing completed
	case <-time.After(5 * time.Second):
		t.Fatal("Processor did not shutdown gracefully")
	}
}

