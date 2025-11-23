package processor

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/your-org/involta/internal/domain"
)

// ProcessingTask represents a document processing task with index for ordering
type ProcessingTask struct {
	Index    int
	Document *domain.Document
}

// ProcessingResult represents a processing result with index for ordering
type ProcessingResult struct {
	Index    int
	Document *domain.Document
	Error    error
}

// OrderedProcessor implements domain.DocumentProcessor with worker pool and order preservation
type OrderedProcessor struct {
	workers      int
	inputQueue   chan *ProcessingTask
	outputQueue  chan *ProcessingResult
	wg           sync.WaitGroup
	logger       *zap.Logger
	ctx          context.Context
	cancel       context.CancelFunc
	
	// Shutdown management
	shutdownOnce sync.Once
	shutdownChan chan struct{}
}

// NewDocumentProcessor creates a new ordered document processor with worker pool
func NewDocumentProcessor(workers int, queueSize int, logger *zap.Logger) *OrderedProcessor {
	if workers < 1 {
		workers = 1
	}
	if queueSize < 1 {
		queueSize = 100
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	processor := &OrderedProcessor{
		workers:     workers,
		inputQueue:  make(chan *ProcessingTask, queueSize),
		outputQueue: make(chan *ProcessingResult, queueSize),
		logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
		shutdownChan: make(chan struct{}),
	}
	
	return processor
}

// Start starts the worker pool
func (p *OrderedProcessor) Start() {
	// Start workers
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
	
	p.logger.Info("ordered processor started",
		zap.Int("workers", p.workers),
	)
}

// Stop stops the worker pool gracefully
func (p *OrderedProcessor) Stop() {
	p.shutdownOnce.Do(func() {
		close(p.shutdownChan)
		p.cancel()
		
		// Close input queue to signal workers to stop
		close(p.inputQueue)
		
		// Wait for all workers to finish
		p.wg.Wait()
		
		// Close output queue
		close(p.outputQueue)
		
		p.logger.Info("ordered processor stopped")
	})
}

// ProcessDocuments processes multiple documents while preserving order (implements domain.DocumentProcessor)
func (p *OrderedProcessor) ProcessDocuments(ctx context.Context, documents []*domain.Document) ([]*domain.ProcessedDocument, error) {
	if len(documents) == 0 {
		return []*domain.ProcessedDocument{}, nil
	}
	
	// Create context with timeout for processing
	processCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	
	// Send all tasks to input queue
	for i, doc := range documents {
		task := &ProcessingTask{
			Index:    i,
			Document: doc,
		}
		
		select {
		case <-processCtx.Done():
			return nil, processCtx.Err()
		case <-p.ctx.Done():
			return nil, p.ctx.Err()
		case p.inputQueue <- task:
			// Task sent successfully
		}
	}
	
	// Collect results with order preservation
	resultsMap := make(map[int]*ProcessingResult)
	var resultsMu sync.Mutex
	
	// Start goroutine to collect results
	collectDone := make(chan struct{})
	go func() {
		defer close(collectDone)
		
		collected := 0
		for collected < len(documents) {
			select {
			case <-processCtx.Done():
				return
			case <-p.ctx.Done():
				return
			case result, ok := <-p.outputQueue:
				if !ok {
					// Output queue closed
					return
				}
				
				resultsMu.Lock()
				resultsMap[result.Index] = result
				collected++
				resultsMu.Unlock()
			}
		}
	}()
	
	// Wait for all results to be collected
	select {
	case <-processCtx.Done():
		return nil, processCtx.Err()
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	case <-collectDone:
		// All results collected
	}
	
	// Build result slice preserving original order
	processedDocs := make([]*domain.ProcessedDocument, len(documents))
	for i, doc := range documents {
		result, ok := resultsMap[i]
		if !ok {
			// Result not found, create error result
			processedDocs[i] = &domain.ProcessedDocument{
				Document: doc,
				Result: domain.ProcessingResult{
					ProcessedAt: time.Now().Unix(),
					Status:      domain.ProcessingStatusFailed,
				},
				Error: context.DeadlineExceeded,
			}
			continue
		}
		
		if result.Error != nil {
			processedDocs[i] = &domain.ProcessedDocument{
				Document: result.Document,
				Result: domain.ProcessingResult{
					ProcessedAt: time.Now().Unix(),
					Status:      domain.ProcessingStatusFailed,
				},
				Error: result.Error,
			}
		} else {
			processedDocs[i] = &domain.ProcessedDocument{
				Document: result.Document,
				Result: domain.ProcessingResult{
					ProcessedAt: time.Now().Unix(),
					Status:      domain.ProcessingStatusSuccess,
					Metadata: map[string]interface{}{
						"processed_at": time.Now().Unix(),
					},
				},
				Error: nil,
			}
		}
	}
	
	return processedDocs, nil
}

// worker processes tasks from the input queue
func (p *OrderedProcessor) worker(id int) {
	defer p.wg.Done()
	
	p.logger.Debug("worker started", zap.Int("worker_id", id))
	
	for {
		select {
		case <-p.ctx.Done():
			p.logger.Debug("worker stopping due to context cancellation",
				zap.Int("worker_id", id),
			)
			return
		case <-p.shutdownChan:
			p.logger.Debug("worker stopping due to shutdown",
				zap.Int("worker_id", id),
			)
			return
		case task, ok := <-p.inputQueue:
			if !ok {
				// Input queue closed
				p.logger.Debug("worker stopping due to closed input queue",
					zap.Int("worker_id", id),
				)
				return
			}
			
			// Process the document
			processedDoc := p.processDocument(id, task.Document)
			
			// Send result to output queue
			result := &ProcessingResult{
				Index:    task.Index,
				Document: processedDoc,
				Error:    nil,
			}
			
			select {
			case <-p.ctx.Done():
				return
			case p.outputQueue <- result:
				// Result sent successfully
			}
		}
	}
}

// processDocument processes a single document and excludes fields from each level
func (p *OrderedProcessor) processDocument(workerID int, doc *domain.Document) *domain.Document {
	start := time.Now()
	
	p.logger.Debug("processing document",
		zap.Int("worker_id", workerID),
		zap.String("doc_id", doc.ID),
		zap.String("doc_name", doc.Name),
	)
	
	// Create a copy of the document for processing
	processedDoc := &domain.Document{
		ID:        doc.ID,
		Name:      doc.Name,
		CreatedAt: doc.CreatedAt,
		Items:     make([]domain.Level1Item, len(doc.Items)),
	}
	
	// Process Level1Items and exclude fields
	for i, item := range doc.Items {
		processedItem := domain.Level1Item{
			ID:    item.ID,
			Name:  item.Name,
			Sort:  item.Sort,
			Items: make([]domain.Level2Item, len(item.Items)),
		}
		
		// Process Level2Items and exclude fields
		for j, level2Item := range item.Items {
			processedLevel2Item := domain.Level2Item{
				ID:   level2Item.ID,
				Name: level2Item.Name,
				// Exclude Data field - it's not copied
				// Data: level2Item.Data, // Excluded
			}
			processedItem.Items[j] = processedLevel2Item
		}
		
		processedDoc.Items[i] = processedItem
	}
	
	// Simulate processing work
	time.Sleep(10 * time.Millisecond)
	
	duration := time.Since(start)
	p.logger.Debug("document processed",
		zap.Int("worker_id", workerID),
		zap.String("doc_id", doc.ID),
		zap.Duration("duration", duration),
	)
	
	return processedDoc
}

// Verify that OrderedProcessor implements domain.DocumentProcessor interface
var _ domain.DocumentProcessor = (*OrderedProcessor)(nil)
