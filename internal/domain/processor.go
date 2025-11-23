package domain

import "context"

// DocumentProcessor defines the interface for document processing
type DocumentProcessor interface {
	// ProcessDocuments processes multiple documents while preserving order
	// The order of results matches the order of input documents
	ProcessDocuments(ctx context.Context, documents []*Document) ([]*ProcessedDocument, error)
}

// ProcessedDocument represents a processed document with result
type ProcessedDocument struct {
	Document *Document
	Result   ProcessingResult
	Error    error
}

// ProcessingResult represents the result of document processing
type ProcessingResult struct {
	ProcessedAt int64                  `json:"processed_at"` // Unix timestamp
	Status      ProcessingStatus       `json:"status"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// ProcessingStatus represents the status of processing
type ProcessingStatus string

const (
	ProcessingStatusSuccess ProcessingStatus = "success"
	ProcessingStatusFailed  ProcessingStatus = "failed"
	ProcessingStatusSkipped ProcessingStatus = "skipped"
)

