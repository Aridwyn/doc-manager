package domain

import "context"

// DocumentRepository defines the interface for document persistence
type DocumentRepository interface {
	// Create creates a new document
	Create(ctx context.Context, doc *Document) error

	// GetByID retrieves a document by ID
	GetByID(ctx context.Context, id string) (*Document, error)

	// Update updates an existing document
	Update(ctx context.Context, doc *Document) error

	// Delete deletes a document by ID
	Delete(ctx context.Context, id string) error

	// ListWithPagination retrieves documents with pagination
	ListWithPagination(ctx context.Context, params PaginationParams) (*PaginatedResult, error)
}

// HealthChecker defines the interface for health checks
type HealthChecker interface {
	// CheckConnection checks if the database connection is healthy
	CheckConnection(ctx context.Context) error

	// EnsureCollections ensures that required collections/namespaces exist
	EnsureCollections(ctx context.Context) error
}

