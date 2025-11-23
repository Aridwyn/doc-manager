package usecases

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap/zaptest"

	"github.com/your-org/involta/internal/domain"
)

// MockDocumentRepository is a mock implementation of DocumentRepository
type MockDocumentRepository struct {
	mock.Mock
}

// Ensure MockDocumentRepository embeds mock.Mock
var _ domain.DocumentRepository = (*MockDocumentRepository)(nil)

func (m *MockDocumentRepository) Create(ctx context.Context, doc *domain.Document) error {
	args := m.Called(ctx, doc)
	return args.Error(0)
}

func (m *MockDocumentRepository) GetByID(ctx context.Context, id string) (*domain.Document, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*domain.Document), args.Error(1)
}

func (m *MockDocumentRepository) Update(ctx context.Context, doc *domain.Document) error {
	args := m.Called(ctx, doc)
	return args.Error(0)
}

func (m *MockDocumentRepository) Delete(ctx context.Context, id string) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockDocumentRepository) ListWithPagination(ctx context.Context, params domain.PaginationParams) (*domain.PaginatedResult, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*domain.PaginatedResult), args.Error(1)
}

// MockCache is a mock implementation of Cache
type MockCache struct {
	mock.Mock
}

// Ensure MockCache embeds mock.Mock
var _ domain.Cache = (*MockCache)(nil)

func (m *MockCache) Get(ctx context.Context, key string) (interface{}, bool) {
	args := m.Called(ctx, key)
	return args.Get(0), args.Bool(1)
}

func (m *MockCache) Set(ctx context.Context, key string, value interface{}) error {
	args := m.Called(ctx, key, value)
	return args.Error(0)
}

func (m *MockCache) Delete(ctx context.Context, key string) error {
	args := m.Called(ctx, key)
	return args.Error(0)
}

func (m *MockCache) CleanExpired(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// MockDocumentProcessor is a mock implementation of DocumentProcessor
type MockDocumentProcessor struct {
	mock.Mock
}

// Ensure MockDocumentProcessor embeds mock.Mock
var _ domain.DocumentProcessor = (*MockDocumentProcessor)(nil)

func (m *MockDocumentProcessor) ProcessDocuments(ctx context.Context, documents []*domain.Document) ([]*domain.ProcessedDocument, error) {
	args := m.Called(ctx, documents)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*domain.ProcessedDocument), args.Error(1)
}

// TestDocumentUsecaseGetDocumentWithCache tests GetDocument with cache
func TestDocumentUsecaseGetDocumentWithCache(t *testing.T) {
	mockRepo := new(MockDocumentRepository)
	mockCache := new(MockCache)
	mockProcessor := new(MockDocumentProcessor)
	logger := zaptest.NewLogger(t)

	usecase := NewDocumentUsecase(mockRepo, mockCache, mockProcessor, logger, 10)
	defer usecase.Shutdown()

	ctx := context.Background()
	docID := "test-doc-1"

	cacheDoc := &domain.Document{
		ID:        docID,
		Name:      "Cached Document",
		CreatedAt: time.Now(),
	}

	// Test cache hit
	mockCache.On("Get", ctx, "document:"+docID).Return(cacheDoc, true).Once()

	doc, err := usecase.GetDocument(ctx, docID)
	assert.NoError(t, err)
	assert.Equal(t, cacheDoc, doc)
	mockCache.AssertExpectations(t)
	mockRepo.AssertNotCalled(t, "GetByID")

	// Test cache miss
	repoDoc := &domain.Document{
		ID:        docID,
		Name:      "Repo Document",
		CreatedAt: time.Now(),
	}
	processedDoc := &domain.Document{
		ID:        docID,
		Name:      "Processed Document",
		CreatedAt: repoDoc.CreatedAt,
	}

	mockCache.On("Get", ctx, "document:"+docID).Return(nil, false).Once()
	mockRepo.On("GetByID", ctx, docID).Return(repoDoc, nil).Once()
	mockProcessor.On("ProcessDocuments", mock.Anything, mock.AnythingOfType("[]*domain.Document")).Return([]*domain.ProcessedDocument{
		{
			Document: processedDoc,
			Result: domain.ProcessingResult{
				Status: domain.ProcessingStatusSuccess,
			},
		},
	}, nil).Once()
	mockCache.On("Set", mock.Anything, "document:"+docID, processedDoc).Return(nil).Maybe()

	doc, err = usecase.GetDocument(ctx, docID)
	assert.NoError(t, err)
	assert.Equal(t, processedDoc, doc)
	mockCache.AssertExpectations(t)
	mockRepo.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// TestDocumentUsecaseCreateWithCacheInvalidation tests Create with cache invalidation
func TestDocumentUsecaseCreateWithCacheInvalidation(t *testing.T) {
	mockRepo := new(MockDocumentRepository)
	mockCache := new(MockCache)
	mockProcessor := new(MockDocumentProcessor)
	logger := zaptest.NewLogger(t)

	usecase := NewDocumentUsecase(mockRepo, mockCache, mockProcessor, logger, 10)
	defer usecase.Shutdown()

	ctx := context.Background()
	doc := &domain.Document{
		ID:   "new-doc",
		Name: "New Document",
	}

	mockRepo.On("Create", ctx, mock.AnythingOfType("*domain.Document")).Return(nil).Once()
	mockCache.On("Delete", mock.Anything, "document:new-doc").Return(nil).Maybe()
	mockProcessor.On("ProcessDocuments", mock.Anything, mock.AnythingOfType("[]*domain.Document")).Return([]*domain.ProcessedDocument{}, nil).Maybe()

	err := usecase.CreateDocument(ctx, doc)
	assert.NoError(t, err)
	mockRepo.AssertExpectations(t)
}

// TestDocumentUsecaseListDocuments tests ListDocuments
func TestDocumentUsecaseListDocuments(t *testing.T) {
	mockRepo := new(MockDocumentRepository)
	mockCache := new(MockCache)
	mockProcessor := new(MockDocumentProcessor)
	logger := zaptest.NewLogger(t)

	usecase := NewDocumentUsecase(mockRepo, mockCache, mockProcessor, logger, 10)
	defer usecase.Shutdown()

	ctx := context.Background()
	params := domain.PaginationParams{
		Limit:  10,
		Offset: 0,
	}

	repoResult := &domain.PaginatedResult{
		Items: []*domain.Document{
			{ID: "doc-1", Name: "Document 1"},
			{ID: "doc-2", Name: "Document 2"},
		},
		Total:   2,
		Limit:   10,
		Offset:  0,
		HasMore: false,
	}

	processedResults := []*domain.ProcessedDocument{
		{
			Document: &domain.Document{ID: "doc-1", Name: "Document 1 Processed"},
			Result: domain.ProcessingResult{
				Status: domain.ProcessingStatusSuccess,
			},
		},
		{
			Document: &domain.Document{ID: "doc-2", Name: "Document 2 Processed"},
			Result: domain.ProcessingResult{
				Status: domain.ProcessingStatusSuccess,
			},
		},
	}

	mockRepo.On("ListWithPagination", ctx, params).Return(repoResult, nil).Once()
	mockProcessor.On("ProcessDocuments", mock.Anything, mock.AnythingOfType("[]*domain.Document")).Return(processedResults, nil).Once()

	result, err := usecase.ListDocuments(ctx, params)
	assert.NoError(t, err)
	assert.Equal(t, []*domain.Document{
		processedResults[0].Document,
		processedResults[1].Document,
	}, result.Items)
	mockRepo.AssertExpectations(t)
	mockProcessor.AssertExpectations(t)
}

// TestDocumentUsecaseErrorHandling tests error handling
func TestDocumentUsecaseErrorHandling(t *testing.T) {
	mockRepo := new(MockDocumentRepository)
	mockCache := new(MockCache)
	mockProcessor := new(MockDocumentProcessor)
	logger := zaptest.NewLogger(t)

	usecase := NewDocumentUsecase(mockRepo, mockCache, mockProcessor, logger, 10)
	defer usecase.Shutdown()

	ctx := context.Background()
	expectedError := errors.New("repository error")

	mockCache.On("Get", ctx, "document:error-doc").Return(nil, false).Once()
	mockRepo.On("GetByID", ctx, "error-doc").Return(nil, expectedError).Once()

	doc, err := usecase.GetDocument(ctx, "error-doc")
	assert.Error(t, err)
	assert.Nil(t, doc)
	assert.Equal(t, expectedError, err)
	mockRepo.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}
