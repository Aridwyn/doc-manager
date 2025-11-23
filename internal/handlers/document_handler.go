package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"

	"github.com/your-org/involta/internal/domain"
	"github.com/your-org/involta/internal/middleware"
	"github.com/your-org/involta/internal/usecases"
)

const (
	defaultPage     = 1
	defaultPerPage  = 20
	maxPerPage      = 100
)

// DocumentHandler handles HTTP requests for documents
type DocumentHandler struct {
	usecase *usecases.DocumentUsecase
	logger  *zap.Logger
}

// NewDocumentHandler creates a new document handler
func NewDocumentHandler(usecase *usecases.DocumentUsecase, logger *zap.Logger) *DocumentHandler {
	return &DocumentHandler{
		usecase: usecase,
		logger:  logger,
	}
}

// CreateDocument handles POST /documents
func (h *DocumentHandler) CreateDocument(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	requestID := middleware.GetRequestID(ctx)
	
	var doc domain.Document
	if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
		h.logger.Error("failed to decode request body",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		h.respondError(w, http.StatusBadRequest, "invalid request body", requestID)
		return
	}
	
	// Validate input
	if err := h.validateDocument(&doc); err != nil {
		h.logger.Warn("validation failed",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		h.respondError(w, http.StatusBadRequest, err.Error(), requestID)
		return
	}
	
	if err := h.usecase.CreateDocument(ctx, &doc); err != nil {
		h.logger.Error("failed to create document",
			zap.String("request_id", requestID),
			zap.String("id", doc.ID),
			zap.Error(err),
		)
		h.respondError(w, http.StatusInternalServerError, "failed to create document", requestID)
		return
	}
	
	h.respondJSON(w, http.StatusCreated, doc, requestID)
}

// GetDocument handles GET /documents/{id}
func (h *DocumentHandler) GetDocument(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	requestID := middleware.GetRequestID(ctx)
	
	id := chi.URLParam(r, "id")
	if id == "" {
		h.respondError(w, http.StatusBadRequest, "id parameter is required", requestID)
		return
	}
	
	doc, err := h.usecase.GetDocument(ctx, id)
	if err != nil {
		h.logger.Error("failed to get document",
			zap.String("request_id", requestID),
			zap.String("id", id),
			zap.Error(err),
		)
		h.respondError(w, http.StatusNotFound, "document not found", requestID)
		return
	}
	
	h.respondJSON(w, http.StatusOK, doc, requestID)
}

// UpdateDocument handles PUT /documents/{id}
func (h *DocumentHandler) UpdateDocument(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	requestID := middleware.GetRequestID(ctx)
	
	id := chi.URLParam(r, "id")
	if id == "" {
		h.respondError(w, http.StatusBadRequest, "id parameter is required", requestID)
		return
	}
	
	var doc domain.Document
	if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
		h.logger.Error("failed to decode request body",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		h.respondError(w, http.StatusBadRequest, "invalid request body", requestID)
		return
	}
	
	// Set ID from path
	doc.ID = id
	
	// Validate input
	if err := h.validateDocument(&doc); err != nil {
		h.logger.Warn("validation failed",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		h.respondError(w, http.StatusBadRequest, err.Error(), requestID)
		return
	}
	
	if err := h.usecase.UpdateDocument(ctx, &doc); err != nil {
		h.logger.Error("failed to update document",
			zap.String("request_id", requestID),
			zap.String("id", doc.ID),
			zap.Error(err),
		)
		h.respondError(w, http.StatusInternalServerError, "failed to update document", requestID)
		return
	}
	
	h.respondJSON(w, http.StatusOK, doc, requestID)
}

// DeleteDocument handles DELETE /documents/{id}
func (h *DocumentHandler) DeleteDocument(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	requestID := middleware.GetRequestID(ctx)
	
	id := chi.URLParam(r, "id")
	if id == "" {
		h.respondError(w, http.StatusBadRequest, "id parameter is required", requestID)
		return
	}
	
	if err := h.usecase.DeleteDocument(ctx, id); err != nil {
		h.logger.Error("failed to delete document",
			zap.String("request_id", requestID),
			zap.String("id", id),
			zap.Error(err),
		)
		h.respondError(w, http.StatusInternalServerError, "failed to delete document", requestID)
		return
	}
	
	h.respondJSON(w, http.StatusOK, map[string]string{"message": "document deleted"}, requestID)
}

// ListDocuments handles GET /documents with pagination
func (h *DocumentHandler) ListDocuments(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	requestID := middleware.GetRequestID(ctx)
	
	// Parse pagination parameters
	page, perPage, err := h.parsePaginationParams(r)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error(), requestID)
		return
	}
	
	// Calculate offset
	offset := (page - 1) * perPage
	
	params := domain.PaginationParams{
		Limit:  perPage,
		Offset: offset,
	}
	
	result, err := h.usecase.ListDocuments(ctx, params)
	if err != nil {
		h.logger.Error("failed to list documents",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		h.respondError(w, http.StatusInternalServerError, "failed to list documents", requestID)
		return
	}
	
	// Build pagination response
	response := map[string]interface{}{
		"data": result.Items,
		"pagination": map[string]interface{}{
			"page":        page,
			"per_page":    perPage,
			"total":       result.Total,
			"total_pages": (result.Total + perPage - 1) / perPage,
			"has_more":    result.HasMore,
		},
	}
	
	h.respondJSON(w, http.StatusOK, response, requestID)
}

// parsePaginationParams parses and validates pagination parameters
func (h *DocumentHandler) parsePaginationParams(r *http.Request) (page, perPage int, err error) {
	// Parse page
	pageStr := r.URL.Query().Get("page")
	if pageStr == "" {
		page = defaultPage
	} else {
		page, err = strconv.Atoi(pageStr)
		if err != nil || page < 1 {
			return 0, 0, fmt.Errorf("invalid page parameter: must be a positive integer")
		}
	}
	
	// Parse per_page
	perPageStr := r.URL.Query().Get("per_page")
	if perPageStr == "" {
		perPage = defaultPerPage
	} else {
		perPage, err = strconv.Atoi(perPageStr)
		if err != nil || perPage < 1 {
			return 0, 0, fmt.Errorf("invalid per_page parameter: must be a positive integer")
		}
		if perPage > maxPerPage {
			perPage = maxPerPage
		}
	}
	
	return page, perPage, nil
}

// validateDocument validates document input
func (h *DocumentHandler) validateDocument(doc *domain.Document) error {
	if doc.ID == "" {
		return fmt.Errorf("id is required")
	}
	if doc.Name == "" {
		return fmt.Errorf("name is required")
	}
	return nil
}

// respondJSON sends a JSON response
func (h *DocumentHandler) respondJSON(w http.ResponseWriter, status int, data interface{}, requestID string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Request-ID", requestID)
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		h.logger.Error("failed to encode response",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
	}
}

// respondError sends an error response
func (h *DocumentHandler) respondError(w http.ResponseWriter, status int, message, requestID string) {
	h.respondJSON(w, status, map[string]string{
		"error":      message,
		"request_id": requestID,
	}, requestID)
}
