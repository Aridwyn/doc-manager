package domain

import "time"

// Document represents a document entity with nested items
type Document struct {
	ID        string       `json:"id" reindex:"id,,pk"`
	Name      string       `json:"name" reindex:"name"`
	CreatedAt time.Time    `json:"created_at" reindex:"created_at"`
	Items     []Level1Item `json:"items" reindex:"items"`
}

// Level1Item represents a first-level nested item
type Level1Item struct {
	ID    string       `json:"id"`
	Name  string       `json:"name"`
	Sort  int          `json:"sort"`
	Items []Level2Item `json:"items"`
}

// Level2Item represents a second-level nested item
type Level2Item struct {
	ID   string                 `json:"id"`
	Name string                 `json:"name"`
	Data map[string]interface{} `json:"data"`
}

// PaginationParams represents pagination parameters
type PaginationParams struct {
	Limit  int
	Offset int
}

// PaginatedResult represents a paginated result
type PaginatedResult struct {
	Items      []*Document
	Total      int
	Limit      int
	Offset     int
	HasMore    bool
}
