package middleware

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// RequestIDKey is the context key for request ID
type RequestIDKey struct{}

// GetRequestID extracts request ID from context
func GetRequestID(ctx context.Context) string {
	if id, ok := ctx.Value(RequestIDKey{}).(string); ok {
		return id
	}
	return ""
}

// LoggingMiddleware logs HTTP requests with request ID
func LoggingMiddleware(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			
			// Generate or extract request ID
			requestID := r.Header.Get("X-Request-ID")
			if requestID == "" {
				requestID = uuid.New().String()
			}
			
			// Add request ID to context
			ctx := context.WithValue(r.Context(), RequestIDKey{}, requestID)
			r = r.WithContext(ctx)
			
			// Add request ID to response header
			w.Header().Set("X-Request-ID", requestID)
			
			// Wrap response writer to capture status code
			wrapped := &responseWriter{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}
			
			// Log request
			logger.Info("HTTP request started",
				zap.String("request_id", requestID),
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.String("query", r.URL.RawQuery),
				zap.String("remote_addr", r.RemoteAddr),
			)
			
			// Call next handler
			next.ServeHTTP(wrapped, r)
			
			// Log response
			duration := time.Since(start)
			logger.Info("HTTP request completed",
				zap.String("request_id", requestID),
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Int("status", wrapped.statusCode),
				zap.Duration("duration", duration),
			)
		})
	}
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// RecoveryMiddleware recovers from panics and returns 500 error
func RecoveryMiddleware(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					requestID := GetRequestID(r.Context())
					
					logger.Error("panic recovered",
						zap.String("request_id", requestID),
						zap.String("method", r.Method),
						zap.String("path", r.URL.Path),
						zap.Any("error", err),
						zap.Stack("stack"),
					)
					
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Fprintf(w, `{"error":"internal server error","request_id":"%s"}`, requestID)
				}
			}()
			
			next.ServeHTTP(w, r)
		})
	}
}

// TimeoutMiddleware adds timeout to request context
func TimeoutMiddleware(timeout time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()
			
			r = r.WithContext(ctx)
			
			// Create a channel to signal completion
			done := make(chan struct{})
			
			go func() {
				next.ServeHTTP(w, r)
				close(done)
			}()
			
			select {
			case <-done:
				// Request completed normally
			case <-ctx.Done():
				// Timeout occurred
				requestID := GetRequestID(r.Context())
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusRequestTimeout)
				fmt.Fprintf(w, `{"error":"request timeout","request_id":"%s"}`, requestID)
			}
		})
	}
}

// RateLimiter implements rate limiting using token bucket
type RateLimiter struct {
	requests   int64
	limit      int64
	resetAfter time.Duration
	lastReset  int64 // Unix timestamp
	mu         int64 // Atomic mutex for reset
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(limit int, resetAfter time.Duration) *RateLimiter {
	if limit < 1 {
		limit = 100
	}
	if resetAfter <= 0 {
		resetAfter = 1 * time.Minute
	}
	
	return &RateLimiter{
		requests:   0,
		limit:      int64(limit),
		resetAfter: resetAfter,
		lastReset:  time.Now().Unix(),
	}
}

// Allow checks if request is allowed
func (rl *RateLimiter) Allow() bool {
	now := time.Now().Unix()
	lastReset := atomic.LoadInt64(&rl.lastReset)
	
	// Reset if period has passed
	if now-lastReset >= int64(rl.resetAfter.Seconds()) {
		if atomic.CompareAndSwapInt64(&rl.mu, 0, 1) {
			atomic.StoreInt64(&rl.requests, 0)
			atomic.StoreInt64(&rl.lastReset, now)
			atomic.StoreInt64(&rl.mu, 0)
		}
	}
	
	// Check limit
	current := atomic.AddInt64(&rl.requests, 1)
	return current <= rl.limit
}

// RateLimitMiddleware limits request rate
func RateLimitMiddleware(limiter *RateLimiter, logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !limiter.Allow() {
				requestID := GetRequestID(r.Context())
				
				logger.Warn("rate limit exceeded",
					zap.String("request_id", requestID),
					zap.String("method", r.Method),
					zap.String("path", r.URL.Path),
				)
				
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", fmt.Sprintf("%.0f", limiter.resetAfter.Seconds()))
				w.WriteHeader(http.StatusTooManyRequests)
				fmt.Fprintf(w, `{"error":"rate limit exceeded","request_id":"%s"}`, requestID)
				return
			}
			
			next.ServeHTTP(w, r)
		})
	}
}

