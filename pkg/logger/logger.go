package logger

import (
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	globalLogger *zap.Logger
	once         sync.Once
)

// Init initializes the global logger
func Init(level string, development bool) error {
	var err error
	once.Do(func() {
		var zapLevel zapcore.Level
		if err = zapLevel.UnmarshalText([]byte(level)); err != nil {
			return
		}

		var config zap.Config
		if development {
			config = zap.NewDevelopmentConfig()
		} else {
			config = zap.NewProductionConfig()
		}
		config.Level = zap.NewAtomicLevelAt(zapLevel)
		config.EncoderConfig.TimeKey = "timestamp"
		config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

		globalLogger, err = config.Build()
		if err != nil {
			return
		}
	})
	return err
}

// Get returns the global logger instance
func Get() *zap.Logger {
	if globalLogger == nil {
		// Return a no-op logger if not initialized
		return zap.NewNop()
	}
	return globalLogger
}

// Sync flushes any buffered log entries
func Sync() error {
	if globalLogger != nil {
		return globalLogger.Sync()
	}
	return nil
}

