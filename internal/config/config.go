package config

import (
	"fmt"
	"sync"

	"github.com/spf13/viper"
)

var (
	instance *Config
	once     sync.Once
	mu       sync.RWMutex
)

// Config represents the application configuration
type Config struct {
	Server      ServerConfig      `mapstructure:"server"`
	Reindexer   ReindexerConfig   `mapstructure:"reindexer"`
	Cache       CacheConfig       `mapstructure:"cache"`
	Concurrency ConcurrencyConfig `mapstructure:"concurrency"`
}

// ServerConfig contains HTTP server configuration
type ServerConfig struct {
	Host string `mapstructure:"host" validate:"required"`
	Port int    `mapstructure:"port" validate:"required,min=1,max=65535"`
}

// ReindexerConfig contains Reindexer database configuration
type ReindexerConfig struct {
	DSN            string `mapstructure:"dsn" validate:"required"`
	Namespace      string `mapstructure:"namespace" validate:"required"`
	MaxConnections int    `mapstructure:"max_connections" validate:"min=1"`
}

// CacheConfig contains cache configuration
type CacheConfig struct {
	Shards int `mapstructure:"shards" validate:"min=1"`
	TTL    int `mapstructure:"ttl" validate:"min=0"` // TTL in seconds
}

// ConcurrencyConfig contains concurrency settings
type ConcurrencyConfig struct {
	HTTPMaxWorkers    int `mapstructure:"http_max_workers" validate:"min=1"`
	ProcessorWorkers  int `mapstructure:"processor_workers" validate:"min=1"`
	CacheShards       int `mapstructure:"cache_shards" validate:"min=1"`
	DBMaxConnections  int `mapstructure:"db_max_connections" validate:"min=1"`
}

// Get returns the singleton configuration instance
func Get() *Config {
	once.Do(func() {
		if instance == nil {
			instance = &Config{}
		}
	})
	mu.RLock()
	defer mu.RUnlock()
	return instance
}

// Load initializes and loads configuration from file and environment variables
func Load(configPath string) error {
	mu.Lock()
	defer mu.Unlock()

	viper.SetConfigType("yaml")
	viper.SetEnvPrefix("APP")
	viper.AutomaticEnv()

	// Set default values
	setDefaults()

	// Load from file if provided
	if configPath != "" {
		viper.SetConfigFile(configPath)
		if err := viper.ReadInConfig(); err != nil {
			return fmt.Errorf("failed to read config file: %w", err)
		}
	}

	// Bind environment variables
	bindEnvVars()

	// Unmarshal configuration
	cfg := &Config{}
	if err := viper.Unmarshal(cfg); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Validate configuration
	if err := validate(cfg); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	instance = cfg
	return nil
}

// setDefaults sets default configuration values
func setDefaults() {
	// Server defaults
	viper.SetDefault("server.host", "0.0.0.0")
	viper.SetDefault("server.port", 8080)

	// Reindexer defaults
	// Используем cproto протокол (требует CGO) - RPC/TCP порт 6534
	viper.SetDefault("reindexer.dsn", "cproto://localhost:6534/db")
	viper.SetDefault("reindexer.namespace", "default")
	viper.SetDefault("reindexer.max_connections", 10)

	// Cache defaults
	viper.SetDefault("cache.shards", 16)
	viper.SetDefault("cache.ttl", 900)

	// Concurrency defaults
	viper.SetDefault("concurrency.http_max_workers", 100)
	viper.SetDefault("concurrency.processor_workers", 10)
	viper.SetDefault("concurrency.cache_shards", 16)
	viper.SetDefault("concurrency.db_max_connections", 10)
}

// bindEnvVars binds environment variables to viper keys
func bindEnvVars() {
	// Server
	viper.BindEnv("server.host", "APP_SERVER_HOST")
	viper.BindEnv("server.port", "APP_SERVER_PORT")

	// Reindexer
	viper.BindEnv("reindexer.dsn", "APP_REINDEXER_DSN")
	viper.BindEnv("reindexer.namespace", "APP_REINDEXER_NAMESPACE")
	viper.BindEnv("reindexer.max_connections", "APP_REINDEXER_MAX_CONNECTIONS")

	// Cache
	viper.BindEnv("cache.shards", "APP_CACHE_SHARDS")
	viper.BindEnv("cache.ttl", "APP_CACHE_TTL")

	// Concurrency
	viper.BindEnv("concurrency.http_max_workers", "APP_CONCURRENCY_HTTP_MAX_WORKERS")
	viper.BindEnv("concurrency.processor_workers", "APP_CONCURRENCY_PROCESSOR_WORKERS")
	viper.BindEnv("concurrency.cache_shards", "APP_CONCURRENCY_CACHE_SHARDS")
	viper.BindEnv("concurrency.db_max_connections", "APP_CONCURRENCY_DB_MAX_CONNECTIONS")
}

// validate performs validation on the configuration
func validate(cfg *Config) error {
	// Validate Server
	if cfg.Server.Host == "" {
		return fmt.Errorf("server.host is required")
	}
	if cfg.Server.Port < 1 || cfg.Server.Port > 65535 {
		return fmt.Errorf("server.port must be between 1 and 65535")
	}

	// Validate Reindexer
	if cfg.Reindexer.DSN == "" {
		return fmt.Errorf("reindexer.dsn is required")
	}
	if cfg.Reindexer.Namespace == "" {
		return fmt.Errorf("reindexer.namespace is required")
	}
	if cfg.Reindexer.MaxConnections < 1 {
		return fmt.Errorf("reindexer.max_connections must be at least 1")
	}

	// Validate Cache
	if cfg.Cache.Shards < 1 {
		return fmt.Errorf("cache.shards must be at least 1")
	}
	if cfg.Cache.TTL < 0 {
		return fmt.Errorf("cache.ttl must be non-negative")
	}

	// Validate Concurrency
	if cfg.Concurrency.HTTPMaxWorkers < 1 {
		return fmt.Errorf("concurrency.http_max_workers must be at least 1")
	}
	if cfg.Concurrency.ProcessorWorkers < 1 {
		return fmt.Errorf("concurrency.processor_workers must be at least 1")
	}
	if cfg.Concurrency.CacheShards < 1 {
		return fmt.Errorf("concurrency.cache_shards must be at least 1")
	}
	if cfg.Concurrency.DBMaxConnections < 1 {
		return fmt.Errorf("concurrency.db_max_connections must be at least 1")
	}

	return nil
}

// Reload reloads the configuration (thread-safe)
func Reload(configPath string) error {
	mu.Lock()
	defer mu.Unlock()

	// Reset instance to allow reload
	instance = nil
	once = sync.Once{}

	return Load(configPath)
}

