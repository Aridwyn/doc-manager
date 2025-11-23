package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"

	"github.com/your-org/involta/internal/cache"
	"github.com/your-org/involta/internal/config"
	"github.com/your-org/involta/internal/handlers"
	"github.com/your-org/involta/internal/middleware"
	"github.com/your-org/involta/internal/processor"
	"github.com/your-org/involta/internal/repositories"
	"github.com/your-org/involta/internal/usecases"
	"github.com/your-org/involta/pkg/logger"
)

const (
	// Настройки для проверки здоровья сервиса при старте.
	// Мы даем базе данных немного времени "проснуться", прежде чем паниковать.
	healthCheckRetries    = 5               // Сколько раз попробуем подключиться
	healthCheckRetryDelay = 2 * time.Second // Сколько подождем между попытками
	
	// Время на аккуратное завершение работы сервера (доделать текущие запросы).
	shutdownTimeout       = 30 * time.Second
)

// App — это сердце нашего приложения.
// Структура держит вместе все зависимости, чтобы их не приходилось передавать глобально.
// Это упрощает тестирование и управление жизненным циклом (старт/стоп).
type App struct {
	config     *config.Config
	logger     *zap.Logger
	repo       *repositories.ReindexerRepository
	cache      *cache.ShardedCache
	processor  *processor.OrderedProcessor
	usecase    *usecases.DocumentUsecase
	server     *http.Server
	
	// Механизм для гарантированной однократной инициализации.
	// Защищает от случайного повторного вызова Initialize().
	initOnce sync.Once
	initErr  error
	
	// Управление фоновыми задачами (воркерами).
	// Context позволяет отменить их всех разом при выключении.
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup // Ждем, пока все воркеры закончат работу
	
	// Гарантия, что Shutdown выполнится только один раз.
	shutdownOnce sync.Once
}

// NewApp создает "пустую" заготовку приложения.
// Основная настройка произойдет позже в методе Initialize().
func NewApp() *App {
	ctx, cancel := context.WithCancel(context.Background())
	return &App{
		ctx:    ctx,
		cancel: cancel,
	}
}

// Initialize запускает процесс настройки всех компонентов.
// Работает по принципу "все или ничего": если что-то сломалось — возвращаем ошибку.
func (a *App) Initialize() error {
	a.initOnce.Do(func() {
		a.initErr = a.doInitialize()
	})
	return a.initErr
}

// doInitialize — это "сборочный цех" приложения.
// Порядок важен: сначала базовые вещи (логгер, конфиг), потом слои архитектуры (репозиторий -> кэш -> бизнес-логика -> API).
func (a *App) doInitialize() error {
	// 1. Сначала логгер, чтобы видеть, что происходит (или почему упало).
	if err := logger.Init("info", true); err != nil {
		return fmt.Errorf("не удалось инициализировать логгер: %w", err)
	}
	a.logger = logger.Get()
	
	// 2. Загружаем настройки.
	// Сначала смотрим переменную окружения, если нет — ищем файл по умолчанию.
	configPath := os.Getenv("APP_CONFIG_PATH")
	if configPath == "" {
		configPath = "config.yaml"
	}
	
	// Пытаемся загрузить файл. Если его нет — не страшно, попробуем работать на переменных окружения (ENV).
	if err := config.Load(configPath); err != nil {
		a.logger.Warn("не удалось загрузить конфиг-файл, используем значения по умолчанию и ENV",
			zap.Error(err),
		)
		// Попытка загрузки без файла (только defaults + ENV)
		if err := config.Load(""); err != nil {
			return fmt.Errorf("критическая ошибка конфигурации: %w", err)
		}
	}
	
	a.config = config.Get()
	a.logger.Info("конфигурация загружена",
		zap.String("server_host", a.config.Server.Host),
		zap.Int("server_port", a.config.Server.Port),
	)
	
	// 3. Подключаемся к базе данных (Reindexer).
	// Это критичный этап, здесь же проходят проверки 3.1 и 3.2 (связь и коллекции).
	if err := a.initializeRepository(); err != nil {
		return fmt.Errorf("ошибка инициализации репозитория: %w", err)
	}
	
	// 4. Настраиваем кэш.
	// Он шардированный (разделенный на части) для скорости.
	a.cache = cache.NewShardedCache(
		a.config.Concurrency.CacheShards,
		a.config.Cache.TTL,
	)
	// Запускаем уборщика мусора в кэше (чистит протухшие записи).
	a.cache.StartCleanupWorker()
	
	// 5. Запускаем процессор для фоновой обработки документов.
	a.processor = processor.NewDocumentProcessor(
		a.config.Concurrency.ProcessorWorkers,
		100, // размер очереди
		a.logger,
	)
	a.processor.Start()
	
	// 6. Собираем бизнес-логику (Use Case).
	// Связываем базу, кэш и процессор воедино.
	a.usecase = usecases.NewDocumentUsecase(
		a.repo,
		a.cache,
		a.processor,
		a.logger,
		a.config.Concurrency.HTTPMaxWorkers,
	)
	
	// 7. И наконец, поднимаем HTTP сервер.
	if err := a.initializeServer(); err != nil {
		return fmt.Errorf("ошибка настройки сервера: %w", err)
	}
	
	a.logger.Info("приложение готово к работе")
	return nil
}

// initializeRepository отвечает за подключение к Reindexer.
// Здесь реализована логика повторных попыток (Retry), потому что база может стартовать медленнее приложения.
// Выполняет требования 3.1 (проверка связи) и 3.2 (проверка коллекций).
func (a *App) initializeRepository() error {
	var err error
	
	// Пробуем подключиться несколько раз с паузами.
	for attempt := 0; attempt < healthCheckRetries; attempt++ {
		if attempt > 0 {
			a.logger.Info("повторная попытка подключения к БД",
				zap.Int("попытка", attempt+1),
				zap.Duration("пауза", healthCheckRetryDelay),
			)
			time.Sleep(healthCheckRetryDelay)
		}
		
		// Создаем клиент Reindexer.
		// Используем cproto (RPC) протокол для макс. производительности.
		repo, initErr := repositories.NewReindexerRepository(
			a.config.Reindexer.DSN,
			a.config.Concurrency.DBMaxConnections,
			a.logger,
		)
		if initErr != nil {
			err = initErr
			a.logger.Warn("не удалось создать клиент репозитория",
				zap.Int("попытка", attempt+1),
				zap.Error(initErr),
			)
			continue
		}
		
		// Проверка 3.1: Есть ли живой коннект?
		a.logger.Info("проверка связи с Reindexer...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if checkErr := repo.CheckConnection(ctx); checkErr != nil {
			cancel()
			repo.Close()
			err = checkErr
			a.logger.Warn("нет связи с Reindexer (3.1)",
				zap.Int("попытка", attempt+1),
				zap.Error(checkErr),
			)
			continue
		}
		cancel()
		a.logger.Info("связь с Reindexer установлена (3.1)")
		
		// Проверка 3.2: На месте ли коллекции (таблицы)?
		// Если их нет, они будут созданы автоматически (3.2.1).
		a.logger.Info("проверка коллекций... (3.2)")
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		if ensureErr := repo.EnsureCollections(ctx); ensureErr != nil {
			cancel()
			repo.Close()
			err = ensureErr
			a.logger.Warn("проблема с коллекциями (3.2)",
				zap.Int("попытка", attempt+1),
				zap.Error(ensureErr),
			)
			continue
		}
		cancel()
		a.logger.Info("коллекции проверены/созданы (3.2, 3.2.1)")
		
		// Всё отлично, сохраняем репозиторий и выходим.
		a.repo = repo
		a.logger.Info("репозиторий успешно инициализирован",
			zap.Int("попыток_затрачено", attempt+1),
			zap.String("dsn", a.config.Reindexer.DSN),
		)
		return nil
	}
	
	return fmt.Errorf("не удалось подключиться к БД после %d попыток: %w", healthCheckRetries, err)
}

// initializeServer настраивает HTTP-роутинг и middleware.
func (a *App) initializeServer() error {
	// Создаем обработчики запросов
	docHandler := handlers.NewDocumentHandler(a.usecase, a.logger)
	
	// Роутер Chi
	r := chi.NewRouter()
	
	// Ограничитель скорости (Rate Limiter) — чтобы нас не завалили запросами.
	rateLimiter := middleware.NewRateLimiter(a.config.Concurrency.HTTPMaxWorkers, 1*time.Minute)
	
	// Проверка здоровья сервиса (/health).
	// Важно: без middleware, чтобы отвечать максимально быстро и надежно.
	r.Get("/health", a.healthCheckHandler)
	
	// Цепочка middleware (выполняются по порядку для каждого запроса):
	// 1. Логирование (кто пришел?)
	// 2. Recovery (чтобы паника не уронила весь сервер)
	// 3. Timeout (чтобы запросы не висели вечно)
	// 4. Rate Limit (контроль нагрузки)
	r.Group(func(r chi.Router) {
		r.Use(middleware.LoggingMiddleware(a.logger))
		r.Use(middleware.RecoveryMiddleware(a.logger))
		r.Use(middleware.TimeoutMiddleware(30 * time.Second))
		r.Use(middleware.RateLimitMiddleware(rateLimiter, a.logger))
		
		// Маршруты для документов
		r.Route("/documents", func(r chi.Router) {
			r.Get("/", docHandler.ListDocuments)           // GET /documents
			r.Post("/", docHandler.CreateDocument)         // POST /documents
			r.Get("/{id}", docHandler.GetDocument)         // GET /documents/{id}
			r.Put("/{id}", docHandler.UpdateDocument)      // PUT /documents/{id}
			r.Delete("/{id}", docHandler.DeleteDocument)   // DELETE /documents/{id}
		})
	})
	
	// Настройка самого сервера
	addr := fmt.Sprintf("%s:%d", a.config.Server.Host, a.config.Server.Port)
	a.server = &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  15 * time.Second, // Защита от медленных клиентов (чтение)
		WriteTimeout: 15 * time.Second, // Защита от медленных клиентов (запись)
		IdleTimeout:  60 * time.Second, // Keep-alive соединения
	}
	
	return nil
}

// healthCheckHandler отвечает на проверки "ты жив?".
// Используется Docker'ом и оркестраторами для перезапуска зависших контейнеров.
// Проверяет не только "я запустился", но и "могу ли я говорить с базой".
func (a *App) healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	// Быстрый таймаут для health check'а
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	
	health := map[string]interface{}{
		"status": "ok",
		"timestamp": time.Now().Unix(),
	}
	
	// Проверяем соединение с БД
	if a.repo != nil {
		if err := a.repo.CheckConnection(ctx); err != nil {
			// Если базы нет — сервис нездоров (503)
			w.WriteHeader(http.StatusServiceUnavailable)
			health["status"] = "unhealthy"
			health["error"] = err.Error()
			json.NewEncoder(w).Encode(health)
			return
		}
		health["database"] = "connected"
	}
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(health)
}

// StartBackgroundJobs запускает все фоновые процессы.
func (a *App) StartBackgroundJobs() {
	// Периодическая проверка здоровья в логах
	a.wg.Add(1)
	go a.periodicHealthCheck()
	
	// Здесь можно добавить другие фоновые задачи (метрики, чистка и т.д.)
}

// periodicHealthCheck раз в 30 секунд пишет в лог состояние подключения к БД.
// Полезно для отладки "плавающих" проблем с сетью.
func (a *App) periodicHealthCheck() {
	defer a.wg.Done()
	
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-a.ctx.Done():
			// Приложение выключается
			a.logger.Info("фоновая проверка здоровья остановлена")
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			
			if a.repo != nil {
				if err := a.repo.CheckConnection(ctx); err != nil {
					a.logger.Warn("фоновая проверка: проблема с БД", zap.Error(err))
				} else {
					a.logger.Debug("фоновая проверка: полёт нормальный")
				}
			}
			
			cancel()
		}
	}
}

// Start запускает сервер и начинает принимать запросы.
// Блокирует выполнение только в горутине сервера.
func (a *App) Start() error {
	if err := a.Initialize(); err != nil {
		return err
	}
	
	// Запускаем фон
	a.StartBackgroundJobs()
	
	// Запускаем сервер в отдельной горутине, чтобы main мог слушать сигналы ОС
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.logger.Info("запуск HTTP сервера",
			zap.String("адрес", a.server.Addr),
		)
		// ListenAndServe блокирует, пока сервер не остановят
		if err := a.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			a.logger.Fatal("сервер упал с ошибкой", zap.Error(err))
		}
	}()
	
	return nil
}

// Shutdown аккуратно останавливает приложение.
// Важно: мы не просто рубим питание, а ждем завершения текущих запросов.
func (a *App) Shutdown() error {
	var shutdownErr error
	
	a.shutdownOnce.Do(func() {
		a.logger.Info("начинаем остановку приложения...")
		
		// 1. Сигнал всем фоновым задачам остановиться
		a.cancel()
		
		// 2. Останавливаем прием новых HTTP запросов
		if a.server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			if err := a.server.Shutdown(ctx); err != nil {
				a.logger.Error("ошибка при остановке сервера", zap.Error(err))
				shutdownErr = err
			}
			cancel()
		}
		
		// 3. Останавливаем бизнес-логику
		if a.usecase != nil {
			a.usecase.Shutdown()
		}
		
		// 4. Останавливаем обработчик документов
		if a.processor != nil {
			a.processor.Stop()
		}
		
		// 5. Останавливаем чистильщик кэша
		if a.cache != nil {
			a.cache.StopCleanupWorker()
		}
		
		// 6. Закрываем соединение с БД
		if a.repo != nil {
			if err := a.repo.Close(); err != nil {
				a.logger.Error("ошибка при закрытии БД", zap.Error(err))
				if shutdownErr == nil {
					shutdownErr = err
				}
			}
		}
		
		// 7. Ждем, пока все горутины (фоновые задачи) действительно завершатся
		done := make(chan struct{})
		go func() {
			a.wg.Wait()
			close(done)
		}()
		
		select {
		case <-done:
			a.logger.Info("все фоновые процессы завершены")
		case <-time.After(shutdownTimeout):
			a.logger.Warn("таймаут ожидания завершения процессов (принудительный выход)")
		}
		
		// Сбрасываем буфер логов на диск
		if a.logger != nil {
			_ = a.logger.Sync()
		}
		
		a.logger.Info("приложение остановлено успешно")
	})
	
	return shutdownErr
}

func main() {
	app := NewApp()
	
	// Настройка и запуск
	if err := app.Initialize(); err != nil {
		fmt.Fprintf(os.Stderr, "Фатальная ошибка инициализации: %v\n", err)
		os.Exit(1)
	}
	
	if err := app.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Фатальная ошибка запуска: %v\n", err)
		os.Exit(1)
	}
	
	// Ожидание сигналов завершения от ОС (Ctrl+C или docker stop)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit // Блокируем выполнение здесь до получения сигнала
	
	// Аккуратное завершение
	if err := app.Shutdown(); err != nil {
		fmt.Fprintf(os.Stderr, "Ошибка при остановке: %v\n", err)
		os.Exit(1)
	}
}
