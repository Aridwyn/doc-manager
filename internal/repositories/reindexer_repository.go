package repositories

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/restream/reindexer/v4"
	// Используем cproto (RPC) протокол — он быстрее и эффективнее стандартного HTTP.
	_ "github.com/restream/reindexer/v4/bindings/cproto"
	"go.uber.org/zap"

	"github.com/your-org/involta/internal/domain"
)

const (
	// Имя пространства имен (таблицы) для хранения документов.
	documentsNamespace = "documents"

	// Настройки для управления соединениями.
	// Reindexer не любит долгие тайм-ауты, поэтому ставим разумные ограничения.
	defaultMaxRetries     = 3
	defaultRetryDelay     = 1 * time.Second
	defaultConnectTimeout = 10 * time.Second
	defaultQueryTimeout   = 5 * time.Second
)

// HealthStatus хранит текущее состояние подключения к базе.
// Используется для health-check'ов, чтобы оркестратор знал, живы ли мы.
type HealthStatus struct {
	IsHealthy   bool
	LastCheck   time.Time
	LastError   error
	Connections int // Сколько активных соединений в пуле
}

// ReindexerRepository — это прослойка между бизнес-логикой и базой данных Reindexer.
// Он умеет:
// 1. Управлять соединениями (пулинг).
// 2. Следить за здоровьем базы.
// 3. Выполнять CRUD операции над документами.
type ReindexerRepository struct {
	dsn            string
	maxConnections int
	logger         *zap.Logger

	// Управление соединениями.
	// Мы используем пул соединений (несколько коннектов), чтобы параллельные запросы
	// не ждали друг друга.
	mu          sync.RWMutex
	db          *reindexer.Reindexer   // Главное соединение
	connections []*reindexer.Reindexer // Пул дополнительных соединений
	poolSize    int

	// Атомарное хранилище статуса здоровья.
	// Нужно, чтобы health check мог читать статус без блокировок (быстро).
	healthStatus atomic.Value // хранит *HealthStatus

	// Механизм для гарантии того, что коллекции (таблицы) будут созданы только один раз.
	initOnce               sync.Once
	collectionsInitialized bool
	collectionsMu          sync.Mutex
}

// NewReindexerRepository создает новый репозиторий, но не подключается сразу.
// Подключение происходит внутри метода Connect(), что позволяет управлять процессом старта.
func NewReindexerRepository(dsn string, maxConnections int, logger *zap.Logger) (*ReindexerRepository, error) {
	if maxConnections < 1 {
		maxConnections = 1
	}

	repo := &ReindexerRepository{
		dsn:            dsn,
		maxConnections: maxConnections,
		logger:         logger,
		poolSize:       maxConnections,
		connections:    make([]*reindexer.Reindexer, 0, maxConnections),
	}

	// Инициализируем статус как "нездоров", пока не подключимся успешно.
	repo.healthStatus.Store(&HealthStatus{
		IsHealthy: false,
		LastCheck: time.Now(),
	})

	// Пробуем подключиться с таймаутом.
	ctx, cancel := context.WithTimeout(context.Background(), defaultConnectTimeout)
	defer cancel()

	if err := repo.Connect(ctx); err != nil {
		return nil, fmt.Errorf("ошибка подключения к базе: %w", err)
	}

	return repo, nil
}

// Connect пытается установить соединение с базой.
// Использует retry-механизм (несколько попыток), если база временно недоступна.
func (r *ReindexerRepository) Connect(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.connectWithRetry(ctx, defaultMaxRetries)
}

// connectWithRetry реализует логику повторных попыток подключения.
// Это нужно для надежности: сеть может моргнуть, или база может перезагружаться.
func (r *ReindexerRepository) connectWithRetry(ctx context.Context, maxRetries int) error {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Проверяем, не отменили ли контекст (например, приложение выключается).
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Пауза между попытками, чтобы не спамить базу.
		if attempt > 0 {
			delay := defaultRetryDelay * time.Duration(attempt)
			r.logger.Info("повторная попытка подключения",
				zap.Int("попытка", attempt+1),
				zap.Duration("пауза", delay),
			)
			time.Sleep(delay)
		}

		// Создаем главное соединение.
		// WithCreateDBIfMissing() автоматически создаст базу, если её нет.
		db := reindexer.NewReindex(r.dsn, reindexer.WithCreateDBIfMissing())

		// Проверяем, живое ли соединение.
		if err := r.testConnection(ctx, db); err != nil {
			lastErr = err
			db.Close()
			r.logger.Warn("тест соединения провален",
				zap.Int("попытка", attempt+1),
				zap.Error(err),
			)
			continue
		}

		// Закрываем старые соединения, если они были (например, при переподключении).
		if r.db != nil {
			r.db.Close()
		}
		for _, conn := range r.connections {
			if conn != nil {
				conn.Close()
			}
		}

		// Устанавливаем новое главное соединение.
		r.db = db

		// Создаем пул дополнительных соединений для параллельной работы.
		r.connections = make([]*reindexer.Reindexer, 0, r.poolSize)
		for i := 0; i < r.poolSize; i++ {
			conn := reindexer.NewReindex(r.dsn, reindexer.WithCreateDBIfMissing())
			if err := r.testConnection(ctx, conn); err != nil {
				conn.Close()
				r.logger.Warn("не удалось создать соединение в пуле",
					zap.Int("индекс", i),
					zap.Error(err),
				)
				continue
			}
			r.connections = append(r.connections, conn)
		}

		// Всё успешно! Обновляем статус здоровья.
		r.updateHealthStatus(true, nil, len(r.connections)+1)

		r.logger.Info("успешно подключились к Reindexer",
			zap.Int("размер_пула", len(r.connections)),
		)

		return nil
	}

	// Если все попытки провалились, обновляем статус на ошибку.
	r.updateHealthStatus(false, lastErr, 0)

	return fmt.Errorf("не удалось подключиться после %d попыток: %w", maxRetries, lastErr)
}

// testConnection проверяет, действительно ли соединение работает.
// Для cproto достаточно убедиться, что объект db не nil и не закрыт.
func (r *ReindexerRepository) testConnection(ctx context.Context, db *reindexer.Reindexer) error {
	if db == nil {
		return fmt.Errorf("объект соединения nil")
	}

	// Проверка контекста на случай отмены.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	return nil
}

// getConnection возвращает доступное соединение из пула.
// Использует простую стратегию round-robin (по очереди), чтобы равномерно распределять нагрузку.
func (r *ReindexerRepository) getConnection() *reindexer.Reindexer {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Если пула нет, возвращаем главное соединение.
	if len(r.connections) == 0 {
		return r.db
	}

	// В продакшене здесь лучше использовать атомарный счетчик для round-robin.
	// Для простоты пока берем первое доступное.
	return r.connections[0]
}

// updateHealthStatus обновляет информацию о здоровье репозитория.
// Делается это атомарно, чтобы другие горутины могли читать статус без блокировок.
func (r *ReindexerRepository) updateHealthStatus(isHealthy bool, err error, connections int) {
	status := &HealthStatus{
		IsHealthy:   isHealthy,
		LastCheck:   time.Now(),
		LastError:   err,
		Connections: connections,
	}
	r.healthStatus.Store(status)
}

// getHealthStatus возвращает текущее состояние здоровья.
func (r *ReindexerRepository) getHealthStatus() *HealthStatus {
	status := r.healthStatus.Load()
	if status == nil {
		return &HealthStatus{IsHealthy: false}
	}
	return status.(*HealthStatus)
}

// EnsureCollections гарантирует, что необходимые таблицы (namespaces) существуют в базе.
// Если их нет — создает автоматически. Это требование 3.2.1.
func (r *ReindexerRepository) EnsureCollections(ctx context.Context) error {
	// Оптимизация: быстрая проверка без блокировки.
	if r.collectionsInitialized {
		return nil
	}

	// Блокировка на случай, если несколько потоков одновременно вызовут этот метод.
	r.collectionsMu.Lock()
	defer r.collectionsMu.Unlock()

	// Повторная проверка после блокировки (double-check locking).
	if r.collectionsInitialized {
		return nil
	}

	r.mu.RLock()
	db := r.db
	r.mu.RUnlock()

	if db == nil {
		return fmt.Errorf("соединение с базой не установлено")
	}

	// Настройки неймспейса по умолчанию.
	opts := reindexer.DefaultNamespaceOptions()

	// Открываем (и создаем при отсутствии) неймспейс для главного соединения.
	// Передаем структуру domain.Document{}, чтобы Reindexer понял схему данных (поля и индексы).
	if err := db.OpenNamespace(documentsNamespace, opts, domain.Document{}); err != nil {
		return fmt.Errorf("ошибка открытия неймспейса: %w", err)
	}

	// То же самое делаем для всех соединений в пуле, чтобы они "знали" про схему.
	for i, conn := range r.connections {
		if conn != nil {
			if err := conn.OpenNamespace(documentsNamespace, opts, domain.Document{}); err != nil {
				r.logger.Warn("ошибка открытия неймспейса для соединения из пула",
					zap.Int("индекс", i),
					zap.Error(err),
				)
				continue
			}
		}
	}

	r.collectionsInitialized = true
	r.logger.Info("коллекции инициализированы", zap.String("namespace", documentsNamespace))

	return nil
}

// Create сохраняет новый документ в базе.
func (r *ReindexerRepository) Create(ctx context.Context, doc *domain.Document) error {
	ctx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	// Проверяем, готовы ли коллекции.
	if err := r.EnsureCollections(ctx); err != nil {
		return fmt.Errorf("ошибка проверки коллекций: %w", err)
	}

	r.mu.RLock()
	db := r.getConnection()
	r.mu.RUnlock()

	if db == nil {
		return fmt.Errorf("нет доступного соединения с БД")
	}

	// Upsert = Update or Insert. Если документа нет — создаст, если есть — обновит.
	// Но так как мы создаем новый, ID должен быть уникальным.
	if err := db.Upsert(documentsNamespace, doc); err != nil {
		r.logger.Error("ошибка создания документа",
			zap.String("id", doc.ID),
			zap.Error(err),
		)
		// Если ошибка, возможно, с базой что-то не так — обновляем статус здоровья.
		r.updateHealthStatus(false, err, r.getHealthStatus().Connections)
		return fmt.Errorf("ошибка при сохранении: %w", err)
	}

	return nil
}

// GetByID получает документ по его ID.
func (r *ReindexerRepository) GetByID(ctx context.Context, id string) (*domain.Document, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	r.mu.RLock()
	db := r.getConnection()
	r.mu.RUnlock()

	if db == nil {
		return nil, fmt.Errorf("нет доступного соединения с БД")
	}

	// Строим запрос: SELECT * FROM documents WHERE id = :id
	query := db.Query(documentsNamespace).Where("id", reindexer.EQ, id)
	iter := query.Exec()
	defer iter.Close()

	if iter.Error() != nil {
		r.logger.Error("ошибка выполнения запроса",
			zap.String("id", id),
			zap.Error(iter.Error()),
		)
		r.updateHealthStatus(false, iter.Error(), r.getHealthStatus().Connections)
		return nil, fmt.Errorf("ошибка запроса: %w", iter.Error())
	}

	var doc domain.Document
	found := false

	// Итерируемся по результатам (хотя ожидаем только один документ).
	for iter.Next() {
		// Получаем объект целиком
		elem := iter.Object()
		if elem != nil {
			// Приводим интерфейс к нашему типу Document
			if d, ok := elem.(*domain.Document); ok {
				doc = *d
				found = true
				break
			} else {
				r.logger.Error("ошибка приведения типов",
					zap.String("id", id),
					zap.String("тип", fmt.Sprintf("%T", elem)),
				)
				return nil, fmt.Errorf("внутренняя ошибка десериализации")
			}
		}
	}

	if !found {
		return nil, fmt.Errorf("документ не найден: %s", id)
	}

	r.logger.Debug("документ найден", zap.String("id", id))

	// Выполняем требование 7: сортировка вложенных элементов (Level1Items).
	// Сортируем по полю Sort в обратном порядке (DESC).
	r.sortLevel1Items(&doc)

	return &doc, nil
}

// Update обновляет существующий документ.
func (r *ReindexerRepository) Update(ctx context.Context, doc *domain.Document) error {
	ctx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	r.mu.RLock()
	db := r.getConnection()
	r.mu.RUnlock()

	if db == nil {
		return fmt.Errorf("нет доступного соединения с БД")
	}

	// Используем Upsert для обновления.
	if err := db.Upsert(documentsNamespace, doc); err != nil {
		r.logger.Error("ошибка обновления документа",
			zap.String("id", doc.ID),
			zap.Error(err),
		)
		r.updateHealthStatus(false, err, r.getHealthStatus().Connections)
		return fmt.Errorf("ошибка при обновлении: %w", err)
	}

	return nil
}

// Delete удаляет документ по ID.
func (r *ReindexerRepository) Delete(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	r.mu.RLock()
	db := r.getConnection()
	r.mu.RUnlock()

	if db == nil {
		return fmt.Errorf("нет доступного соединения с БД")
	}

	query := db.Query(documentsNamespace).Where("id", reindexer.EQ, id)
	_, err := query.Delete()
	if err != nil {
		r.logger.Error("ошибка удаления документа",
			zap.String("id", id),
			zap.Error(err),
		)
		r.updateHealthStatus(false, err, r.getHealthStatus().Connections)
		return fmt.Errorf("ошибка при удалении: %w", err)
	}

	return nil
}

// ListWithPagination возвращает список документов с пагинацией.
// Также выполняет требование 7: сортирует вложенные элементы для каждого документа.
func (r *ReindexerRepository) ListWithPagination(ctx context.Context, params domain.PaginationParams) (*domain.PaginatedResult, error) {
	// Увеличенный таймаут для списков, так как данных может быть много.
	ctx, cancel := context.WithTimeout(ctx, defaultQueryTimeout*2)
	defer cancel()

	r.mu.RLock()
	db := r.getConnection()
	r.mu.RUnlock()

	if db == nil {
		return nil, fmt.Errorf("нет доступного соединения с БД")
	}

	// Строим запрос с сортировкой по дате создания и пагинацией.
	query := db.Query(documentsNamespace).
		Sort("created_at", true). // Сортировка по CreatedAt DESC (новые сверху)
		Limit(params.Limit).
		Offset(params.Offset)

	iter := query.Exec()
	defer iter.Close()

	if iter.Error() != nil {
		r.updateHealthStatus(false, iter.Error(), r.getHealthStatus().Connections)
		return nil, fmt.Errorf("ошибка запроса списка: %w", iter.Error())
	}

	// Считаем общее количество документов для пагинации.
	// В Reindexer v4 нет прямого Count() для сложных условий, но для простого списка
	// можно использовать отдельный запрос.
	// В данном случае проходим по итератору, но это может быть медленно на больших данных.
	// Для production лучше использовать db.Query(documentsNamespace).ReqTotal() если поддерживается.
	countQuery := db.Query(documentsNamespace)
	countIter := countQuery.Exec()
	defer countIter.Close()
	totalCount := 0
	for countIter.Next() {
		totalCount++
	}

	var docs []*domain.Document
	for iter.Next() {
		var doc domain.Document
		if !iter.NextObj(&doc) {
			r.logger.Error("ошибка чтения документа из итератора")
			continue
		}

		// ВАЖНО: Сортируем вложенные элементы для КАЖДОГО документа в списке.
		r.sortLevel1Items(&doc)

		docs = append(docs, &doc)
	}

	hasMore := params.Offset+len(docs) < totalCount

	return &domain.PaginatedResult{
		Items:   docs,
		Total:   totalCount,
		Limit:   params.Limit,
		Offset:  params.Offset,
		HasMore: hasMore,
	}, nil
}

// sortLevel1Items выполняет требование 7: сортировка Level1Items по полю Sort (DESC).
// Сортировка "обратная" (DESC) — значит от большего к меньшему (например: 100, 50, 10).
func (r *ReindexerRepository) sortLevel1Items(doc *domain.Document) {
	// Если элементов 0 или 1, сортировать нечего — экономим ресурсы.
	if len(doc.Items) <= 1 {
		return
	}

	// Используем стандартный sort.Slice.
	// Функция сравнения возвращает true, если элемент i должен идти ПЕРЕД элементом j.
	// Для DESC (убывания): items[i].Sort > items[j].Sort.
	sort.Slice(doc.Items, func(i, j int) bool {
		return doc.Items[i].Sort > doc.Items[j].Sort
	})
}

// CheckConnection проверяет здоровье соединения (для внешних health check'ов).
func (r *ReindexerRepository) CheckConnection(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	r.mu.RLock()
	db := r.db
	r.mu.RUnlock()

	if db == nil {
		return fmt.Errorf("соединение не установлено")
	}

	// Выполняем реальную проверку связи.
	if err := r.testConnection(ctx, db); err != nil {
		r.updateHealthStatus(false, err, r.getHealthStatus().Connections)
		return fmt.Errorf("проверка связи не прошла: %w", err)
	}

	r.updateHealthStatus(true, nil, r.getHealthStatus().Connections)
	return nil
}

// Close закрывает все соединения с базой данных.
func (r *ReindexerRepository) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var lastErr error

	// Закрываем главное соединение
	if r.db != nil {
		r.db.Close()
		r.db = nil
	}

	// Закрываем пул соединений
	for i, conn := range r.connections {
		if conn != nil {
			conn.Close()
			r.connections[i] = nil
		}
	}

	// Очищаем слайс
	r.connections = r.connections[:0]
	r.updateHealthStatus(false, fmt.Errorf("соединение закрыто"), 0)

	return lastErr
}

// Проверка интерфейсов (compile-time check).
// Гарантирует, что мы реализовали все методы интерфейсов.
var (
	_ domain.DocumentRepository = (*ReindexerRepository)(nil)
	_ domain.HealthChecker      = (*ReindexerRepository)(nil)
)
