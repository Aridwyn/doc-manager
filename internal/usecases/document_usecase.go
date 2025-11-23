package usecases

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/your-org/involta/internal/domain"
)

// DocumentUsecase отвечает за бизнес-логику работы с документами.
// Он связывает воедино базу данных, кэш и внешнюю обработку (processor).
// Главные задачи:
// 1. Кэширование "горячих" данных (Cache-Aside).
// 2. Контроль нагрузки (Rate Limiting).
// 3. Асинхронная обработка документов.
type DocumentUsecase struct {
	repo      domain.DocumentRepository
	cache     domain.Cache
	processor domain.DocumentProcessor
	logger    *zap.Logger

	// Управление конкурентностью
	wg              sync.WaitGroup
	processingQueue chan *domain.Document // Канал для фоновой обработки
	rateLimiter     *RateLimiter          // Семафор для ограничения одновременных операций

	// Конфигурация
	maxConcurrentOps int
}

// RateLimiter — простой ограничитель нагрузки на семафоре.
// Не дает запустить больше N операций одновременно, защищая ресурсы сервера.
type RateLimiter struct {
	semaphore     chan struct{}
	maxConcurrent int
}

// NewRateLimiter создает ограничитель с буфером на maxConcurrent запросов.
func NewRateLimiter(maxConcurrent int) *RateLimiter {
	if maxConcurrent < 1 {
		maxConcurrent = 10
	}
	return &RateLimiter{
		semaphore:     make(chan struct{}, maxConcurrent),
		maxConcurrent: maxConcurrent,
	}
}

// Acquire пытается получить разрешение на работу.
// Если лимит исчерпан — блокируется и ждет, пока кто-то не освободит место.
// Если контекст отменен (например, таймаут запроса) — возвращает ошибку.
func (rl *RateLimiter) Acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case rl.semaphore <- struct{}{}:
		return nil
	}
}

// Release освобождает место для следующих запросов.
func (rl *RateLimiter) Release() {
	select {
	case <-rl.semaphore:
	default:
		// Защита от паники при попытке освободить пустой семафор
	}
}

// NewDocumentUsecase создает и запускает usecase.
// Сразу стартует фоновый процессор для обработки очереди.
func NewDocumentUsecase(
	repo domain.DocumentRepository,
	cache domain.Cache,
	processor domain.DocumentProcessor,
	logger *zap.Logger,
	maxConcurrentOps int,
) *DocumentUsecase {
	if maxConcurrentOps < 1 {
		maxConcurrentOps = 10
	}

	usecase := &DocumentUsecase{
		repo:             repo,
		cache:            cache,
		processor:        processor,
		logger:           logger,
		processingQueue:  make(chan *domain.Document, 100),
		rateLimiter:      NewRateLimiter(maxConcurrentOps),
		maxConcurrentOps: maxConcurrentOps,
	}

	// Запускаем фонового воркера
	usecase.startBackgroundProcessor()

	return usecase
}

// startBackgroundProcessor запускает горутину, которая разгребает очередь processingQueue.
// Работает пакетами (batch) для эффективности — обрабатывает до 10 документов за раз.
func (u *DocumentUsecase) startBackgroundProcessor() {
	u.wg.Add(1)
	go func() {
		defer u.wg.Done()

		// Буфер для накопления пачки документов
		batch := make([]*domain.Document, 0, 10)
		
		// Тикер, чтобы не ждать вечно, если пакет не набирается полностью
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case doc, ok := <-u.processingQueue:
				if !ok {
					// Канал закрыт (Shutdown) — дорабатываем остатки и выходим
					if len(batch) > 0 {
						u.processBatch(context.Background(), batch)
					}
					return
				}

				batch = append(batch, doc)

				// Если пакет полный — отправляем в обработку
				if len(batch) >= 10 {
					u.processBatch(context.Background(), batch)
					batch = batch[:0] // Очищаем буфер
				}

			case <-ticker.C:
				// Прошло 5 секунд — обрабатываем то, что накопилось, не ждем полного пакета
				if len(batch) > 0 {
					u.processBatch(context.Background(), batch)
					batch = batch[:0]
				}
			}
		}
	}()
}

// processBatch обрабатывает пачку документов.
func (u *DocumentUsecase) processBatch(ctx context.Context, documents []*domain.Document) {
	if len(documents) == 0 {
		return
	}

	u.wg.Add(1)
	go func() {
		defer u.wg.Done()

		// Соблюдаем лимиты даже в фоновой работе
		if err := u.rateLimiter.Acquire(ctx); err != nil {
			u.logger.Error("не удалось получить слот rate limiter",
				zap.Error(err),
			)
			return
		}
		defer u.rateLimiter.Release()

		// Тяжелая работа по обработке документов
		results, err := u.processor.ProcessDocuments(ctx, documents)
		if err != nil {
			u.logger.Error("ошибка пакетной обработки",
				zap.Int("количество", len(documents)),
				zap.Error(err),
			)
			return
		}

		// Статистика
		successCount := 0
		for _, result := range results {
			if result.Error == nil && result.Result.Status == domain.ProcessingStatusSuccess {
				successCount++
			}
		}

		u.logger.Info("пакет обработан",
			zap.Int("всего", len(documents)),
			zap.Int("успешно", successCount),
		)
	}()
}

// GetDocument получает документ по ID.
// Реализует паттерн Cache-Aside:
// 1. Ищем в кэше. Нашли -> вернули.
// 2. Не нашли -> идем в базу.
// 3. Нашли в базе -> асинхронно кладем в кэш -> вернули результат.
func (u *DocumentUsecase) GetDocument(ctx context.Context, id string) (*domain.Document, error) {
	cacheKey := fmt.Sprintf("document:%s", id)
	
	// 1. Проверка кэша (быстрый путь)
	if cached, ok := u.cache.Get(ctx, cacheKey); ok {
		if doc, ok := cached.(*domain.Document); ok {
			u.logger.Debug("попадание в кэш", zap.String("id", id))
			return doc, nil
		}
	}

	// Ограничение нагрузки перед походом в базу
	if err := u.rateLimiter.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("превышен лимит запросов: %w", err)
	}
	defer u.rateLimiter.Release()

	// 2. Запрос в базу (медленный путь)
	doc, err := u.repo.GetByID(ctx, id)
	if err != nil {
		u.logger.Error("не удалось получить документ из БД",
			zap.String("id", id),
			zap.Error(err),
		)
		return nil, err
	}

	// Обработка документа перед отдачей (синхронно для единичного запроса)
	processedDoc, err := u.processSingleDocument(ctx, doc)
	if err != nil {
		u.logger.Error("ошибка обработки документа",
			zap.String("id", id),
			zap.Error(err),
		)
		return nil, err
	}

	// 3. Сохранение в кэш (асинхронно, чтобы не задерживать ответ пользователю)
	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
		// Короткий таймаут для кэша, это не критичная операция
		cacheCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		if err := u.cache.Set(cacheCtx, cacheKey, processedDoc); err != nil {
			u.logger.Warn("не удалось закэшировать документ",
				zap.String("id", id),
				zap.Error(err),
			)
		}
	}()

	return processedDoc, nil
}

// scheduleAsyncProcessing ставит документ в очередь на обработку.
// Если очередь полна — пропускает, чтобы не тормозить основной поток (non-blocking).
func (u *DocumentUsecase) scheduleAsyncProcessing(doc *domain.Document) {
	select {
	case u.processingQueue <- doc:
		// Успешно добавили в очередь
	default:
		// Очередь переполнена, пропускаем (в продакшене можно писать метрику drop_rate)
		u.logger.Warn("очередь обработки полна, пропускаем",
			zap.String("doc_id", doc.ID),
		)
	}
}

// processSingleDocument — обертка для обработки одного документа.
func (u *DocumentUsecase) processSingleDocument(ctx context.Context, doc *domain.Document) (*domain.Document, error) {
	if doc == nil {
		return nil, fmt.Errorf("документ nil")
	}
	processedDocs, err := u.processDocumentsSync(ctx, []*domain.Document{doc})
	if err != nil {
		return nil, err
	}
	if len(processedDocs) == 0 || processedDocs[0] == nil {
		return nil, fmt.Errorf("процессор вернул пустой результат")
	}
	return processedDocs[0], nil
}

// processDocumentsSync выполняет синхронную обработку списка документов.
// Гарантирует порядок результатов.
func (u *DocumentUsecase) processDocumentsSync(ctx context.Context, documents []*domain.Document) ([]*domain.Document, error) {
	if len(documents) == 0 {
		return []*domain.Document{}, nil
	}

	results, err := u.processor.ProcessDocuments(ctx, documents)
	if err != nil {
		return nil, err
	}
	if len(results) != len(documents) {
		return nil, fmt.Errorf("несовпадение количества результатов обработки")
	}

	// Распаковываем результаты
	processed := make([]*domain.Document, len(results))
	for i, result := range results {
		if result == nil {
			return nil, fmt.Errorf("получен nil результат по индексу %d", i)
		}
		if result.Error != nil {
			return nil, result.Error
		}
		if result.Document == nil {
			return nil, fmt.Errorf("получен nil документ по индексу %d", i)
		}
		processed[i] = result.Document
	}

	return processed, nil
}

// ListDocuments возвращает список документов.
// Выполняет требование 7 (сортировка) на уровне репозитория, здесь — только обработка.
func (u *DocumentUsecase) ListDocuments(ctx context.Context, params domain.PaginationParams) (*domain.PaginatedResult, error) {
	// Лимитируем нагрузку
	if err := u.rateLimiter.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("превышен лимит запросов: %w", err)
	}
	defer u.rateLimiter.Release()

	// Запрос в репозиторий
	result, err := u.repo.ListWithPagination(ctx, params)
	if err != nil {
		u.logger.Error("ошибка получения списка",
			zap.Error(err),
		)
		return nil, err
	}

	// Если есть документы, прогоняем их через процессор перед отдачей
	if len(result.Items) > 0 {
		processedDocs, err := u.processDocumentsSync(ctx, result.Items)
		if err != nil {
			u.logger.Error("ошибка обработки списка документов",
				zap.Error(err),
			)
			return nil, err
		}
		result.Items = processedDocs
	}

	return result, nil
}

// CreateDocument создает документ.
// Также запускает асинхронную инвалидацию кэша и фоновую обработку.
func (u *DocumentUsecase) CreateDocument(ctx context.Context, doc *domain.Document) error {
	if err := u.rateLimiter.Acquire(ctx); err != nil {
		return fmt.Errorf("превышен лимит запросов: %w", err)
	}
	defer u.rateLimiter.Release()

	if doc.CreatedAt.IsZero() {
		doc.CreatedAt = time.Now()
	}

	// Сохраняем в БД
	if err := u.repo.Create(ctx, doc); err != nil {
		u.logger.Error("ошибка создания в БД",
			zap.String("id", doc.ID),
			zap.Error(err),
		)
		return err
	}

	// Инвалидируем кэш (удаляем, если вдруг там что-то было)
	// Делаем это в фоне, чтобы клиент быстрее получил ответ OK
	u.invalidateCacheAsync(ctx, doc.ID)

	// Ставим в очередь на доп. обработку
	u.scheduleAsyncProcessing(doc)

	u.logger.Info("документ создан",
		zap.String("id", doc.ID),
		zap.String("name", doc.Name),
	)

	return nil
}

// UpdateDocument обновляет документ.
// Принцип тот же: БД -> Инвалидация кэша -> Фоновая обработка.
func (u *DocumentUsecase) UpdateDocument(ctx context.Context, doc *domain.Document) error {
	if err := u.rateLimiter.Acquire(ctx); err != nil {
		return fmt.Errorf("превышен лимит запросов: %w", err)
	}
	defer u.rateLimiter.Release()

	if err := u.repo.Update(ctx, doc); err != nil {
		u.logger.Error("ошибка обновления в БД",
			zap.String("id", doc.ID),
			zap.Error(err),
		)
		return err
	}

	// Кэш удаляем, чтобы при следующем запросе данные перечитались из БД
	u.invalidateCacheAsync(ctx, doc.ID)

	u.scheduleAsyncProcessing(doc)

	u.logger.Info("документ обновлен", zap.String("id", doc.ID))

	return nil
}

// DeleteDocument удаляет документ и чистит кэш.
func (u *DocumentUsecase) DeleteDocument(ctx context.Context, id string) error {
	if err := u.rateLimiter.Acquire(ctx); err != nil {
		return fmt.Errorf("превышен лимит запросов: %w", err)
	}
	defer u.rateLimiter.Release()

	if err := u.repo.Delete(ctx, id); err != nil {
		u.logger.Error("ошибка удаления из БД",
			zap.String("id", id),
			zap.Error(err),
		)
		return err
	}

	u.invalidateCacheAsync(ctx, id)

	u.logger.Info("документ удален", zap.String("id", id))

	return nil
}

// invalidateCacheAsync запускает горутину для удаления записи из кэша.
// Это позволяет не тормозить основной поток ошибками Redis/Memcached (в нашем случае in-memory).
func (u *DocumentUsecase) invalidateCacheAsync(ctx context.Context, id string) {
	u.wg.Add(1)
	go func() {
		defer u.wg.Done()

		cacheCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		cacheKey := fmt.Sprintf("document:%s", id)
		if err := u.cache.Delete(cacheCtx, cacheKey); err != nil {
			u.logger.Warn("не удалось очистить кэш",
				zap.String("id", id),
				zap.Error(err),
			)
		}
	}()
}

// Shutdown корректно останавливает работу usecase'а.
func (u *DocumentUsecase) Shutdown() {
	// Закрываем канал — сигнал воркерам остановиться
	close(u.processingQueue)

	// Ждем, пока все фоновые задачи (инвалидация, обработка) закончатся
	u.wg.Wait()

	u.logger.Info("бизнес-логика остановлена")
}
