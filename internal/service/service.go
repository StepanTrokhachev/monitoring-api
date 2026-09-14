package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"

	"anomaly-monitor/internal/config"
	"anomaly-monitor/internal/datasource"
	"anomaly-monitor/internal/model"
	"anomaly-monitor/internal/notifier"
	"anomaly-monitor/internal/repository"
)

// EventService — центральная бизнес-логика.
// Запускает polling, обогащает события через ML-сервис,
// сохраняет в репозиторий, рассылает алерты.
type EventService struct {
	cfg        *config.Config
	src        datasource.Reader
	repo       repository.EventRepo
	enriched   *datasource.EnrichedStore
	mailer     *notifier.Mailer
	log        *zap.Logger
	onNew      func([]model.AccessEvent)
	mu         sync.Mutex
	threshold  float64
	recipients string
}

func New(
	cfg *config.Config,
	src datasource.Reader,
	repo repository.EventRepo,
	enriched *datasource.EnrichedStore,
	mailer *notifier.Mailer,
	log *zap.Logger,
) *EventService {
	return &EventService{
		cfg:        cfg,
		src:        src,
		repo:       repo,
		enriched:   enriched,
		mailer:     mailer,
		log:        log,
		threshold:  cfg.AlertThreshold,
		recipients: cfg.AlertRecipients,
	}
}

// SetNewEventCallback регистрирует callback, который вызывается при появлении новых событий.
// Используется WebSocket-хабом для push-уведомлений.
func (s *EventService) SetNewEventCallback(fn func([]model.AccessEvent)) {
	s.onNew = fn
}

// UpdateThreshold обновляет порог алертов в runtime (из API).
func (s *EventService) UpdateThreshold(threshold float64, recipients string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.threshold = threshold
	s.recipients = recipients
}

// GetThreshold возвращает текущие настройки алертов.
func (s *EventService) GetThreshold() (float64, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.threshold, s.recipients
}

// pollBatchSize — сколько событий обрабатываем за один вызов Poll.
// Защищает от загрузки всего CSV (1М+ строк) за один раз.
const pollBatchSize = 1000

// Poll читает новые события из источника, обогащает через ML, сохраняет.
// Вызывается планировщиком каждые N секунд.
// При большом объёме данных обрабатывает не более pollBatchSize за вызов —
// следующий вызов подхватит остаток.
func (s *EventService) Poll(ctx context.Context) error {
	lastID := s.repo.GetLastID()

	allEvents, err := s.src.ReadSince(lastID)
	if err != nil {
		return fmt.Errorf("datasource read: %w", err)
	}
	if len(allEvents) == 0 {
		s.log.Info("poll: новых событий нет", zap.Int64("last_id", lastID))
		return nil
	}

	// Ограничиваем размер батча за один poll
	events := allEvents
	if len(events) > pollBatchSize {
		s.log.Info("ограничиваем poll до первых событий",
			zap.Int("total_available", len(allEvents)),
			zap.Int("processing_now", pollBatchSize),
		)
		events = allEvents[:pollBatchSize]
	}

	s.log.Info("новые события", zap.Int("count", len(events)))

	// Обогащаем через ML-сервис (внутри разбивается на чанки по mlChunkSize)
	enriched, err := s.enrichBatch(ctx, events, true)
	if err != nil {
		// Событие без оценки аномальности бесполезно и искажает статистику:
		// сохранённое с score = 0 оно будет учтено как заведомо нормальное.
		// Пропускаем цикл целиком — lastID не сдвигается, и следующий
		// цикл повторит обработку этих же событий.
		s.log.Error("обогащение не выполнено, цикл пропущен",
			zap.Int("count", len(events)),
			zap.Error(err),
		)
		return fmt.Errorf("ml enrich: %w", err)
	}

	// Сохраняем в репозиторий
	if err := s.repo.Save(enriched); err != nil {
		return fmt.Errorf("repo save: %w", err)
	}

	// Сохраняем обогащённые данные на диск
	if s.enriched != nil {
		if err := s.enriched.Append(enriched); err != nil {
			s.log.Warn("enriched store append failed", zap.Error(err))
		}
	}

	// Push в WebSocket
	if s.onNew != nil {
		s.onNew(enriched)
	}

	// Отправляем алерты
	s.sendAlerts(enriched)

	// Асинхронно пересчитываем профили пользователей в ML-сервисе.
	// Не блокируем Poll — запускаем в горутине.
	go s.triggerProfilesRebuild()

	return nil
}

// triggerProfilesRebuild вызывает POST /profiles/rebuild на ML-сервисе.
// ML пересчитывает user_profiles.pkl из enriched_log.csv в фоне.
func (s *EventService) triggerProfilesRebuild() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.cfg.MLServiceURL+"/profiles/rebuild", nil)
	if err != nil {
		s.log.Warn("profiles/rebuild: create request failed", zap.Error(err))
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.log.Warn("profiles/rebuild: request failed", zap.Error(err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		s.log.Info("profiles/rebuild: запущен пересчёт профилей в ML")
	} else {
		body, _ := io.ReadAll(resp.Body)
		s.log.Warn("profiles/rebuild: неожиданный статус",
			zap.Int("status", resp.StatusCode),
			zap.String("body", string(body)),
		)
	}
}

// mlChunkSize — размер одного батча к ML-сервису.
const mlChunkSize = 200

// mlWorkers — количество параллельных горутин для отправки чанков.
const mlWorkers = 4

// computeBursts вычисляет burst_10min для каждого события среза:
// число событий того же пользователя за 10 минут, предшествующих событию,
// включая само событие. Границы окна — [t-10min, t], как в обучающем
// конвейере (features.build_features_batch после перевода на окно назад).
//
// withRepoHistory управляет учётом уже сохранённой истории:
//   - true  (Poll): события батча ещё не в репозитории, историю берём оттуда;
//   - false (ReEnrich): срез содержит весь журнал целиком, и те же события
//     уже лежат в репозитории — обращение к нему дало бы двойной счёт.
func (s *EventService) computeBursts(events []model.AccessEvent, withRepoHistory bool) []int {
	bursts := make([]int, len(events))
	// Метки времени уже просмотренных событий батча по пользователям.
	seen := make(map[int64][]time.Time, len(events))

	for i, ev := range events {
		from := ev.AccessDate.Add(-10 * time.Minute)

		count := 0
		if withRepoHistory {
			count = s.repo.CountUserEventsInWindow(ev.UserID, from, ev.AccessDate)
		}

		// Предшествующие события того же пользователя внутри текущего среза.
		for _, t := range seen[ev.UserID] {
			if !t.Before(from) && !t.After(ev.AccessDate) {
				count++
			}
		}

		bursts[i] = count + 1 // само событие
		seen[ev.UserID] = append(seen[ev.UserID], ev.AccessDate)
	}
	return bursts
}

// enrichBatch параллельно обогащает события через ML (worker pool).
// burst_10min считается здесь, до разбиения на чанки: значение зависит от
// предшествующих событий пользователя, и внутри отдельного чанка они видны
// лишь частично.
func (s *EventService) enrichBatch(ctx context.Context, events []model.AccessEvent, withRepoHistory bool) ([]model.AccessEvent, error) {
	total := len(events)
	numChunks := (total + mlChunkSize - 1) / mlChunkSize

	s.log.Info("ML обогащение",
		zap.Int("total", total),
		zap.Int("chunks", numChunks),
		zap.Int("workers", mlWorkers),
	)

	bursts := s.computeBursts(events, withRepoHistory)

	type job struct{ start, end int }
	jobs := make(chan job, numChunks)

	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error

	// Запускаем mlWorkers горутин
	for w := 0; w < mlWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if err := s.enrichChunk(ctx, events[j.start:j.end], bursts[j.start:j.end]); err != nil {
					s.log.Warn("chunk failed",
						zap.Int("start", j.start),
						zap.Int("end", j.end),
						zap.Error(err),
					)
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
				}
			}
		}()
	}

	// Раздаём задачи
	for start := 0; start < total; start += mlChunkSize {
		end := start + mlChunkSize
		if end > total {
			end = total
		}
		jobs <- job{start, end}
	}
	close(jobs)
	wg.Wait()

	return events, firstErr
}

// enrichChunk отправляет один чанк в ML и записывает результаты обратно в срез.
// bursts — заранее посчитанные значения burst_10min, параллельные events.
func (s *EventService) enrichChunk(ctx context.Context, events []model.AccessEvent, bursts []int) error {
	type mlEvent struct {
		IP         string `json:"ip"`
		UserID     int64  `json:"user_id"`
		TaskID     int64  `json:"task_id"`
		AccessDate string `json:"access_date"`
		Burst10Min int    `json:"burst_10min"`
	}
	type mlRequest struct {
		Events []mlEvent `json:"events"`
	}
	type mlResult struct {
		Score       float64 `json:"score"`
		IsAnomaly   bool    `json:"is_anomaly"`
		BlockAction string  `json:"block_action"`
	}
	type mlResponse struct {
		Total     int        `json:"total"`
		Anomalies int        `json:"anomalies"`
		Results   []mlResult `json:"results"`
	}

	mlEvs := make([]mlEvent, len(events))
	for i, ev := range events {
		mlEvs[i] = mlEvent{
			IP:         ev.IP,
			UserID:     ev.UserID,
			TaskID:     ev.TaskID,
			AccessDate: ev.AccessDate.Format("2006-01-02T15:04:05"),
			Burst10Min: bursts[i],
		}
	}

	body, _ := json.Marshal(mlRequest{Events: mlEvs})

	ctx2, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx2, http.MethodPost,
		s.cfg.MLServiceURL+"/predict/batch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var mlResp mlResponse
	if err := json.Unmarshal(respBody, &mlResp); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if len(mlResp.Results) != len(events) {
		return fmt.Errorf("ожидали %d, получили %d результатов", len(events), len(mlResp.Results))
	}

	for i := range events {
		events[i].AnomalyScore = mlResp.Results[i].Score
		events[i].IsAnomaly = mlResp.Results[i].IsAnomaly
		events[i].BlockAction = mlResp.Results[i].BlockAction
	}
	return nil
}

// sendAlerts рассылает письма по событиям с score >= threshold.
func (s *EventService) sendAlerts(events []model.AccessEvent) {
	s.mu.Lock()
	thr := s.threshold
	rcpt := s.recipients
	s.mu.Unlock()

	if rcpt == "" {
		return
	}

	for _, ev := range events {
		if ev.AnomalyScore >= thr {
			if err := s.mailer.SendAlert(ev, thr, rcpt); err != nil {
				s.log.Warn("не удалось отправить алерт",
					zap.Int64("user_id", ev.UserID),
					zap.Error(err),
				)
				continue
			}
			s.log.Info("алерт отправлен",
				zap.Int64("user_id", ev.UserID),
				zap.Float64("score", ev.AnomalyScore),
				zap.String("to", rcpt),
			)
		}
	}
}

// ─── Методы для API ────────────────────────────────────────────────────────

func (s *EventService) ListEvents(f model.FilterParams) (model.PaginatedEvents, error) {
	return s.repo.List(f)
}

func (s *EventService) ListUserStats() ([]model.UserStat, error) {
	return s.repo.ListUserStats()
}

func (s *EventService) GetUserStats(userID int64) (model.UserStat, error) {
	return s.repo.GetUserStats(userID)
}

// GetUserHourly возвращает почасовую агрегацию по пользователю из всей истории.
func (s *EventService) GetUserHourly(userID int64) ([]model.HourlyStat, error) {
	return s.repo.GetUserHourly(userID), nil
}

// GetSummary возвращает сводную статистику для главной страницы.
func (s *EventService) GetSummary() (model.Summary, error) {
	return s.repo.GetSummary()
}

// GetEvent возвращает одно событие по ID.
func (s *EventService) GetEvent(id int64) (model.AccessEvent, error) {
	return s.repo.GetByID(id)
}

// ReEnrich переобогащает все события из enriched_log.csv с актуальными профилями.
// Запускается вручную через POST /api/v1/reenrich после пересчёта профилей ML.
// Результат: enriched_log.csv перезаписывается с новыми score/is_anomaly.
func (s *EventService) ReEnrich() error {
	if s.enriched == nil {
		return fmt.Errorf("enriched store не инициализирован")
	}

	s.log.Info("ReEnrich: загружаем все события из enriched_log.csv...")
	events, err := s.enriched.LoadAll()
	if err != nil {
		return fmt.Errorf("load enriched: %w", err)
	}
	if len(events) == 0 {
		s.log.Warn("ReEnrich: нет данных для переобогащения")
		return nil
	}

	s.log.Info("ReEnrich: начинаем переобогащение",
		zap.Int("total", len(events)),
		zap.Int("chunk_size", mlChunkSize),
	)

	// Обогащаем с новыми профилями (параллельно по чанкам).
	// withRepoHistory = false: срез уже содержит весь журнал целиком.
	ctx := context.Background()
	enriched, err := s.enrichBatch(ctx, events, false)
	if err != nil {
		s.log.Warn("ReEnrich: ML частично не ответил", zap.Error(err))
		enriched = events // используем что есть
	}

	// Перезаписываем enriched_log.csv полностью
	newPath := s.enriched.Path() + ".tmp"
	tmpStore := datasource.NewEnrichedStore(newPath)
	if err := tmpStore.Append(enriched); err != nil {
		return fmt.Errorf("write tmp: %w", err)
	}

	// Атомарно заменяем файл
	if err := os.Rename(newPath, s.enriched.Path()); err != nil {
		return fmt.Errorf("rename: %w", err)
	}

	// Обновляем in-memory репозиторий
	newRepo := repository.NewMemoryRepo()
	if err := newRepo.Save(enriched); err != nil {
		return fmt.Errorf("repo save: %w", err)
	}
	s.repo = newRepo

	// Push обновлённых аномалий в WebSocket
	var anomEvents []model.AccessEvent
	for _, ev := range enriched {
		if ev.IsAnomaly {
			anomEvents = append(anomEvents, ev)
		}
	}
	if s.onNew != nil && len(anomEvents) > 0 {
		s.onNew(anomEvents)
	}
	s.log.Info("ReEnrich завершён",
		zap.Int("total", len(enriched)),
		zap.Int("anomalies", len(anomEvents)),
	)
	return nil
}
