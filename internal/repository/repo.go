package repository

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"anomaly-monitor/internal/model"
)

// EventRepo — интерфейс хранилища событий.
// MemoryRepo реализует его сейчас (без потери данных между запросами).
// PostgresRepo реализует его когда появится PostgresDSN.
type EventRepo interface {
	Save(events []model.AccessEvent) error
	List(f model.FilterParams) (model.PaginatedEvents, error)
	GetUserStats(userID int64) (model.UserStat, error)
	ListUserStats() ([]model.UserStat, error)
	GetLastID() int64
	GetByID(id int64) (model.AccessEvent, error)
	GetSummary() (model.Summary, error)
	GetUserHourly(userID int64) []model.HourlyStat
}

// ─────────────────────────────────────────────
// In-memory реализация
// ─────────────────────────────────────────────

type MemoryRepo struct {
	mu     sync.RWMutex
	events []model.AccessEvent
	lastID int64
}

func NewMemoryRepo() *MemoryRepo {
	return &MemoryRepo{}
}

func (r *MemoryRepo) Save(events []model.AccessEvent) error {
	if len(events) == 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ev := range events {
		r.events = append(r.events, ev)
		if ev.ID > r.lastID {
			r.lastID = ev.ID
		}
	}
	return nil
}

func (r *MemoryRepo) GetLastID() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastID
}

func (r *MemoryRepo) List(f model.FilterParams) (model.PaginatedEvents, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var filtered []model.AccessEvent
	for _, ev := range r.events {
		if f.UserID != nil && ev.UserID != *f.UserID {
			continue
		}
		if f.TaskID != nil && ev.TaskID != *f.TaskID {
			continue
		}
		if f.IP != nil && !strings.Contains(ev.IP, *f.IP) {
			continue
		}
		if f.OnlyAnom && !ev.IsAnomaly {
			continue
		}
		if f.AccessResult != nil && ev.AccessResult != *f.AccessResult {
			continue
		}
		if f.BlockAction != nil && ev.BlockAction != *f.BlockAction {
			continue
		}
		if f.DateFrom != nil && ev.AccessDate.Before(*f.DateFrom) {
			continue
		}
		if f.DateTo != nil && ev.AccessDate.After(*f.DateTo) {
			continue
		}
		filtered = append(filtered, ev)
	}

	// Сортировка: новые первые
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].AccessDate.After(filtered[j].AccessDate)
	})

	total := len(filtered)
	page := f.Page
	if page < 1 {
		page = 1
	}
	size := f.PageSize
	if size < 1 || size > 500 {
		size = 50
	}

	start := (page - 1) * size
	end := start + size
	if start >= total {
		return model.PaginatedEvents{Total: total, Page: page, PageSize: size, Items: []model.AccessEvent{}}, nil
	}
	if end > total {
		end = total
	}

	return model.PaginatedEvents{
		Total:    total,
		Page:     page,
		PageSize: size,
		Items:    filtered[start:end],
	}, nil
}

func (r *MemoryRepo) GetUserStats(userID int64) (model.UserStat, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.calcStat(userID), nil
}

func (r *MemoryRepo) ListUserStats() ([]model.UserStat, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[int64]struct{})
	for _, ev := range r.events {
		seen[ev.UserID] = struct{}{}
	}

	var stats []model.UserStat
	for uid := range seen {
		stats = append(stats, r.calcStat(uid))
	}
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].AnomalyCount > stats[j].AnomalyCount
	})
	return stats, nil
}

func (r *MemoryRepo) calcStat(userID int64) model.UserStat {
	stat := model.UserStat{UserID: userID}
	ips := make(map[string]struct{})
	tasks := make(map[int64]struct{})
	var lastSeen time.Time

	for _, ev := range r.events {
		if ev.UserID != userID {
			continue
		}
		stat.TotalEvents++
		if ev.AccessResult == 1 {
			stat.AllowedCount++
		} else {
			stat.DeniedCount++
		}
		if ev.IsAnomaly {
			stat.AnomalyCount++
		}
		if ev.AnomalyScore > stat.MaxScore {
			stat.MaxScore = ev.AnomalyScore
		}
		if ev.AccessDate.After(lastSeen) {
			lastSeen = ev.AccessDate
		}
		ips[ev.IP] = struct{}{}
		tasks[ev.TaskID] = struct{}{}
	}

	stat.LastSeen = lastSeen
	stat.UniqueIPs = len(ips)
	stat.UniqueTasks = len(tasks)
	return stat
}

// ─────────────────────────────────────────────
// PostgreSQL-заглушка (раскомментировать при подключении)
// ─────────────────────────────────────────────
//
// type PostgresRepo struct { db *sqlx.DB }
//
// func NewPostgresRepo(dsn string) (*PostgresRepo, error) {
//     db, err := sqlx.Connect("postgres", dsn)
//     if err != nil { return nil, err }
//     return &PostgresRepo{db: db}, nil
// }
//
// Схема таблицы (migrations/001_init.sql):
//   CREATE TABLE access_events (
//       id             BIGSERIAL PRIMARY KEY,
//       ip             VARCHAR(45),
//       hostname       VARCHAR(255),
//       user_id        BIGINT,
//       task_id        BIGINT,
//       access_result  SMALLINT,
//       access_date    TIMESTAMPTZ,
//       anomaly_score  FLOAT DEFAULT 0,
//       is_anomaly     BOOLEAN DEFAULT FALSE,
//       block_action   VARCHAR(16) DEFAULT 'allow'
//   );
//   CREATE INDEX ON access_events(user_id);
//   CREATE INDEX ON access_events(access_date DESC);
//   CREATE INDEX ON access_events(is_anomaly) WHERE is_anomaly = TRUE;

// GetUserHourly возвращает почасовую статистику по конкретному пользователю
// по ВСЕЙ его истории в репозитории (без ограничений page_size).
func (r *MemoryRepo) GetUserHourly(userID int64) []model.HourlyStat {
	r.mu.RLock()
	defer r.mu.RUnlock()

	hourly := make([]model.HourlyStat, 24)
	for h := range hourly {
		hourly[h].Hour = h
	}
	for _, ev := range r.events {
		if ev.UserID != userID {
			continue
		}
		h := ev.AccessDate.Hour()
		hourly[h].Total++
		if ev.IsAnomaly {
			hourly[h].Anomaly++
		}
	}
	return hourly
}

// GetByID возвращает событие по ID.
func (r *MemoryRepo) GetByID(id int64) (model.AccessEvent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, ev := range r.events {
		if ev.ID == id {
			return ev, nil
		}
	}
	return model.AccessEvent{}, fmt.Errorf("event %d not found", id)
}

// GetSummary возвращает сводку для главной страницы.
func (r *MemoryRepo) GetSummary() (model.Summary, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	s := model.Summary{}
	users := make(map[int64]struct{})
	hourly := make([]model.HourlyStat, 24)
	for i := range hourly {
		hourly[i].Hour = i
	}

	bucketLo := []float64{0, .1, .2, .3, .4, .5, .6, .7, .8, .9}
	buckets := make([]model.ScoreBucket, 10)
	for i, lo := range bucketLo {
		hi := lo + .1
		if i == 9 {
			hi = 1.01
		}
		buckets[i] = model.ScoreBucket{
			Label: fmt.Sprintf("%.1f–%.1f", lo, hi),
			Lo:    lo, Hi: hi,
		}
	}

	for _, ev := range r.events {
		s.TotalEvents++
		if ev.IsAnomaly {
			s.TotalAnomalies++
		}
		if ev.AccessResult == 0 {
			s.TotalDenied++
		}
		users[ev.UserID] = struct{}{}
		h := ev.AccessDate.Hour()
		hourly[h].Total++
		if ev.IsAnomaly {
			hourly[h].Anomaly++
		}
		for i, b := range buckets {
			if ev.AnomalyScore >= b.Lo && ev.AnomalyScore < b.Hi {
				buckets[i].Count++
				break
			}
		}
	}

	s.UniqueUsers = len(users)
	if s.TotalEvents > 0 {
		s.AnomalyPct = float64(s.TotalAnomalies) / float64(s.TotalEvents) * 100
	}
	s.HourlyStats = hourly
	s.ScoreBuckets = buckets
	return s, nil
}
