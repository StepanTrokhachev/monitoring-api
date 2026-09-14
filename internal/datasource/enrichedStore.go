package datasource

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"anomaly-monitor/internal/model"
)

// EnrichedStore сохраняет обогащённые события (с anomaly_score) в CSV-файл
// и загружает их при рестарте сервера — так данные не теряются.
//
// Формат файла:
//
//	id,ip,hostname,user_id,task_id,access_result,access_date,anomaly_score,is_anomaly,block_action
type EnrichedStore struct {
	path string
	mu   sync.Mutex
}

func NewEnrichedStore(path string) *EnrichedStore {
	return &EnrichedStore{path: path}
}

// Path возвращает путь к файлу хранилища.
func (s *EnrichedStore) Path() string {
	return s.path
}

// Append дописывает новые события в конец файла.
// Если файл не существует — создаёт с заголовком.
func (s *EnrichedStore) Append(events []model.AccessEvent) error {
	if len(events) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	needHeader := false
	if _, err := os.Stat(s.path); os.IsNotExist(err) {
		needHeader = true
	}

	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("enriched store open: %w", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if needHeader {
		_ = w.Write([]string{
			"id", "ip", "hostname", "user_id", "task_id",
			"access_result", "access_date",
			"anomaly_score", "is_anomaly", "block_action",
		})
	}

	for _, ev := range events {
		isAnom := "0"
		if ev.IsAnomaly {
			isAnom = "1"
		}
		_ = w.Write([]string{
			strconv.FormatInt(ev.ID, 10),
			ev.IP,
			ev.Hostname,
			strconv.FormatInt(ev.UserID, 10),
			strconv.FormatInt(ev.TaskID, 10),
			strconv.Itoa(ev.AccessResult),
			ev.AccessDate.Format("2006-01-02 15:04:05"),
			strconv.FormatFloat(ev.AnomalyScore, 'f', 6, 64),
			isAnom,
			ev.BlockAction,
		})
	}
	w.Flush()
	return w.Error()
}

// LoadAll читает все ранее сохранённые обогащённые события.
// Вызывается при старте сервера для восстановления состояния.
func (s *EnrichedStore) LoadAll() ([]model.AccessEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.Open(s.path)
	if os.IsNotExist(err) {
		return nil, nil // файл ещё не создан — это нормально
	}
	if err != nil {
		return nil, fmt.Errorf("enriched store load: %w", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("enriched store csv read: %w", err)
	}
	if len(rows) < 2 {
		return nil, nil // только заголовок или пусто
	}

	// Строим индекс по заголовку
	col := make(map[string]int)
	for i, h := range rows[0] {
		col[h] = i
	}

	layouts := []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05", time.RFC3339}

	get := func(row []string, name string) string {
		i, ok := col[name]
		if !ok || i >= len(row) {
			return ""
		}
		return row[i]
	}

	var events []model.AccessEvent
	for _, row := range rows[1:] {
		var ev model.AccessEvent

		if v, err := strconv.ParseInt(get(row, "id"), 10, 64); err == nil {
			ev.ID = v
		}
		ev.IP = get(row, "ip")
		ev.Hostname = get(row, "hostname")

		if v, err := strconv.ParseInt(get(row, "user_id"), 10, 64); err == nil {
			ev.UserID = v
		}
		if v, err := strconv.ParseInt(get(row, "task_id"), 10, 64); err == nil {
			ev.TaskID = v
		}
		if v, err := strconv.Atoi(get(row, "access_result")); err == nil {
			ev.AccessResult = v
		}

		for _, layout := range layouts {
			if t, err := time.Parse(layout, get(row, "access_date")); err == nil {
				ev.AccessDate = t
				break
			}
		}

		if v, err := strconv.ParseFloat(get(row, "anomaly_score"), 64); err == nil {
			ev.AnomalyScore = v
		}
		ev.IsAnomaly = get(row, "is_anomaly") == "1"
		ev.BlockAction = get(row, "block_action")

		if ev.IP != "" {
			events = append(events, ev)
		}
	}

	return events, nil
}

// MaxID возвращает максимальный ID среди сохранённых событий.
// Используется чтобы при рестарте не перечитывать уже обработанные строки.
func (s *EnrichedStore) MaxID() (int64, error) {
	events, err := s.LoadAll()
	if err != nil {
		return 0, err
	}
	var maxID int64
	for _, ev := range events {
		if ev.ID > maxID {
			maxID = ev.ID
		}
	}
	return maxID, nil
}
