package datasource

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"anomaly-monitor/internal/model"
)

// Reader — интерфейс источника данных.
// CSV реализует его сейчас; Oracle/PostgreSQL реализуют позже
// без изменений в остальном коде.
type Reader interface {
	// ReadSince возвращает события, которые появились после lastID.
	// При первом вызове lastID=0 — вернёт все записи.
	ReadSince(lastID int64) ([]model.AccessEvent, error)
}

// ─────────────────────────────────────────────
// CSV-источник
// ─────────────────────────────────────────────

// CSVReader читает ACCESS_LOG из CSV-файла.
// Формат строки совпадает с тем, что пишет PHP-сервис:
//
//	id,ip,hostname,user_id,task_id,access_result,access_date
//
// Если id отсутствует в CSV (старый формат без id) — генерируем
// порядковый номер самостоятельно.
type CSVReader struct {
	path        string
	mu          sync.Mutex
	dateLayouts []string
}

func NewCSVReader(path string) *CSVReader {
	return &CSVReader{
		path: path,
		dateLayouts: []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			time.RFC3339,
		},
	}
}

// ReadSince читает CSV построчно (streaming) — не грузит весь файл в память.
// Возвращает события с ID > lastID.
func (r *CSVReader) ReadSince(lastID int64) ([]model.AccessEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	f, err := os.Open(r.path)
	if err != nil {
		return nil, fmt.Errorf("csv open: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.TrimLeadingSpace = true
	reader.Comment = '#'
	reader.ReuseRecord = true // оптимизация памяти

	// Читаем заголовок
	header, err := reader.Read()
	if err != nil {
		return nil, nil // пустой файл
	}
	// Копируем заголовок (ReuseRecord перезаписывает слайс)
	headerCopy := make([]string, len(header))
	copy(headerCopy, header)
	colIndex := r.buildColIndex(headerCopy)

	var events []model.AccessEvent
	lineNum := int64(0)

	for {
		row, err := reader.Read()
		if err != nil {
			break // EOF или ошибка — заканчиваем
		}
		lineNum++

		// Копируем строку (из-за ReuseRecord)
		rowCopy := make([]string, len(row))
		copy(rowCopy, row)

		ev, err := r.parseRow(rowCopy, colIndex, lineNum)
		if err != nil {
			continue
		}
		if ev.ID > lastID {
			events = append(events, ev)
		}
	}
	return events, nil
}

// buildColIndex строит map имя_колонки→индекс.
// Нормализует имена: регистронезависимо, убирает пробелы.
// Поддерживает форматы:
//
//	С заголовком: ID,IP,USER_ID,TASK_ID,ACCESS_DATE  (реальный CSV из ЕИС)
//	Без заголовка: первое поле — число
func (r *CSVReader) buildColIndex(header []string) map[string]int {
	idx := make(map[string]int, len(header))
	// Если первое поле — число, заголовка нет
	if _, err := strconv.ParseFloat(strings.TrimSpace(header[0]), 64); err == nil {
		// Позиционный формат: ID, IP, USER_ID, TASK_ID, ACCESS_DATE
		defaults := []string{"id", "ip", "user_id", "task_id", "access_date"}
		for i, name := range defaults {
			if i < len(header) {
				idx[name] = i
			}
		}
		return idx
	}
	// Именованный заголовок — нормализуем
	for i, name := range header {
		key := strings.ToLower(strings.TrimSpace(name))
		idx[key] = i
	}
	return idx
}

func (r *CSVReader) parseRow(row []string, col map[string]int, fallbackID int64) (model.AccessEvent, error) {
	get := func(name string) string {
		i, ok := col[name]
		if !ok || i >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[i])
	}

	var ev model.AccessEvent

	// ID (обязательное поле)
	idStr := get("id")
	if idStr == "" {
		return ev, fmt.Errorf("missing id")
	}
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return ev, fmt.Errorf("invalid id: %s", idStr)
	}
	ev.ID = id

	// IP (обязательное)
	ev.IP = get("ip")
	if ev.IP == "" {
		return ev, fmt.Errorf("missing ip")
	}

	// Hostname — опционально (в реальном CSV его нет)
	ev.Hostname = get("hostname")

	// USER_ID
	if v, err := strconv.ParseInt(get("user_id"), 10, 64); err == nil {
		ev.UserID = v
	}

	// TASK_ID
	if v, err := strconv.ParseInt(get("task_id"), 10, 64); err == nil {
		ev.TaskID = v
	}

	// ACCESS_RESULT — опционально (в реальном CSV его нет, по умолчанию 1)
	if s := get("access_result"); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			ev.AccessResult = v
		}
	} else {
		ev.AccessResult = 1 // разрешён по умолчанию
	}

	// ACCESS_DATE — пробуем несколько форматов включая с миллисекундами
	if s := get("access_date"); s != "" {
		// Убираем .000 в конце если есть
		s = strings.Split(s, ".")[0]
		for _, layout := range r.dateLayouts {
			if t, err := time.Parse(layout, s); err == nil {
				ev.AccessDate = t
				break
			}
		}
	}

	return ev, nil
}

// ─────────────────────────────────────────────
// Oracle-источник (заглушка для будущего)
// ─────────────────────────────────────────────

// OracleReader будет читать из MANDATORY_ACCESS_LOGS.ACCESS_LOG.
// Чтобы подключить: раскомментировать и добавить драйвер godror.
//
// Схема таблицы Oracle (из документации мандатного доступа):
//   ACCESS_LOG (ip, hostname, user_id, task_id, access_result, access_date)
//
// При переключении: в config.DataSource поставить "oracle",
// в config.OracleDSN — строку подключения.
// Остальной код (service, handler) не меняется.

type OracleReader struct {
	// db *sqlx.DB  // раскомментировать при подключении godror
	lastQuery string
}

// NewOracleReader создаёт Reader для Oracle.
// Сейчас возвращает заглушку — добавьте реализацию при появлении доступа к БД.
func NewOracleReader(_ string) *OracleReader {
	return &OracleReader{
		lastQuery: `
			SELECT
				ROWNUM            AS id,
				ip,
				hostname,
				user_id,
				task_id,
				access_result,
				access_date
			FROM MANDATORY_ACCESS_LOGS.ACCESS_LOG
			WHERE ROWNUM > :last_id
			ORDER BY access_date ASC
		`,
	}
}

func (r *OracleReader) ReadSince(lastID int64) ([]model.AccessEvent, error) {
	// TODO: раскомментировать когда будет драйвер godror и DSN
	//
	// rows, err := r.db.NamedQuery(r.lastQuery, map[string]interface{}{"last_id": lastID})
	// if err != nil { return nil, err }
	// defer rows.Close()
	// var events []model.AccessEvent
	// for rows.Next() {
	//     var ev model.AccessEvent
	//     if err := rows.StructScan(&ev); err != nil { continue }
	//     events = append(events, ev)
	// }
	// return events, nil

	return nil, fmt.Errorf("oracle reader: not implemented yet — set DATA_SOURCE=csv")
}

// ─────────────────────────────────────────────
// Фабрика
// ─────────────────────────────────────────────

// New создаёт нужный Reader по значению dataSource из конфига.
func New(dataSource, csvPath, oracleDSN string) (Reader, error) {
	switch dataSource {
	case "csv", "":
		if csvPath == "" {
			return nil, fmt.Errorf("CSV_PATH не задан")
		}
		return NewCSVReader(csvPath), nil
	case "oracle":
		return NewOracleReader(oracleDSN), nil
	default:
		return nil, fmt.Errorf("неизвестный DATA_SOURCE: %s", dataSource)
	}
}
