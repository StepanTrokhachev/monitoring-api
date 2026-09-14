package model

import "time"

// AccessEvent — одна запись лога доступа из MANDATORY_ACCESS_LOGS / CSV.
// Поля соответствуют тому, что пишет PHP-сервис мандатного доступа.
type AccessEvent struct {
	ID           int64     `db:"id"           json:"id"`
	IP           string    `db:"ip"            json:"ip"`
	Hostname     string    `db:"hostname"      json:"hostname"`
	UserID       int64     `db:"user_id"       json:"user_id"`
	TaskID       int64     `db:"task_id"       json:"task_id"`
	AccessResult int       `db:"access_result" json:"access_result"` // 1=разрешён, 0=отказ
	AccessDate   time.Time `db:"access_date"   json:"access_date"`

	// Обогащение от ML-сервиса (заполняется после /predict)
	AnomalyScore float64 `db:"anomaly_score" json:"anomaly_score"`
	IsAnomaly    bool    `db:"is_anomaly"    json:"is_anomaly"`
	BlockAction  string  `db:"block_action"  json:"block_action"` // allow|monitor|block
}

// UserStat — агрегированная статистика по пользователю.
type UserStat struct {
	UserID       int64     `db:"user_id"          json:"user_id"`
	TotalEvents  int       `db:"total_events"     json:"total_events"`
	AllowedCount int       `db:"allowed_count"    json:"allowed_count"`
	DeniedCount  int       `db:"denied_count"     json:"denied_count"`
	AnomalyCount int       `db:"anomaly_count"    json:"anomaly_count"`
	MaxScore     float64   `db:"max_score"        json:"max_score"`
	LastSeen     time.Time `db:"last_seen"        json:"last_seen"`
	UniqueIPs    int       `db:"unique_ips"       json:"unique_ips"`
	UniqueTasks  int       `db:"unique_tasks"     json:"unique_tasks"`
}

// Alert — уведомление, отправленное на почту при превышении порога.
type Alert struct {
	ID        int64     `db:"id"           json:"id"`
	EventID   int64     `db:"event_id"     json:"event_id"`
	UserID    int64     `db:"user_id"      json:"user_id"`
	Score     float64   `db:"score"        json:"score"`
	Threshold float64   `db:"threshold"    json:"threshold"`
	SentAt    time.Time `db:"sent_at"      json:"sent_at"`
	Recipient string    `db:"recipient"    json:"recipient"`
	Status    string    `db:"status"       json:"status"` // sent|failed
}

// WSMessage — сообщение, отправляемое клиенту через WebSocket.
type WSMessage struct {
	Type    string      `json:"type"` // "event"|"anomaly"|"stats"|"ping"
	Payload interface{} `json:"payload"`
}

// FilterParams — параметры фильтрации для GET /events.
type FilterParams struct {
	UserID       *int64     `form:"user_id"`
	TaskID       *int64     `form:"task_id"`
	IP           *string    `form:"ip"`
	OnlyAnom     bool       `form:"only_anomalies"`
	AccessResult *int       `form:"access_result"`
	BlockAction  *string    `form:"action"`
	DateFrom     *time.Time `form:"date_from" time_format:"2006-01-02T15:04:05"`
	DateTo       *time.Time `form:"date_to"   time_format:"2006-01-02T15:04:05"`
	Page         int        `form:"page,default=1"`
	PageSize     int        `form:"page_size,default=50"`
}

// PaginatedEvents — ответ для списка событий с пагинацией.
type PaginatedEvents struct {
	Total    int           `json:"total"`
	Page     int           `json:"page"`
	PageSize int           `json:"page_size"`
	Items    []AccessEvent `json:"items"`
}

// ThresholdConfig — настройка порога уведомлений (хранится в БД/памяти).
type ThresholdConfig struct {
	ID              int64   `db:"id"               json:"id"`
	AlertThreshold  float64 `db:"alert_threshold"  json:"alert_threshold"`  // score >= X → письмо
	EmailRecipients string  `db:"email_recipients" json:"email_recipients"` // через запятую
	UpdatedAt       string  `db:"updated_at"       json:"updated_at"`
}

// Summary — сводка для главной страницы дашборда.
type Summary struct {
	TotalEvents    int           `json:"total_events"`
	TotalAnomalies int           `json:"total_anomalies"`
	TotalDenied    int           `json:"total_denied"`
	UniqueUsers    int           `json:"unique_users"`
	AnomalyPct     float64       `json:"anomaly_pct"`
	HourlyStats    []HourlyStat  `json:"hourly_stats"`
	ScoreBuckets   []ScoreBucket `json:"score_buckets"`
}

// HourlyStat — статистика по одному часу суток.
type HourlyStat struct {
	Hour    int `json:"hour"`
	Total   int `json:"total"`
	Anomaly int `json:"anomaly"`
}

// ScoreBucket — bucket гистограммы распределения score.
type ScoreBucket struct {
	Label string  `json:"label"`
	Lo    float64 `json:"lo"`
	Hi    float64 `json:"hi"`
	Count int     `json:"count"`
}
