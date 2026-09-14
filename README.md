# Система мониторинга аномалий АСУ

## Архитектура

```
CSV / Oracle DB
      ↓ polling (30 сек)
Go Backend (порт 8080)
  ├── REST API  → Angular / dashboard.html
  ├── WebSocket → live обновления
  └── HTTP      → Python ML-сервис (порт 8000)
                        ↓
                  rf_anomaly_model.pkl
```

## Быстрый старт

### 1. Python ML-сервис

```bash
cd ml_service
pip install -r requirements.txt

# Если модель ещё не обучена — обучить через Streamlit:
streamlit run ../script.py
# затем скопировать rf_anomaly_model.pkl и rf_scaler.pkl в ml_service/

# Запуск сервиса
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Проверка: http://localhost:8000/docs

### 2. Go Backend

```bash
cd backend

# Скопировать .env.example → .env и заполнить
cp .env.example .env

# Создать папку с данными и положить CSV
mkdir -p data
cp /path/to/TRANSFORMED_ACCESS_LOG_2.csv data/

# Установить зависимости и запустить
go mod tidy
go run ./cmd/server/main.go
```

Проверка: http://localhost:8080/health

### 3. Frontend

```bash
# Открыть dashboard.html в браузере
# По умолчанию BASE_URL = 'http://localhost:8080'
# Менять в начале <script> в dashboard.html:
#   const BASE_URL = 'http://ваш-сервер:8080';
#   const USE_MOCK = false; // false = реальный API
```

## API эндпоинты Go Backend

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/health` | Статус сервера |
| GET | `/api/v1/summary` | KPI для главной (события, аномалии, распределение по часам, score) |
| GET | `/api/v1/events` | Список событий с фильтрами и пагинацией |
| GET | `/api/v1/events/:id` | Одно событие по ID |
| GET | `/api/v1/users` | Статистика по всем пользователям |
| GET | `/api/v1/users/:id` | Карточка пользователя |
| GET | `/api/v1/users/:id/events` | События конкретного пользователя |
| GET | `/api/v1/anomalies` | Только аномальные события |
| GET | `/api/v1/config/threshold` | Текущий порог алертов |
| PUT | `/api/v1/config/threshold` | Обновить порог и email получателей |
| GET | `/ws` | WebSocket подключение |

### Параметры фильтрации GET /api/v1/events

| Параметр | Тип | Описание |
|----------|-----|----------|
| `user_id` | int | Фильтр по ID пользователя |
| `task_id` | int | Фильтр по ID задачи |
| `ip` | string | Фильтр по IP (contains) |
| `only_anomalies` | bool | Только аномалии |
| `access_result` | 0\|1 | Фильтр по результату доступа |
| `action` | string | block\|monitor\|allow |
| `date_from` | datetime | С даты (2006-01-02T15:04:05) |
| `date_to` | datetime | По дату |
| `page` | int | Страница (по умолчанию 1) |
| `page_size` | int | Размер страницы (по умолчанию 50) |

### PUT /api/v1/config/threshold

```json
{
  "threshold": 0.85,
  "recipients": "admin@company.ru,sec@company.ru"
}
```

### WebSocket сообщения (сервер → клиент)

```json
// Новые события
{ "type": "events", "payload": [...AccessEvent] }

// Аномалии отдельно
{ "type": "anomaly", "payload": [...AccessEvent] }

// Сводка при подключении
{ "type": "summary", "payload": { total_events, total_anomalies, ... } }
```

## Переключение с CSV на Oracle

1. В `.env`:
   ```
   DATA_SOURCE=oracle
   ORACLE_DSN=user/password@host:1521/ORCL
   ```
2. Раскомментировать `OracleReader.ReadSince()` в `internal/datasource/datasource.go`
3. Добавить драйвер: `go get github.com/godror/godror`

Остальной код (service, handler, frontend) не меняется.

## Структура проекта

```
backend/
├── cmd/server/main.go          # точка входа
├── internal/
│   ├── config/config.go        # загрузка .env
│   ├── datasource/datasource.go # CSV + Oracle reader
│   ├── model/model.go          # структуры данных
│   ├── repository/repo.go      # in-memory + интерфейс PostgreSQL
│   ├── service/service.go      # polling, ML, алерты
│   ├── handler/handler.go      # HTTP + WebSocket
│   └── notifier/mailer.go      # email уведомления
├── .env.example
└── go.mod

ml_service/
├── main.py                     # FastAPI ML-сервис
├── features.py                 # feature engineering
└── requirements.txt

dashboard.html                  # Angular-совместимый SPA дашборд
script.py                       # Streamlit: обучение модели
```
