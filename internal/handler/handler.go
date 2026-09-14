package handler

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"anomaly-monitor/internal/model"
	"anomaly-monitor/internal/service"
)

// ─────────────────────────────────────────────
// WebSocket Hub — рассылает события всем клиентам
// ─────────────────────────────────────────────

type Hub struct {
	mu       sync.RWMutex
	clients  map[*websocket.Conn]struct{}
	upgrader websocket.Upgrader
	log      *zap.Logger
}

func NewHub(log *zap.Logger) *Hub {
	return &Hub{
		clients: make(map[*websocket.Conn]struct{}),
		log:     log,
		upgrader: websocket.Upgrader{
			CheckOrigin:     func(r *http.Request) bool { return true },
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
	}
}

func (h *Hub) Register(conn *websocket.Conn) {
	h.mu.Lock()
	h.clients[conn] = struct{}{}
	h.mu.Unlock()
	h.log.Info("WS клиент подключился", zap.Int("total", len(h.clients)))
}

func (h *Hub) Unregister(conn *websocket.Conn) {
	h.mu.Lock()
	delete(h.clients, conn)
	h.mu.Unlock()
	h.log.Info("WS клиент отключился", zap.Int("total", len(h.clients)))
}

// Broadcast рассылает сообщение всем подключённым клиентам.
func (h *Hub) Broadcast(msg model.WSMessage) {
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for conn := range h.clients {
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			h.log.Warn("WS write error", zap.Error(err))
		}
	}
}

// BroadcastEvents вызывается сервисом при появлении новых событий.
func (h *Hub) BroadcastEvents(events []model.AccessEvent) {
	h.Broadcast(model.WSMessage{Type: "events", Payload: events})

	// Отдельно рассылаем аномалии с типом "anomaly" для подсветки
	var anoms []model.AccessEvent
	for _, e := range events {
		if e.IsAnomaly {
			anoms = append(anoms, e)
		}
	}
	if len(anoms) > 0 {
		h.Broadcast(model.WSMessage{Type: "anomaly", Payload: anoms})
	}
}

// ─────────────────────────────────────────────
// HTTP Handlers
// ─────────────────────────────────────────────

type Handler struct {
	svc *service.EventService
	hub *Hub
	log *zap.Logger
}

func New(svc *service.EventService, hub *Hub, log *zap.Logger) *Handler {
	return &Handler{svc: svc, hub: hub, log: log}
}

// RegisterRoutes регистрирует все маршруты.
func (h *Handler) RegisterRoutes(r *gin.Engine) {
	api := r.Group("/api/v1")
	{
		// События
		api.GET("/events", h.ListEvents)
		api.GET("/events/:id", h.GetEvent)

		// Пользователи
		api.GET("/users", h.ListUsers)
		api.GET("/users/:id", h.GetUser)
		api.GET("/users/:id/events", h.GetUserEvents)
		api.GET("/users/:id/hourly", h.GetUserHourly)

		// Аномалии
		api.GET("/anomalies", h.ListAnomalies)

		// Конфиг порога
		api.GET("/config/threshold", h.GetThreshold)
		api.PUT("/config/threshold", h.SetThreshold)

		// Переобогащение уже сохранённых событий с новыми профилями
		api.POST("/reenrich", h.ReEnrich)

		// Сводка (KPI для главной)
		api.GET("/summary", h.GetSummary)
	}

	// WebSocket
	r.GET("/ws", h.HandleWS)

	// Healthcheck
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "time": time.Now()})
	})
}

// ─── GET /api/v1/events ───────────────────────────────────────────────────

func (h *Handler) ListEvents(c *gin.Context) {
	var f model.FilterParams

	if v := c.Query("user_id"); v != "" {
		if id, err := strconv.ParseInt(v, 10, 64); err == nil {
			f.UserID = &id
		}
	}
	if v := c.Query("task_id"); v != "" {
		if id, err := strconv.ParseInt(v, 10, 64); err == nil {
			f.TaskID = &id
		}
	}
	if v := c.Query("ip"); v != "" {
		f.IP = &v
	}
	if c.Query("only_anomalies") == "true" {
		f.OnlyAnom = true
	}
	if v := c.Query("access_result"); v != "" {
		if ar, err := strconv.Atoi(v); err == nil {
			f.AccessResult = &ar
		}
	}
	if v := c.Query("date_from"); v != "" {
		if t, err := time.Parse("2006-01-02T15:04:05", v); err == nil {
			f.DateFrom = &t
		}
	}
	if v := c.Query("date_to"); v != "" {
		if t, err := time.Parse("2006-01-02T15:04:05", v); err == nil {
			f.DateTo = &t
		}
	}
	f.Page, _ = strconv.Atoi(c.DefaultQuery("page", "1"))
	f.PageSize, _ = strconv.Atoi(c.DefaultQuery("page_size", "50"))

	result, err := h.svc.ListEvents(f)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, result)
}

// ─── GET /api/v1/events/:id ───────────────────────────────────────────────

func (h *Handler) GetEvent(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	ev, err := h.svc.GetEvent(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, ev)
}

// ─── GET /api/v1/users ────────────────────────────────────────────────────

func (h *Handler) ListUsers(c *gin.Context) {
	stats, err := h.svc.ListUserStats()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": stats, "total": len(stats)})
}

// ─── GET /api/v1/users/:id ────────────────────────────────────────────────

func (h *Handler) GetUser(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	stat, err := h.svc.GetUserStats(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, stat)
}

// ─── GET /api/v1/users/:id/events ─────────────────────────────────────────

func (h *Handler) GetUserEvents(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	// page_size из query, дефолт 500 — достаточно для графика
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "500"))
	if pageSize < 1 || pageSize > 5000 {
		pageSize = 500
	}
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))

	f := model.FilterParams{
		UserID:   &id,
		Page:     page,
		PageSize: pageSize,
	}
	if c.Query("only_anomalies") == "true" {
		f.OnlyAnom = true
	}
	result, err := h.svc.ListEvents(f)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, result)
}

// ─── GET /api/v1/users/:id/hourly ─────────────────────────────────────────────

func (h *Handler) GetUserHourly(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	hourly, err := h.svc.GetUserHourly(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"hourly": hourly})
}

// ─── GET /api/v1/anomalies ────────────────────────────────────────────────

func (h *Handler) ListAnomalies(c *gin.Context) {
	f := model.FilterParams{
		OnlyAnom: true,
		Page:     1,
		PageSize: 200,
	}
	if v := c.Query("action"); v != "" {
		f.BlockAction = &v
	}
	result, err := h.svc.ListEvents(f)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, result)
}

// ─── GET /api/v1/summary ─────────────────────────────────────────────────

func (h *Handler) GetSummary(c *gin.Context) {
	summary, err := h.svc.GetSummary()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, summary)
}

// ─── GET /api/v1/config/threshold ─────────────────────────────────────────

func (h *Handler) GetThreshold(c *gin.Context) {
	thr, rcpt := h.svc.GetThreshold()
	c.JSON(http.StatusOK, gin.H{
		"threshold":  thr,
		"recipients": rcpt,
	})
}

// ─── PUT /api/v1/config/threshold ─────────────────────────────────────────

func (h *Handler) SetThreshold(c *gin.Context) {
	var body struct {
		Threshold  float64 `json:"threshold"`
		Recipients string  `json:"recipients"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if body.Threshold < 0.1 || body.Threshold > 1.0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "threshold must be between 0.1 and 1.0"})
		return
	}
	h.svc.UpdateThreshold(body.Threshold, body.Recipients)
	h.log.Info("порог обновлён", zap.Float64("threshold", body.Threshold))
	c.JSON(http.StatusOK, gin.H{"ok": true, "threshold": body.Threshold})
}

// ─── POST /api/v1/reenrich ────────────────────────────────────────────────────

func (h *Handler) ReEnrich(c *gin.Context) {
	go func() {
		if err := h.svc.ReEnrich(); err != nil {
			h.log.Error("reenrich failed", zap.Error(err))
		}
	}()
	c.JSON(http.StatusAccepted, gin.H{
		"status":  "started",
		"message": "Переобогащение запущено в фоне. Следите за логами.",
	})
}

// ─── GET /ws ───────────────────────────────────────────────────────────────

func (h *Handler) HandleWS(c *gin.Context) {
	conn, err := h.hub.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		h.log.Error("WS upgrade failed", zap.Error(err))
		return
	}
	h.hub.Register(conn)
	defer h.hub.Unregister(conn)

	// Отправляем начальный снапшот
	summary, _ := h.svc.GetSummary()
	conn.WriteJSON(model.WSMessage{Type: "summary", Payload: summary})

	// Читаем ping от клиента (keepalive)
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}
