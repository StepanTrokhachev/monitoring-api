package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"

	"anomaly-monitor/internal/config"
	"anomaly-monitor/internal/datasource"
	"anomaly-monitor/internal/handler"
	"anomaly-monitor/internal/notifier"
	"anomaly-monitor/internal/repository"
	"anomaly-monitor/internal/service"
)

func main() {
	// ── Logger ────────────────────────────────
	log, _ := zap.NewProduction()
	defer log.Sync()

	// ── Config ────────────────────────────────
	cfg := config.Load()
	log.Info("конфигурация загружена",
		zap.String("data_source", cfg.DataSource),
		zap.String("port", cfg.Port),
		zap.Int("polling_sec", cfg.PollingIntervalSec),
	)

	// ── DataSource (CSV или Oracle) ───────────
	src, err := datasource.New(cfg.DataSource, cfg.CSVPath, cfg.OracleDSN)
	if err != nil {
		log.Fatal("datasource init failed", zap.Error(err))
	}

	// ── EnrichedStore — хранит обогащённые события на диске ──
	// Путь берём из конфига, по умолчанию data/enriched_log.csv
	enrichedPath := cfg.EnrichedPath
	if enrichedPath == "" {
		enrichedPath = "data/enriched_log.csv"
	}
	// Создаём папку data/ если не существует
	if err := os.MkdirAll("data", 0755); err != nil {
		log.Warn("не удалось создать папку data/", zap.Error(err))
	}
	enrichedStore := datasource.NewEnrichedStore(enrichedPath)

	// ── Repository (in-memory) ────────────────
	repo := repository.NewMemoryRepo()

	// Загружаем ранее сохранённые обогащённые события при рестарте
	log.Info("загрузка ранее обогащённых событий...", zap.String("path", enrichedPath))
	prevEvents, loadErr := enrichedStore.LoadAll()
	if loadErr != nil {
		log.Warn("не удалось загрузить enriched_log.csv", zap.Error(loadErr))
	} else if len(prevEvents) > 0 {
		_ = repo.Save(prevEvents)
		log.Info("восстановлено событий из enriched_log.csv",
			zap.Int("count", len(prevEvents)),
		)
	}

	// ── Notifier ──────────────────────────────
	mailer := notifier.NewMailer(cfg, log)

	// ── Service ───────────────────────────────
	svc := service.New(cfg, src, repo, enrichedStore, mailer, log)

	// ── WebSocket Hub ─────────────────────────
	hub := handler.NewHub(log)
	svc.SetNewEventCallback(hub.BroadcastEvents)

	// ── Gin router ────────────────────────────
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: false,
		MaxAge:           12 * time.Hour,
	}))

	h := handler.New(svc, hub, log)
	h.RegisterRoutes(r)

	// ── Первоначальный polling ─────────────────────────────────
	// Если enriched_log.csv уже есть — запускаем сервер сразу,
	// первый cron-poll подхватит только новые строки из CSV.
	// Если файла нет (первый запуск) — читаем CSV порциями.
	ctx := context.Background()
	if len(prevEvents) == 0 {
		log.Info("первый запуск: загрузка данных из источника (может занять время)...")
		if err := svc.Poll(ctx); err != nil {
			log.Warn("первоначальный poll завершился с ошибкой", zap.Error(err))
		}
	} else {
		log.Info("данные восстановлены из enriched_log.csv, пропускаем полный poll при старте")
		// Лёгкий poll — подхватит только строки новее maxID
		if err := svc.Poll(ctx); err != nil {
			log.Warn("инкрементальный poll завершился с ошибкой", zap.Error(err))
		}
	}

	// ── Планировщик polling ───────────────────
	c := cron.New()
	spec := fmt.Sprintf("@every %ds", cfg.PollingIntervalSec)
	if _, err := c.AddFunc(spec, func() {
		if err := svc.Poll(context.Background()); err != nil {
			log.Error("poll error", zap.Error(err))
		}
	}); err != nil {
		log.Fatal("cron add func failed", zap.Error(err))
	}
	c.Start()
	defer c.Stop()

	// ── HTTP-сервер ───────────────────────────
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		log.Info("сервер запущен", zap.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("server error", zap.Error(err))
		}
	}()

	// ── Graceful shutdown ─────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("завершение работы...")
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Error("shutdown error", zap.Error(err))
	}
	log.Info("сервер остановлен")
}
